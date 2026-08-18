//! Persistent at-most-once transport for an established agent-browser daemon.
//!
//! The transport distinguishes failures that happen before any command bytes
//! are written from failures whose delivery is uncertain. Only pre-send
//! failures may reconnect and retry. A timeout, EOF, malformed response, or
//! write failure after transmission begins never replays the command.

use serde_json::Value;
use std::collections::BTreeMap;
use std::env;
#[cfg(windows)]
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
#[cfg(windows)]
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::net::UnixStream;

pub const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureStage {
    BeforeSend,
    DeliveryUnknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelFailure {
    pub stage: FailureStage,
    pub message: String,
}

impl ChannelFailure {
    fn before_send(message: impl Into<String>) -> Self {
        Self {
            stage: FailureStage::BeforeSend,
            message: message.into(),
        }
    }

    fn delivery_unknown(message: impl Into<String>) -> Self {
        Self {
            stage: FailureStage::DeliveryUnknown,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ChannelFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.stage {
            FailureStage::BeforeSend => write!(formatter, "{}", self.message),
            FailureStage::DeliveryUnknown => write!(
                formatter,
                "{}; command delivery is unknown and was not replayed",
                self.message
            ),
        }
    }
}

impl std::error::Error for ChannelFailure {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SessionKey {
    socket_dir: PathBuf,
    session: String,
    #[cfg(windows)]
    port_identity: String,
}

impl SessionKey {
    pub fn new(session: &str, namespace: Option<&str>) -> Result<Self, ChannelFailure> {
        validate_session(session).map_err(ChannelFailure::before_send)?;
        let namespace = namespace
            .map(sanitize_component)
            .filter(|value| !value.is_empty());
        #[cfg(windows)]
        let port_identity = namespace
            .as_ref()
            .map(|value| format!("{value}:{session}"))
            .unwrap_or_else(|| session.to_string());
        Ok(Self {
            socket_dir: namespaced_socket_dir(socket_base_dir(), namespace.as_deref()),
            session: session.to_string(),
            #[cfg(windows)]
            port_identity,
        })
    }

    pub fn session(&self) -> &str {
        &self.session
    }

    pub fn socket_dir(&self) -> &Path {
        &self.socket_dir
    }
}

enum Transport {
    #[cfg(unix)]
    Unix(UnixStream),
    #[cfg(windows)]
    Tcp(TcpStream),
}

impl Transport {
    fn kind(&self) -> &'static str {
        match self {
            #[cfg(unix)]
            Self::Unix(_) => "unix",
            #[cfg(windows)]
            Self::Tcp(_) => "tcp",
        }
    }

    fn set_timeouts(&self, timeout: Duration) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => {
                stream.set_read_timeout(Some(timeout))?;
                stream.set_write_timeout(Some(Duration::from_secs(5)))
            }
            #[cfg(windows)]
            Self::Tcp(stream) => {
                stream.set_read_timeout(Some(timeout))?;
                stream.set_write_timeout(Some(Duration::from_secs(5)))
            }
        }
    }
}

impl Read for Transport {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => stream.read(buffer),
            #[cfg(windows)]
            Self::Tcp(stream) => stream.read(buffer),
        }
    }
}

impl Write for Transport {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => stream.write(buffer),
            #[cfg(windows)]
            Self::Tcp(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => stream.flush(),
            #[cfg(windows)]
            Self::Tcp(stream) => stream.flush(),
        }
    }
}

struct Channel {
    reader: BufReader<Transport>,
}

impl Channel {
    fn connect(key: &SessionKey) -> Result<Self, ChannelFailure> {
        Ok(Self {
            reader: BufReader::new(connect_transport(key)?),
        })
    }

    fn kind(&self) -> &'static str {
        self.reader.get_ref().kind()
    }

    fn send(&mut self, command: &Value, timeout: Duration) -> Result<Value, ChannelFailure> {
        self.reader
            .get_ref()
            .set_timeouts(timeout)
            .map_err(|error| {
                ChannelFailure::before_send(format!(
                    "failed to configure daemon channel: {error}"
                ))
            })?;
        let mut payload = serde_json::to_vec(command).map_err(|error| {
            ChannelFailure::before_send(format!("failed to encode daemon command: {error}"))
        })?;
        if payload.len() > MAX_REQUEST_BYTES {
            return Err(ChannelFailure::before_send(format!(
                "daemon command exceeded {} MiB",
                MAX_REQUEST_BYTES / (1024 * 1024)
            )));
        }
        payload.push(b'\n');

        // Once write_all begins, a failure may represent a partial or complete
        // command. Treat all subsequent failures as delivery-unknown.
        self.reader
            .get_mut()
            .write_all(&payload)
            .and_then(|_| self.reader.get_mut().flush())
            .map_err(|error| {
                ChannelFailure::delivery_unknown(format!(
                    "failed while transmitting daemon command: {error}"
                ))
            })?;

        let mut response = Vec::new();
        let bytes = self
            .reader
            .by_ref()
            .take((MAX_RESPONSE_BYTES + 1) as u64)
            .read_until(b'\n', &mut response)
            .map_err(|error| {
                ChannelFailure::delivery_unknown(format!(
                    "failed to read daemon response: {error}"
                ))
            })?;
        if bytes == 0 {
            return Err(ChannelFailure::delivery_unknown(
                "daemon closed the persistent channel before responding",
            ));
        }
        if response.len() > MAX_RESPONSE_BYTES {
            return Err(ChannelFailure::delivery_unknown(format!(
                "daemon response exceeded {} MiB",
                MAX_RESPONSE_BYTES / (1024 * 1024)
            )));
        }
        while matches!(response.last(), Some(b'\n' | b'\r')) {
            response.pop();
        }
        serde_json::from_slice(&response).map_err(|error| {
            ChannelFailure::delivery_unknown(format!(
                "daemon returned invalid JSON: {error}"
            ))
        })
    }
}

#[derive(Debug, Clone)]
pub struct SendOutcome {
    pub response: Value,
    pub transport: &'static str,
    pub reused_connection: bool,
    pub reconnected: bool,
}

#[derive(Default)]
pub struct ChannelPool {
    channels: BTreeMap<SessionKey, Channel>,
}

impl ChannelPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn contains(&self, key: &SessionKey) -> bool {
        self.channels.contains_key(key)
    }

    pub fn close(&mut self, key: &SessionKey) -> bool {
        self.channels.remove(key).is_some()
    }

    pub fn send(
        &mut self,
        key: SessionKey,
        command: &Value,
        timeout: Duration,
    ) -> Result<SendOutcome, ChannelFailure> {
        let reused_connection = self.channels.contains_key(&key);
        if !reused_connection {
            self.channels.insert(key.clone(), Channel::connect(&key)?);
        }

        let first = self
            .channels
            .get_mut(&key)
            .expect("channel exists")
            .send(command, timeout);
        match first {
            Ok(response) => Ok(SendOutcome {
                response,
                transport: self.channels.get(&key).expect("channel exists").kind(),
                reused_connection,
                reconnected: false,
            }),
            Err(failure) if failure.stage == FailureStage::BeforeSend => {
                self.channels.remove(&key);
                let mut channel = Channel::connect(&key)?;
                let transport = channel.kind();
                let response = channel.send(command, timeout)?;
                self.channels.insert(key, channel);
                Ok(SendOutcome {
                    response,
                    transport,
                    reused_connection: false,
                    reconnected: true,
                })
            }
            Err(failure) => {
                self.channels.remove(&key);
                Err(failure)
            }
        }
    }
}

fn socket_base_dir() -> PathBuf {
    if let Ok(path) = env::var("AGENT_BROWSER_SOCKET_DIR") {
        if !path.is_empty() {
            return PathBuf::from(path);
        }
    }
    if let Ok(path) = env::var("XDG_RUNTIME_DIR") {
        if !path.is_empty() {
            return PathBuf::from(path).join("agent-browser");
        }
    }
    dirs::home_dir()
        .map(|home| home.join(".agent-browser"))
        .unwrap_or_else(|| env::temp_dir().join("agent-browser"))
}

fn namespaced_socket_dir(base: PathBuf, namespace: Option<&str>) -> PathBuf {
    namespace
        .filter(|value| !value.is_empty())
        .map(|value| base.join("namespaces").join(value).join("run"))
        .unwrap_or(base)
}

fn sanitize_component(value: &str) -> String {
    let mut output = String::new();
    let mut separator = false;
    for character in value.chars() {
        if character.is_alphanumeric() {
            output.extend(character.to_lowercase());
            separator = false;
        } else if character == '-' || character == '_' {
            if !output.is_empty() && !separator {
                output.push(character);
                separator = true;
            }
        } else if !output.is_empty() && !separator {
            output.push('-');
            separator = true;
        }
    }
    while output.ends_with(['-', '_']) {
        output.pop();
    }
    output
}

fn validate_session(session: &str) -> Result<(), String> {
    if !session.is_empty()
        && session
            .chars()
            .all(|character| character.is_alphanumeric() || character == '-' || character == '_')
    {
        Ok(())
    } else {
        Err(format!(
            "invalid session name '{session}'; use alphanumeric characters, hyphens, or underscores"
        ))
    }
}

fn connect_transport(key: &SessionKey) -> Result<Transport, ChannelFailure> {
    #[cfg(unix)]
    {
        let path = key.socket_dir.join(format!("{}.sock", key.session));
        UnixStream::connect(&path)
            .map(Transport::Unix)
            .map_err(|error| {
                ChannelFailure::before_send(format!(
                    "cannot connect to {}: {error}; establish the session once with agent-browser",
                    path.display()
                ))
            })
    }
    #[cfg(windows)]
    {
        let port_file = key.socket_dir.join(format!("{}.port", key.session));
        let port = fs::read_to_string(port_file)
            .ok()
            .and_then(|value| value.trim().parse::<u16>().ok())
            .unwrap_or_else(|| port_for_identity(&key.port_identity));
        TcpStream::connect(("127.0.0.1", port))
            .map(Transport::Tcp)
            .map_err(|error| {
                ChannelFailure::before_send(format!(
                    "cannot connect to 127.0.0.1:{port}: {error}; establish the session once with agent-browser"
                ))
            })
    }
}

#[cfg(any(windows, test))]
fn port_for_identity(identity: &str) -> u16 {
    let mut hash: i32 = 0;
    for character in identity.chars() {
        hash = ((hash << 5).wrapping_sub(hash)).wrapping_add(character as i32);
    }
    49_152 + (hash.unsigned_abs() % 16_383) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_matches_daemon_layout() {
        assert_eq!(
            sanitize_component("Next Dev Loop: /Users/me/worktree!"),
            "next-dev-loop-users-me-worktree"
        );
        assert_eq!(
            namespaced_socket_dir(PathBuf::from("/tmp/ab"), Some("worktree-one")),
            PathBuf::from("/tmp/ab/namespaces/worktree-one/run")
        );
    }

    #[test]
    fn unsafe_sessions_are_rejected() {
        assert!(SessionKey::new("default", None).is_ok());
        assert!(SessionKey::new("work_1", None).is_ok());
        assert!(SessionKey::new("../other", None).is_err());
        assert!(SessionKey::new("", None).is_err());
    }

    #[test]
    fn windows_port_hash_matches_daemon() {
        assert_eq!(port_for_identity("default"), 50_838);
        assert_eq!(port_for_identity("my-session"), 63_105);
        assert_eq!(port_for_identity("work"), 51_184);
    }

    #[test]
    fn delivery_unknown_message_prohibits_replay() {
        let failure = ChannelFailure::delivery_unknown("read timed out");
        assert_eq!(failure.stage, FailureStage::DeliveryUnknown);
        assert!(failure.to_string().contains("was not replayed"));
    }
}
