//! Persistent zero-child-process transport for an established agent-browser daemon.
//!
//! Input and output are newline-delimited JSON. The process keeps one daemon
//! connection per `(namespace, session)` and therefore avoids child CLI startup,
//! repeated socket handshakes, output-reader threads, and polling on every action.

use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::env;
#[cfg(windows)]
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
#[cfg(windows)]
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

const SCHEMA_VERSION: &str = "agent.browser.fast-channel/v1";
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const MAX_TIMEOUT_MS: u64 = 600_000;
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Request {
    #[serde(default)]
    id: Value,
    #[serde(default = "default_session")]
    session: String,
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
    #[serde(default)]
    operation: Option<String>,
    #[serde(default)]
    command: Option<Value>,
}

fn default_session() -> String {
    "default".to_string()
}

const fn default_timeout() -> u64 {
    DEFAULT_TIMEOUT_MS
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    socket_dir: PathBuf,
    session: String,
    #[cfg(windows)]
    port_identity: String,
}

impl Key {
    fn new(session: &str, namespace: Option<&str>) -> Result<Self, String> {
        validate_session(session)?;
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
    fn connect(key: &Key) -> Result<Self, String> {
        Ok(Self {
            reader: BufReader::new(connect_transport(key)?),
        })
    }

    fn kind(&self) -> &'static str {
        self.reader.get_ref().kind()
    }

    fn send(&mut self, command: &Value, timeout: Duration) -> Result<Value, String> {
        self.reader
            .get_ref()
            .set_timeouts(timeout)
            .map_err(|error| format!("failed to configure daemon channel: {error}"))?;
        let mut payload = serde_json::to_vec(command)
            .map_err(|error| format!("failed to encode daemon command: {error}"))?;
        payload.push(b'\n');
        self.reader
            .get_mut()
            .write_all(&payload)
            .and_then(|_| self.reader.get_mut().flush())
            .map_err(|error| format!("failed to send daemon command: {error}"))?;

        let mut line = String::new();
        let bytes = self
            .reader
            .read_line(&mut line)
            .map_err(|error| format!("failed to read daemon response: {error}"))?;
        if bytes == 0 {
            return Err("daemon closed the persistent channel before responding".to_string());
        }
        if line.len() > MAX_RESPONSE_BYTES {
            return Err(format!(
                "daemon response exceeded {} MiB",
                MAX_RESPONSE_BYTES / (1024 * 1024)
            ));
        }
        serde_json::from_str(line.trim_end())
            .map_err(|error| format!("daemon returned invalid JSON: {error}"))
    }
}

struct Outcome {
    response: Value,
    transport: &'static str,
    reused: bool,
    reconnected: bool,
}

struct Pool {
    channels: BTreeMap<Key, Channel>,
}

impl Pool {
    fn new() -> Self {
        Self {
            channels: BTreeMap::new(),
        }
    }

    fn send(&mut self, key: Key, command: &Value, timeout: Duration) -> Result<Outcome, String> {
        let reused = self.channels.contains_key(&key);
        if !reused {
            self.channels.insert(key.clone(), Channel::connect(&key)?);
        }
        let first = self
            .channels
            .get_mut(&key)
            .expect("channel exists")
            .send(command, timeout);
        match first {
            Ok(response) => Ok(Outcome {
                response,
                transport: self.channels.get(&key).expect("channel exists").kind(),
                reused,
                reconnected: false,
            }),
            Err(first_error) => {
                self.channels.remove(&key);
                let mut channel = Channel::connect(&key).map_err(|connect_error| {
                    format!(
                        "persistent channel failed ({first_error}); reconnect failed ({connect_error})"
                    )
                })?;
                let transport = channel.kind();
                let response = channel.send(command, timeout).map_err(|retry_error| {
                    format!(
                        "persistent channel failed ({first_error}); retry failed ({retry_error})"
                    )
                })?;
                self.channels.insert(key, channel);
                Ok(Outcome {
                    response,
                    transport,
                    reused: false,
                    reconnected: true,
                })
            }
        }
    }
}

struct Server {
    pool: Pool,
    sequence: u64,
}

impl Server {
    fn new() -> Self {
        Self {
            pool: Pool::new(),
            sequence: 0,
        }
    }

    fn handle(&mut self, request: Request) -> Value {
        let started = Instant::now();
        let id = request.id.clone();
        let operation = request
            .operation
            .clone()
            .unwrap_or_else(|| "send".to_string());
        let key = match Key::new(&request.session, request.namespace.as_deref()) {
            Ok(key) => key,
            Err(message) => return error(id, message, started),
        };

        match operation.as_str() {
            "ping" => success(
                id,
                json!({
                    "pong": true,
                    "session": request.session,
                    "connected": self.pool.channels.contains_key(&key),
                }),
                started,
            ),
            "status" => success(
                id,
                json!({
                    "session": request.session,
                    "connected": self.pool.channels.contains_key(&key),
                    "socketDir": key.socket_dir,
                }),
                started,
            ),
            "close" => success(
                id,
                json!({
                    "session": request.session,
                    "closed": self.pool.channels.remove(&key).is_some(),
                }),
                started,
            ),
            "send" => self.send(request, key, id, started),
            _ => error(id, "operation must be send, ping, status, or close", started),
        }
    }

    fn send(&mut self, request: Request, key: Key, id: Value, started: Instant) -> Value {
        if !(1..=MAX_TIMEOUT_MS).contains(&request.timeout_ms) {
            return error(
                id,
                format!("timeoutMs must be between 1 and {MAX_TIMEOUT_MS}"),
                started,
            );
        }
        let mut command: Map<String, Value> = match request.command {
            Some(Value::Object(command)) => command,
            Some(_) => return error(id, "command must be a JSON object", started),
            None => return error(id, "operation=send requires command", started),
        };
        if command.get("action").and_then(Value::as_str).is_none() {
            return error(id, "command.action must be a string", started);
        }
        if !command.contains_key("id") {
            self.sequence = self.sequence.wrapping_add(1);
            command.insert("id".to_string(), json!(self.command_id()));
        }

        match self.pool.send(
            key,
            &Value::Object(command),
            Duration::from_millis(request.timeout_ms),
        ) {
            Ok(outcome) => json!({
                "schemaVersion": SCHEMA_VERSION,
                "id": id,
                "ok": outcome.response.get("success").and_then(Value::as_bool).unwrap_or(true),
                "result": outcome.response,
                "channel": {
                    "session": request.session,
                    "transport": outcome.transport,
                    "reusedConnection": outcome.reused,
                    "reconnected": outcome.reconnected,
                },
                "timing": { "roundTripMicros": elapsed_micros(started) },
            }),
            Err(message) => error(id, message, started),
        }
    }

    fn command_id(&self) -> String {
        let micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros();
        format!("fast-{}-{micros}-{}", std::process::id(), self.sequence)
    }
}

fn main() {
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut output = io::stdout().lock();
    let mut server = Server::new();
    let mut line = String::new();

    loop {
        line.clear();
        let bytes = match input.read_line(&mut line) {
            Ok(bytes) => bytes,
            Err(read_error) => {
                let response = error(
                    Value::Null,
                    format!("failed to read channel input: {read_error}"),
                    Instant::now(),
                );
                let _ = write_line(&mut output, &response);
                break;
            }
        };
        if bytes == 0 {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        let response = if line.len() > MAX_REQUEST_BYTES {
            error(
                Value::Null,
                format!(
                    "request exceeded {} MiB",
                    MAX_REQUEST_BYTES / (1024 * 1024)
                ),
                Instant::now(),
            )
        } else {
            match serde_json::from_str::<Request>(line.trim_end()) {
                Ok(request) => server.handle(request),
                Err(parse_error) => error(
                    Value::Null,
                    format!("invalid request JSON: {parse_error}"),
                    Instant::now(),
                ),
            }
        };
        if write_line(&mut output, &response).is_err() {
            break;
        }
    }
}

fn write_line(writer: &mut impl Write, value: &Value) -> io::Result<()> {
    serde_json::to_writer(&mut *writer, value)
        .map_err(|encode_error| io::Error::new(io::ErrorKind::InvalidData, encode_error))?;
    writer.write_all(b"\n")?;
    writer.flush()
}

fn success(id: Value, result: Value, started: Instant) -> Value {
    json!({
        "schemaVersion": SCHEMA_VERSION,
        "id": id,
        "ok": true,
        "result": result,
        "timing": { "roundTripMicros": elapsed_micros(started) },
    })
}

fn error(id: Value, message: impl Into<String>, started: Instant) -> Value {
    json!({
        "schemaVersion": SCHEMA_VERSION,
        "id": id,
        "ok": false,
        "error": message.into(),
        "timing": { "roundTripMicros": elapsed_micros(started) },
    })
}

fn elapsed_micros(started: Instant) -> u128 {
    started.elapsed().as_micros()
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

fn connect_transport(key: &Key) -> Result<Transport, String> {
    #[cfg(unix)]
    {
        let path = key.socket_dir.join(format!("{}.sock", key.session));
        UnixStream::connect(&path)
            .map(Transport::Unix)
            .map_err(|connect_error| {
                format!(
                    "cannot connect to {}: {connect_error}; establish the session once with agent-browser",
                    path.display()
                )
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
            .map_err(|connect_error| {
                format!(
                    "cannot connect to 127.0.0.1:{port}: {connect_error}; establish the session once with agent-browser"
                )
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
        assert!(validate_session("default").is_ok());
        assert!(validate_session("work_1").is_ok());
        assert!(validate_session("../other").is_err());
        assert!(validate_session("").is_err());
    }

    #[test]
    fn windows_port_hash_matches_daemon() {
        assert_eq!(port_for_identity("default"), 50_838);
        assert_eq!(port_for_identity("my-session"), 63_105);
        assert_eq!(port_for_identity("work"), 51_184);
    }

    #[test]
    fn request_defaults_to_hot_send() {
        let request: Request = serde_json::from_value(json!({
            "command": {"action": "snapshot"}
        }))
        .unwrap();
        assert_eq!(request.session, "default");
        assert_eq!(request.timeout_ms, DEFAULT_TIMEOUT_MS);
        assert!(request.operation.is_none());
    }
}
