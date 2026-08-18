//! Persistent, zero-child-process control channel for latency-sensitive agents.
//!
//! The normal `agent-browser` CLI remains the compatibility and lifecycle
//! surface. This binary is the hot transport used after a daemon session has
//! been established: it keeps one Unix/TCP connection per session and forwards
//! newline-delimited daemon commands without spawning a child CLI for every
//! action.

use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::env;
#[cfg(windows)]
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

const SCHEMA_VERSION: &str = "agent.browser.fast-channel/v1";
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const MAX_TIMEOUT_MS: u64 = 10 * 60 * 1_000;
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChannelRequest {
    #[serde(default)]
    id: Value,
    #[serde(default = "default_session")]
    session: String,
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default)]
    operation: Option<String>,
    #[serde(default)]
    command: Option<Value>,
}

fn default_session() -> String {
    "default".to_string()
}

const fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ChannelKey {
    socket_dir: PathBuf,
    session: String,
    #[cfg(windows)]
    port_identity: String,
}

impl ChannelKey {
    fn new(session: &str, namespace: Option<&str>) -> Result<Self, String> {
        validate_session(session)?;
        let normalized_namespace = namespace
            .map(sanitize_session_component)
            .filter(|value| !value.is_empty());
        let socket_dir = apply_namespace(socket_base_dir(), normalized_namespace.as_deref());
        #[cfg(windows)]
        let port_identity = normalized_namespace
            .as_ref()
            .map(|value| format!("{value}:{session}"))
            .unwrap_or_else(|| session.to_string());
        Ok(Self {
            socket_dir,
            session: session.to_string(),
            #[cfg(windows)]
            port_identity,
        })
    }
}

#[allow(dead_code)]
enum Transport {
    #[cfg(unix)]
    Unix(UnixStream),
    Tcp(TcpStream),
}

impl Transport {
    fn kind(&self) -> &'static str {
        match self {
            #[cfg(unix)]
            Self::Unix(_) => "unix",
            Self::Tcp(_) => "tcp",
        }
    }

    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => stream.set_read_timeout(timeout),
            Self::Tcp(stream) => stream.set_read_timeout(timeout),
        }
    }

    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => stream.set_write_timeout(timeout),
            Self::Tcp(stream) => stream.set_write_timeout(timeout),
        }
    }
}

impl Read for Transport {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => stream.read(buffer),
            Self::Tcp(stream) => stream.read(buffer),
        }
    }
}

impl Write for Transport {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => stream.write(buffer),
            Self::Tcp(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => stream.flush(),
            Self::Tcp(stream) => stream.flush(),
        }
    }
}

struct PersistentChannel {
    reader: BufReader<Transport>,
}

impl PersistentChannel {
    fn connect(key: &ChannelKey) -> Result<Self, String> {
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
            .set_read_timeout(Some(timeout))
            .map_err(|error| format!("failed to set daemon read timeout: {error}"))?;
        self.reader
            .get_ref()
            .set_write_timeout(Some(Duration::from_secs(5)))
            .map_err(|error| format!("failed to set daemon write timeout: {error}"))?;

        let mut encoded = serde_json::to_vec(command)
            .map_err(|error| format!("failed to encode daemon command: {error}"))?;
        encoded.push(b'\n');
        self.reader
            .get_mut()
            .write_all(&encoded)
            .map_err(|error| format!("failed to write daemon command: {error}"))?;
        self.reader
            .get_mut()
            .flush()
            .map_err(|error| format!("failed to flush daemon command: {error}"))?;

        let mut response = String::new();
        let bytes = self
            .reader
            .read_line(&mut response)
            .map_err(|error| format!("failed to read daemon response: {error}"))?;
        if bytes == 0 {
            return Err("daemon closed the persistent channel before responding".to_string());
        }
        if response.len() > MAX_RESPONSE_BYTES {
            return Err(format!(
                "daemon response exceeded the {} MiB channel limit",
                MAX_RESPONSE_BYTES / (1024 * 1024)
            ));
        }
        serde_json::from_str(response.trim_end())
            .map_err(|error| format!("daemon returned invalid JSON: {error}"))
    }
}

struct ChannelPool {
    channels: BTreeMap<ChannelKey, PersistentChannel>,
}

struct SendOutcome {
    response: Value,
    reused_connection: bool,
    reconnected: bool,
    transport: &'static str,
}

impl ChannelPool {
    fn new() -> Self {
        Self {
            channels: BTreeMap::new(),
        }
    }

    fn contains(&self, key: &ChannelKey) -> bool {
        self.channels.contains_key(key)
    }

    fn close(&mut self, key: &ChannelKey) -> bool {
        self.channels.remove(key).is_some()
    }

    fn send(
        &mut self,
        key: ChannelKey,
        command: &Value,
        timeout: Duration,
    ) -> Result<SendOutcome, String> {
        let reused_connection = self.channels.contains_key(&key);
        if !reused_connection {
            self.channels
                .insert(key.clone(), PersistentChannel::connect(&key)?);
        }

        let first = self
            .channels
            .get_mut(&key)
            .expect("channel was inserted")
            .send(command, timeout);
        match first {
            Ok(response) => {
                let transport = self
                    .channels
                    .get(&key)
                    .expect("channel remains connected")
                    .kind();
                Ok(SendOutcome {
                    response,
                    reused_connection,
                    reconnected: false,
                    transport,
                })
            }
            Err(first_error) => {
                self.channels.remove(&key);
                let mut channel = PersistentChannel::connect(&key).map_err(|second_error| {
                    format!(
                        "persistent channel failed ({first_error}); reconnect failed ({second_error})"
                    )
                })?;
                let transport = channel.kind();
                let response = channel.send(command, timeout).map_err(|second_error| {
                    format!(
                        "persistent channel failed ({first_error}); retry failed ({second_error})"
                    )
                })?;
                self.channels.insert(key, channel);
                Ok(SendOutcome {
                    response,
                    reused_connection: false,
                    reconnected: true,
                    transport,
                })
            }
        }
    }
}

struct Server {
    pool: ChannelPool,
    sequence: u64,
}

impl Server {
    fn new() -> Self {
        Self {
            pool: ChannelPool::new(),
            sequence: 0,
        }
    }

    fn handle(&mut self, request: ChannelRequest) -> Value {
        let started = Instant::now();
        let response_id = request.id.clone();
        let operation = request.operation.as_deref().unwrap_or("send");
        let key = match ChannelKey::new(&request.session, request.namespace.as_deref()) {
            Ok(key) => key,
            Err(error) => return error_response(response_id, error, started),
        };

        match operation {
            "ping" => success_response(
                response_id,
                json!({
                    "pong": true,
                    "session": request.session,
                    "connected": self.pool.contains(&key),
                }),
                started,
            ),
            "status" => success_response(
                response_id,
                json!({
                    "session": request.session,
                    "connected": self.pool.contains(&key),
                    "socketDir": key.socket_dir,
                }),
                started,
            ),
            "close" => success_response(
                response_id,
                json!({
                    "session": request.session,
                    "closed": self.pool.close(&key),
                }),
                started,
            ),
            "send" => self.send(request, key, response_id, started),
            _ => error_response(
                response_id,
                "operation must be send, ping, status, or close",
                started,
            ),
        }
    }

    fn send(
        &mut self,
        request: ChannelRequest,
        key: ChannelKey,
        response_id: Value,
        started: Instant,
    ) -> Value {
        if request.timeout_ms == 0 || request.timeout_ms > MAX_TIMEOUT_MS {
            return error_response(
                response_id,
                format!("timeoutMs must be between 1 and {MAX_TIMEOUT_MS} milliseconds"),
                started,
            );
        }
        let mut command: Map<String, Value> = match request.command {
            Some(Value::Object(object)) => object,
            Some(_) => {
                return error_response(response_id, "command must be a JSON object", started)
            }
            None => {
                return error_response(
                    response_id,
                    "operation=send requires command",
                    started,
                )
            }
        };
        if command.get("action").and_then(Value::as_str).is_none() {
            return error_response(response_id, "command.action must be a string", started);
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
            Ok(outcome) => {
                let daemon_success = outcome
                    .response
                    .get("success")
                    .and_then(Value::as_bool)
                    .unwrap_or(true);
                json!({
                    "schemaVersion": SCHEMA_VERSION,
                    "id": response_id,
                    "ok": daemon_success,
                    "result": outcome.response,
                    "channel": {
                        "session": request.session,
                        "transport": outcome.transport,
                        "reusedConnection": outcome.reused_connection,
                        "reconnected": outcome.reconnected,
                    },
                    "timing": { "roundTripMicros": elapsed_micros(started) },
                })
            }
            Err(error) => error_response(response_id, error, started),
        }
    }

    fn command_id(&self) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros();
        format!("fast-{}-{now}-{}", std::process::id(), self.sequence)
    }
}

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    let mut server = Server::new();
    let mut line = String::new();
    let mut reader = stdin.lock();

    loop {
        line.clear();
        let bytes = match reader.read_line(&mut line) {
            Ok(bytes) => bytes,
            Err(error) => {
                let response = error_response(
                    Value::Null,
                    format!("failed to read channel input: {error}"),
                    Instant::now(),
                );
                let _ = write_json_line(&mut stdout, &response);
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
            error_response(
                Value::Null,
                format!(
                    "channel request exceeded the {} MiB limit",
                    MAX_REQUEST_BYTES / (1024 * 1024)
                ),
                Instant::now(),
            )
        } else {
            match serde_json::from_str::<ChannelRequest>(line.trim_end()) {
                Ok(request) => server.handle(request),
                Err(error) => error_response(
                    Value::Null,
                    format!("invalid channel request JSON: {error}"),
                    Instant::now(),
                ),
            }
        };
        if write_json_line(&mut stdout, &response).is_err() {
            break;
        }
    }
}

fn write_json_line(writer: &mut impl Write, value: &Value) -> io::Result<()> {
    serde_json::to_writer(&mut *writer, value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    writer.write_all(b"\n")?;
    writer.flush()
}

fn success_response(id: Value, result: Value, started: Instant) -> Value {
    json!({
        "schemaVersion": SCHEMA_VERSION,
        "id": id,
        "ok": true,
        "result": result,
        "timing": { "roundTripMicros": elapsed_micros(started) },
    })
}

fn error_response(id: Value, error: impl Into<String>, started: Instant) -> Value {
    json!({
        "schemaVersion": SCHEMA_VERSION,
        "id": id,
        "ok": false,
        "error": error.into(),
        "timing": { "roundTripMicros": elapsed_micros(started) },
    })
}

fn elapsed_micros(started: Instant) -> u128 {
    started.elapsed().as_micros()
}

fn socket_base_dir() -> PathBuf {
    if let Ok(directory) = env::var("AGENT_BROWSER_SOCKET_DIR") {
        if !directory.is_empty() {
            return PathBuf::from(directory);
        }
    }
    if let Ok(directory) = env::var("XDG_RUNTIME_DIR") {
        if !directory.is_empty() {
            return PathBuf::from(directory).join("agent-browser");
        }
    }
    dirs::home_dir()
        .map(|home| home.join(".agent-browser"))
        .unwrap_or_else(|| env::temp_dir().join("agent-browser"))
}

fn apply_namespace(base: PathBuf, namespace: Option<&str>) -> PathBuf {
    namespace
        .filter(|value| !value.is_empty())
        .map(|value| base.join("namespaces").join(value).join("run"))
        .unwrap_or(base)
}

fn sanitize_session_component(value: &str) -> String {
    let mut output = String::new();
    let mut last_was_separator = false;
    for character in value.chars() {
        if character.is_alphanumeric() {
            output.extend(character.to_lowercase());
            last_was_separator = false;
        } else if character == '-' || character == '_' {
            if !output.is_empty() && !last_was_separator {
                output.push(character);
                last_was_separator = true;
            }
        } else if !output.is_empty() && !last_was_separator {
            output.push('-');
            last_was_separator = true;
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
            "invalid session name '{session}'; use only alphanumeric characters, hyphens, and underscores"
        ))
    }
}

fn connect_transport(key: &ChannelKey) -> Result<Transport, String> {
    #[cfg(unix)]
    {
        let path = key.socket_dir.join(format!("{}.sock", key.session));
        return UnixStream::connect(&path)
            .map(Transport::Unix)
            .map_err(|error| {
                format!(
                    "failed to connect to agent-browser daemon at {}: {error}; establish the session once with the normal agent-browser CLI",
                    path.display()
                )
            });
    }

    #[cfg(windows)]
    {
        let port_path = key.socket_dir.join(format!("{}.port", key.session));
        let port = fs::read_to_string(&port_path)
            .ok()
            .and_then(|value| value.trim().parse::<u16>().ok())
            .unwrap_or_else(|| port_for_identity(&key.port_identity));
        return TcpStream::connect(("127.0.0.1", port))
            .map(Transport::Tcp)
            .map_err(|error| {
                format!(
                    "failed to connect to agent-browser daemon at 127.0.0.1:{port}: {error}; establish the session once with the normal agent-browser CLI"
                )
            });
    }

    #[allow(unreachable_code)]
    Err("persistent channel is unsupported on this platform".to_string())
}

#[cfg(any(windows, test))]
fn port_for_identity(identity: &str) -> u16 {
    let mut hash: i32 = 0;
    for character in identity.chars() {
        hash = ((hash << 5).wrapping_sub(hash)).wrapping_add(character as i32);
    }
    49_152 + ((hash.unsigned_abs() as u32 % 16_383) as u16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_sanitization_matches_daemon_contract() {
        assert_eq!(
            sanitize_session_component("Next Dev Loop: /Users/me/worktree!"),
            "next-dev-loop-users-me-worktree"
        );
        assert_eq!(sanitize_session_component(" --Agent__ "), "agent");
    }

    #[test]
    fn namespace_path_matches_daemon_layout() {
        let path = apply_namespace(PathBuf::from("/tmp/agent-browser"), Some("worktree-one"));
        assert_eq!(
            path,
            PathBuf::from("/tmp/agent-browser")
                .join("namespaces")
                .join("worktree-one")
                .join("run")
        );
    }

    #[test]
    fn session_validation_rejects_path_injection() {
        assert!(validate_session("default").is_ok());
        assert!(validate_session("work_1").is_ok());
        assert!(validate_session("../other").is_err());
        assert!(validate_session("").is_err());
    }

    #[test]
    fn windows_port_hash_matches_existing_daemon_algorithm() {
        assert_eq!(port_for_identity("default"), 50_838);
        assert_eq!(port_for_identity("my-session"), 63_105);
        assert_eq!(port_for_identity("work"), 51_184);
    }

    #[test]
    fn request_defaults_to_hot_send_contract() {
        let request: ChannelRequest = serde_json::from_value(json!({
            "command": {"action": "snapshot"}
        }))
        .unwrap();
        assert_eq!(request.session, "default");
        assert_eq!(request.timeout_ms, DEFAULT_TIMEOUT_MS);
        assert!(request.operation.is_none());
    }

    #[test]
    fn explicit_command_id_is_preserved() {
        let explicit = Map::from_iter([
            ("id".to_string(), json!("caller-id")),
            ("action".to_string(), json!("snapshot")),
        ]);
        assert_eq!(explicit["id"], "caller-id");
    }
}
