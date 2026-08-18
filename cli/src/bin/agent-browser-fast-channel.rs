//! Persistent JSONL control channel for an established agent-browser daemon.

use agent_browser::fast_transport::{ChannelPool, FailureStage, SessionKey, MAX_REQUEST_BYTES};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::io::{self, BufRead, Write};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SCHEMA_VERSION: &str = "agent.browser.fast-channel/v1";
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const MAX_TIMEOUT_MS: u64 = 600_000;

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

    fn handle(&mut self, request: Request) -> Value {
        let started = Instant::now();
        let id = request.id.clone();
        let operation = request
            .operation
            .clone()
            .unwrap_or_else(|| "send".to_string());
        let key = match SessionKey::new(&request.session, request.namespace.as_deref()) {
            Ok(key) => key,
            Err(failure) => return error(id, failure.to_string(), false, started),
        };

        match operation.as_str() {
            "ping" => success(
                id,
                json!({
                    "pong": true,
                    "session": request.session,
                    "connected": self.pool.contains(&key),
                }),
                started,
            ),
            "status" => success(
                id,
                json!({
                    "session": request.session,
                    "connected": self.pool.contains(&key),
                    "socketDir": key.socket_dir(),
                }),
                started,
            ),
            "close" => success(
                id,
                json!({
                    "session": request.session,
                    "closed": self.pool.close(&key),
                }),
                started,
            ),
            "send" => self.send(request, key, id, started),
            _ => error(
                id,
                "operation must be send, ping, status, or close",
                false,
                started,
            ),
        }
    }

    fn send(&mut self, request: Request, key: SessionKey, id: Value, started: Instant) -> Value {
        if !(1..=MAX_TIMEOUT_MS).contains(&request.timeout_ms) {
            return error(
                id,
                format!("timeoutMs must be between 1 and {MAX_TIMEOUT_MS}"),
                false,
                started,
            );
        }
        let mut command: Map<String, Value> = match request.command {
            Some(Value::Object(command)) => command,
            Some(_) => return error(id, "command must be a JSON object", false, started),
            None => return error(id, "operation=send requires command", false, started),
        };
        if command.get("action").and_then(Value::as_str).is_none() {
            return error(id, "command.action must be a string", false, started);
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
                    "reusedConnection": outcome.reused_connection,
                    "reconnected": outcome.reconnected,
                },
                "timing": {"roundTripMicros": elapsed_micros(started)},
            }),
            Err(failure) => error(
                id,
                failure.to_string(),
                failure.stage == FailureStage::DeliveryUnknown,
                started,
            ),
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

    loop {
        let line = match read_bounded_line(&mut input) {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(read_error) => {
                let response = error(
                    Value::Null,
                    format!("failed to read channel input: {read_error}"),
                    false,
                    Instant::now(),
                );
                let _ = write_line(&mut output, &response);
                break;
            }
        };
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let response = match serde_json::from_slice::<Request>(&line) {
            Ok(request) => server.handle(request),
            Err(parse_error) => error(
                Value::Null,
                format!("invalid request JSON: {parse_error}"),
                false,
                Instant::now(),
            ),
        };
        if write_line(&mut output, &response).is_err() {
            break;
        }
    }
}

fn read_bounded_line(reader: &mut impl BufRead) -> io::Result<Option<Vec<u8>>> {
    let mut output = Vec::new();
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            return if output.is_empty() {
                Ok(None)
            } else {
                Ok(Some(output))
            };
        }
        let take = buffer
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(buffer.len(), |position| position + 1);
        if output.len().saturating_add(take) > MAX_REQUEST_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("request exceeded {} MiB", MAX_REQUEST_BYTES / (1024 * 1024)),
            ));
        }
        output.extend_from_slice(&buffer[..take]);
        reader.consume(take);
        if output.last() == Some(&b'\n') {
            return Ok(Some(output));
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
        "timing": {"roundTripMicros": elapsed_micros(started)},
    })
}

fn error(id: Value, message: impl Into<String>, delivery_unknown: bool, started: Instant) -> Value {
    json!({
        "schemaVersion": SCHEMA_VERSION,
        "id": id,
        "ok": false,
        "error": message.into(),
        "deliveryUnknown": delivery_unknown,
        "timing": {"roundTripMicros": elapsed_micros(started)},
    })
}

fn elapsed_micros(started: Instant) -> u128 {
    started.elapsed().as_micros()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

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

    #[test]
    fn bounded_reader_preserves_one_jsonl_frame() {
        let mut reader = Cursor::new(b"{\"operation\":\"ping\"}\nsecond\n".to_vec());
        let first = read_bounded_line(&mut reader).unwrap().unwrap();
        let second = read_bounded_line(&mut reader).unwrap().unwrap();
        assert_eq!(first, b"{\"operation\":\"ping\"}\n");
        assert_eq!(second, b"second\n");
    }
}
