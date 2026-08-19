use serde_json::{json, Value};
use std::collections::HashMap;
use std::env;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct Config {
    listen: String,
    upstream: String,
    observe_ttl: Duration,
    max_line_bytes: usize,
}

#[derive(Default)]
struct Stats {
    requests: AtomicU64,
    forwarded: AtomicU64,
    cache_hits: AtomicU64,
    coalesced_hits: AtomicU64,
    fused_requests: AtomicU64,
    reconnects: AtomicU64,
    failures: AtomicU64,
}

impl Stats {
    fn snapshot(&self) -> Value {
        json!({
            "schemaVersion": "tempera.browser.fastpath.stats/v1",
            "requests": self.requests.load(Ordering::Relaxed),
            "forwarded": self.forwarded.load(Ordering::Relaxed),
            "cacheHits": self.cache_hits.load(Ordering::Relaxed),
            "coalescedHits": self.coalesced_hits.load(Ordering::Relaxed),
            "fusedRequests": self.fused_requests.load(Ordering::Relaxed),
            "reconnects": self.reconnects.load(Ordering::Relaxed),
            "failures": self.failures.load(Ordering::Relaxed),
        })
    }
}

#[derive(Clone)]
struct CachedResponse {
    inserted: Instant,
    line: String,
}

#[derive(Default)]
struct SharedState {
    cache: Mutex<HashMap<String, CachedResponse>>,
    key_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    stats: Stats,
}

fn main() -> io::Result<()> {
    let config = parse_args()?;
    let listener = TcpListener::bind(&config.listen)?;
    let state = Arc::new(SharedState::default());
    eprintln!(
        "tempera-browser-fastpath listening={} upstream={} observe_ttl_ms={}",
        config.listen,
        config.upstream,
        config.observe_ttl.as_millis()
    );

    for connection in listener.incoming() {
        match connection {
            Ok(stream) => {
                stream.set_nodelay(true)?;
                let config = config.clone();
                let state = Arc::clone(&state);
                thread::spawn(move || {
                    if let Err(error) = handle_client(stream, &config, &state) {
                        state.stats.failures.fetch_add(1, Ordering::Relaxed);
                        eprintln!("fast-path client failed: {error}");
                    }
                });
            }
            Err(error) => {
                state.stats.failures.fetch_add(1, Ordering::Relaxed);
                eprintln!("fast-path accept failed: {error}");
            }
        }
    }
    Ok(())
}

fn parse_args() -> io::Result<Config> {
    let mut listen = env::var("TEMPERA_BROWSER_FASTPATH_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:7419".to_string());
    let mut upstream = env::var("TEMPERA_BROWSER_FASTPATH_UPSTREAM")
        .unwrap_or_else(|_| "127.0.0.1:7420".to_string());
    let mut ttl_ms = env::var("TEMPERA_BROWSER_FASTPATH_TTL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(8);
    let mut max_line_bytes = 4 * 1024 * 1024usize;

    let mut args = env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--listen" => listen = required_value(&mut args, "--listen")?,
            "--upstream" => upstream = required_value(&mut args, "--upstream")?,
            "--observe-ttl-ms" => {
                ttl_ms = required_value(&mut args, "--observe-ttl-ms")?
                    .parse()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid TTL"))?;
            }
            "--max-line-bytes" => {
                max_line_bytes = required_value(&mut args, "--max-line-bytes")?
                    .parse()
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "invalid line limit")
                    })?;
            }
            "--help" | "-h" => {
                println!(
                    "tempera-browser-fastpath\n\n\
                     --listen ADDRESS           downstream JSONL listener\n\
                     --upstream ADDRESS         canonical browser daemon\n\
                     --observe-ttl-ms N         micro-cache window, default 8\n\
                     --max-line-bytes N         bounded request/response size\n"
                );
                std::process::exit(0);
            }
            unknown => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument: {unknown}"),
                ));
            }
        }
    }
    if ttl_ms > 100 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "observe TTL must be <= 100 ms",
        ));
    }
    if !(1024..=64 * 1024 * 1024).contains(&max_line_bytes) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "max line bytes must be between 1 KiB and 64 MiB",
        ));
    }
    Ok(Config {
        listen,
        upstream,
        observe_ttl: Duration::from_millis(ttl_ms),
        max_line_bytes,
    })
}

fn required_value(args: &mut impl Iterator<Item = String>, flag: &str) -> io::Result<String> {
    args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{flag} requires a value"),
        )
    })
}

fn handle_client(
    downstream: TcpStream,
    config: &Config,
    state: &Arc<SharedState>,
) -> io::Result<()> {
    let reader_stream = downstream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut writer = BufWriter::new(downstream);
    let mut upstream = Upstream::new(config.upstream.clone(), config.max_line_bytes);
    let mut line = String::new();

    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            return Ok(());
        }
        if read > config.max_line_bytes {
            write_error(&mut writer, "request exceeds fast-path line limit")?;
            continue;
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        state.stats.requests.fetch_add(1, Ordering::Relaxed);
        let request: Value = match serde_json::from_str(trimmed) {
            Ok(request) => request,
            Err(error) => {
                write_error(&mut writer, &format!("request is not JSON: {error}"))?;
                continue;
            }
        };

        let response = if command_name(&request).as_deref() == Some("gatewayStats") {
            json!({"ok": true, "result": state.stats.snapshot()}).to_string()
        } else if command_name(&request).as_deref() == Some("actObserve") {
            state.stats.fused_requests.fetch_add(1, Ordering::Relaxed);
            invalidate_cache(state);
            handle_fused(&request, &mut upstream, state)?
        } else if is_observation(&request) {
            handle_observation(trimmed, &request, &mut upstream, config, state)?
        } else {
            invalidate_cache(state);
            forward_mutating(trimmed, &mut upstream, state)?
        };

        writer.write_all(response.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;
    }
}

fn handle_observation(
    raw: &str,
    request: &Value,
    upstream: &mut Upstream,
    config: &Config,
    state: &Arc<SharedState>,
) -> io::Result<String> {
    let key = observation_key(request);
    if let Some(hit) = cached(&key, config.observe_ttl, state) {
        state.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
        return Ok(hit);
    }

    let key_lock = {
        let mut locks = state.key_locks.lock().expect("key lock map poisoned");
        Arc::clone(
            locks
                .entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    };
    let _guard = key_lock.lock().expect("observation key lock poisoned");

    if let Some(hit) = cached(&key, config.observe_ttl, state) {
        state.stats.coalesced_hits.fetch_add(1, Ordering::Relaxed);
        return Ok(hit);
    }

    let response = forward_readonly(raw, upstream, state)?;
    state.cache.lock().expect("cache poisoned").insert(
        key,
        CachedResponse {
            inserted: Instant::now(),
            line: response.clone(),
        },
    );
    Ok(response)
}

fn handle_fused(
    request: &Value,
    upstream: &mut Upstream,
    state: &Arc<SharedState>,
) -> io::Result<String> {
    let arguments = request
        .get("arguments")
        .and_then(Value::as_object)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing arguments"))?;
    let action = arguments
        .get("actionRequest")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing actionRequest"))?;
    let observe = arguments
        .get("observeRequest")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing observeRequest"))?;

    let native_request = json!({
        "id": request.get("id").cloned().unwrap_or(Value::Null),
        "action": "__tempera_act_observe_v1",
        "actionRequest": action,
        "observeRequest": observe,
    });
    let native_response = forward_mutating(&native_request.to_string(), upstream, state)?;
    let parsed_native: Value = serde_json::from_str(&native_response).unwrap_or(Value::Null);
    let native_protocol_error = parsed_native
        .pointer("/data/nativeProtocolError")
        .and_then(Value::as_bool)
        == Some(true)
        || parsed_native
            .get("error")
            .and_then(Value::as_str)
            .is_some_and(|error| error.contains("Unknown action"));

    if !native_protocol_error {
        let data = parsed_native.get("data").cloned().unwrap_or(Value::Null);
        return Ok(json!({
            "schemaVersion": "tempera.browser.fastpath.act-observe/v1",
            "ok": parsed_native
                .get("success")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            "action": data.get("action").cloned().unwrap_or(Value::Null),
            "observation": data
                .get("observation")
                .cloned()
                .unwrap_or(Value::Null),
            "observationDigest": data
                .get("observationDigest")
                .cloned()
                .unwrap_or(Value::Null),
            "nativeFused": true,
        })
        .to_string());
    }

    // Compatibility only. An old daemon may reject the unknown
    // internal command before any mutation runs. The real action is
    // then sent exactly once; its transport failure is never replayed.
    let action_response = forward_mutating(&action.to_string(), upstream, state)?;
    let parsed_action: Value = serde_json::from_str(&action_response).unwrap_or(Value::Null);
    if parsed_action.get("success").and_then(Value::as_bool) == Some(false)
        || parsed_action.get("ok").and_then(Value::as_bool) == Some(false)
    {
        return Ok(json!({
            "schemaVersion": "tempera.browser.fastpath.act-observe/v1",
            "ok": false,
            "action": parsed_action,
            "observation": null,
            "nativeFused": false,
        })
        .to_string());
    }

    let observation_response = forward_readonly(&observe.to_string(), upstream, state)?;
    let parsed_observation: Value =
        serde_json::from_str(&observation_response).unwrap_or(Value::Null);
    Ok(json!({
        "schemaVersion": "tempera.browser.fastpath.act-observe/v1",
        "ok": parsed_observation
            .get("success")
            .or_else(|| parsed_observation.get("ok"))
            .and_then(Value::as_bool)
            .unwrap_or(true),
        "action": parsed_action,
        "observation": parsed_observation,
        "nativeFused": false,
    })
    .to_string())
}

fn forward_mutating(
    raw: &str,
    upstream: &mut Upstream,
    state: &Arc<SharedState>,
) -> io::Result<String> {
    state.stats.forwarded.fetch_add(1, Ordering::Relaxed);
    match upstream.round_trip(raw) {
        Ok(response) => Ok(response),
        Err(error) => {
            // Once bytes may have reached the canonical daemon, retry
            // would violate at-most-once browser action semantics.
            upstream.disconnect();
            Err(io::Error::new(
                error.kind(),
                format!(
                    "upstream mutation delivery may be unknown; request was not replayed: {error}"
                ),
            ))
        }
    }
}

fn forward_readonly(
    raw: &str,
    upstream: &mut Upstream,
    state: &Arc<SharedState>,
) -> io::Result<String> {
    state.stats.forwarded.fetch_add(1, Ordering::Relaxed);
    match upstream.round_trip(raw) {
        Ok(response) => Ok(response),
        Err(first) => {
            state.stats.reconnects.fetch_add(1, Ordering::Relaxed);
            upstream.disconnect();
            upstream.round_trip(raw).map_err(|second| {
                io::Error::new(
                    second.kind(),
                    format!(
                        "read-only upstream failed after reconnect: first={first}; second={second}"
                    ),
                )
            })
        }
    }
}

fn cached(key: &str, ttl: Duration, state: &Arc<SharedState>) -> Option<String> {
    let mut cache = state.cache.lock().expect("cache poisoned");
    let entry = cache.get(key)?;
    if entry.inserted.elapsed() <= ttl {
        Some(entry.line.clone())
    } else {
        cache.remove(key);
        None
    }
}

fn invalidate_cache(state: &Arc<SharedState>) {
    state.cache.lock().expect("cache poisoned").clear();
}

fn command_name(request: &Value) -> Option<String> {
    request
        .get("command")
        .and_then(|command| {
            command
                .get("name")
                .or_else(|| command.get("type"))
                .or_else(|| command.get("command"))
        })
        .or_else(|| request.get("name"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn is_observation(request: &Value) -> bool {
    matches!(
        command_name(request).as_deref(),
        Some("snapshot" | "state" | "find" | "observe" | "getState")
    )
}

fn observation_key(request: &Value) -> String {
    let session = request
        .get("sessionId")
        .or_else(|| request.get("session_id"))
        .and_then(Value::as_str)
        .unwrap_or("default");
    let target = request
        .get("targetId")
        .or_else(|| request.get("target_id"))
        .and_then(Value::as_str)
        .unwrap_or("default");
    format!("{session}\u{1f}{target}\u{1f}{request}")
}

fn write_error(writer: &mut BufWriter<TcpStream>, message: &str) -> io::Result<()> {
    let response = json!({
        "schemaVersion": "tempera.browser.fastpath.error/v1",
        "ok": false,
        "error": message
    });
    writer.write_all(response.to_string().as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()
}

struct Upstream {
    address: String,
    max_line_bytes: usize,
    connection: Option<(BufReader<TcpStream>, BufWriter<TcpStream>)>,
}

impl Upstream {
    fn new(address: String, max_line_bytes: usize) -> Self {
        Self {
            address,
            max_line_bytes,
            connection: None,
        }
    }

    fn disconnect(&mut self) {
        self.connection = None;
    }

    fn connect(&mut self) -> io::Result<()> {
        let stream = TcpStream::connect(&self.address)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(45)))?;
        stream.set_write_timeout(Some(Duration::from_secs(45)))?;
        let reader = BufReader::new(stream.try_clone()?);
        let writer = BufWriter::new(stream);
        self.connection = Some((reader, writer));
        Ok(())
    }

    fn round_trip(&mut self, request: &str) -> io::Result<String> {
        if self.connection.is_none() {
            self.connect()?;
        }
        let (reader, writer) = self.connection.as_mut().expect("connected");
        writer.write_all(request.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;

        let mut response = String::new();
        let read = reader.read_line(&mut response)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "browser daemon closed the connection",
            ));
        }
        if read > self.max_line_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "browser daemon response exceeds line limit",
            ));
        }
        Ok(response.trim_end_matches(['\r', '\n']).to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observation_detection_is_bounded() {
        assert!(is_observation(&json!({"command": {"name": "snapshot"}})));
        assert!(is_observation(&json!({"name": "find"})));
        assert!(!is_observation(&json!({"command": {"name": "tap"}})));
    }

    #[test]
    fn observation_key_separates_sessions_and_targets() {
        let left = observation_key(&json!({
            "sessionId": "a",
            "targetId": "one",
            "command": {"name": "snapshot"}
        }));
        let right = observation_key(&json!({
            "sessionId": "b",
            "targetId": "one",
            "command": {"name": "snapshot"}
        }));
        assert_ne!(left, right);
    }

    #[test]
    fn ttl_is_rejected_when_it_can_hide_real_state() {
        let saved = env::var("TEMPERA_BROWSER_FASTPATH_TTL_MS").ok();
        env::set_var("TEMPERA_BROWSER_FASTPATH_TTL_MS", "101");
        let result = parse_args();
        if let Some(value) = saved {
            env::set_var("TEMPERA_BROWSER_FASTPATH_TTL_MS", value);
        } else {
            env::remove_var("TEMPERA_BROWSER_FASTPATH_TTL_MS");
        }
        assert!(result.is_err());
    }
}
