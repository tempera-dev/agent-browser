//! Low-latency MCP core for agent-browser.
//!
//! This server preserves the established `agent_browser_*` tool names for the
//! high-frequency browser loop while dispatching directly over one persistent
//! daemon connection. The full `agent-browser mcp` server remains the broad
//! compatibility surface for state, auth, files, plugins, and debug tooling.

use agent_browser::fast_transport::{
    ChannelFailure, ChannelPool, FailureStage, SessionKey, MAX_REQUEST_BYTES,
};
use serde_json::{json, Map, Value};
use std::env;
use std::io::{self, BufRead, Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PROTOCOL_VERSION: &str = "2025-11-25";
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] =
    &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const MAX_TIMEOUT_MS: u64 = 600_000;
const MAX_BOOTSTRAP_OUTPUT_BYTES: u64 = 32 * 1024 * 1024;
const MAX_SELECTOR_BYTES: usize = 16 * 1024;
const MAX_TEXT_BYTES: usize = 1024 * 1024;
const MAX_URL_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone)]
struct Config {
    session: String,
    namespace: Option<String>,
    timeout_ms: u64,
    bootstrap: bool,
}

impl Config {
    fn from_environment_and_args() -> Result<Self, String> {
        let mut session = env::var("AGENT_BROWSER_SESSION").unwrap_or_else(|_| "default".into());
        let mut namespace = env::var("AGENT_BROWSER_NAMESPACE").ok();
        let mut timeout_ms = env::var("AGENT_BROWSER_MCP_TIMEOUT")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_TIMEOUT_MS);
        let mut bootstrap = env::var("AGENT_BROWSER_FAST_MCP_BOOTSTRAP")
            .ok()
            .map(|value| !matches!(value.to_ascii_lowercase().as_str(), "0" | "false" | "no"))
            .unwrap_or(true);

        let args = env::args().skip(1).collect::<Vec<_>>();
        let mut index = 0usize;
        while index < args.len() {
            match args[index].as_str() {
                "--session" => {
                    session = args
                        .get(index + 1)
                        .cloned()
                        .ok_or_else(|| "--session requires a value".to_string())?;
                    index += 1;
                }
                "--namespace" => {
                    namespace = Some(
                        args.get(index + 1)
                            .cloned()
                            .ok_or_else(|| "--namespace requires a value".to_string())?,
                    );
                    index += 1;
                }
                "--timeout-ms" => {
                    timeout_ms = args
                        .get(index + 1)
                        .ok_or_else(|| "--timeout-ms requires a value".to_string())?
                        .parse::<u64>()
                        .map_err(|_| "--timeout-ms must be an integer".to_string())?;
                    index += 1;
                }
                "--no-bootstrap" => bootstrap = false,
                "--help" | "-h" => {
                    eprintln!(
                        "agent-browser-fast-mcp [--session NAME] [--namespace NAME] [--timeout-ms MS] [--no-bootstrap]"
                    );
                    std::process::exit(0);
                }
                unknown => return Err(format!("unknown fast MCP option: {unknown}")),
            }
            index += 1;
        }

        if !(1..=MAX_TIMEOUT_MS).contains(&timeout_ms) {
            return Err(format!(
                "MCP timeout must be between 1 and {MAX_TIMEOUT_MS} milliseconds"
            ));
        }
        SessionKey::new(&session, namespace.as_deref()).map_err(|error| error.to_string())?;
        Ok(Self {
            session,
            namespace,
            timeout_ms,
            bootstrap,
        })
    }
}

#[derive(Debug)]
struct BuiltCommand {
    session: String,
    timeout_ms: u64,
    command: Value,
    cli_args: Vec<String>,
    bootstrap_safe: bool,
}

struct Server {
    config: Config,
    pool: ChannelPool,
    sequence: u64,
}

impl Server {
    fn new(config: Config) -> Self {
        Self {
            config,
            pool: ChannelPool::new(),
            sequence: 0,
        }
    }

    fn handle(&mut self, request: Value) -> Option<Value> {
        let id = request.get("id").cloned();
        let method = request.get("method").and_then(Value::as_str)?;
        if id.is_none() {
            return None;
        }
        let id = id.unwrap_or(Value::Null);
        match method {
            "initialize" => Some(rpc_result(id, initialize_result(&request))),
            "ping" => Some(rpc_result(id, json!({}))),
            "tools/list" => Some(rpc_result(id, json!({"tools": tool_specs()}))),
            "tools/call" => Some(self.call_tool(id, request.get("params"))),
            "resources/list" | "prompts/list" => Some(rpc_result(id, json!({})) ),
            _ => Some(rpc_error(id, -32601, format!("Method not found: {method}"))),
        }
    }

    fn call_tool(&mut self, id: Value, params: Option<&Value>) -> Value {
        let params = match params.and_then(Value::as_object) {
            Some(params) => params,
            None => return rpc_error(id, -32602, "tools/call requires object params"),
        };
        let name = match params.get("name").and_then(Value::as_str) {
            Some(name) => name,
            None => return rpc_error(id, -32602, "tools/call requires a tool name"),
        };
        let arguments = params
            .get("arguments")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();

        if name == "agent_browser_fast_status" {
            return match self.status(&arguments) {
                Ok(status) => rpc_result(id, tool_result(status, false)),
                Err(error) => rpc_result(id, tool_result(error_value(error), true)),
            };
        }

        let built = match self.build(name, &arguments) {
            Ok(built) => built,
            Err(error) => return rpc_result(id, tool_result(error_value(error), true)),
        };
        let result = self.execute(built);
        let is_error = result
            .get("success")
            .and_then(Value::as_bool)
            .is_some_and(|success| !success);
        rpc_result(id, tool_result(result, is_error))
    }

    fn status(&self, arguments: &Map<String, Value>) -> Result<Value, String> {
        let session = optional_string(arguments, "session")
            .unwrap_or_else(|| self.config.session.clone());
        let key = SessionKey::new(&session, self.config.namespace.as_deref())
            .map_err(|error| error.to_string())?;
        Ok(json!({
            "success": true,
            "data": {
                "session": session,
                "namespace": self.config.namespace,
                "connected": self.pool.contains(&key),
                "bootstrapEnabled": self.config.bootstrap,
                "transport": "persistent-at-most-once",
            }
        }))
    }

    fn execute(&mut self, mut built: BuiltCommand) -> Value {
        self.sequence = self.sequence.wrapping_add(1);
        if built.command.get("id").is_none() {
            built.command["id"] = json!(self.command_id());
        }
        let key = match SessionKey::new(&built.session, self.config.namespace.as_deref()) {
            Ok(key) => key,
            Err(error) => return channel_error(error),
        };
        match self.pool.send(
            key,
            &built.command,
            Duration::from_millis(built.timeout_ms),
        ) {
            Ok(outcome) => annotate_transport(
                outcome.response,
                json!({
                    "mode": "persistent",
                    "transport": outcome.transport,
                    "reusedConnection": outcome.reused_connection,
                    "reconnectedBeforeSend": outcome.reconnected,
                }),
            ),
            Err(error)
                if error.stage == FailureStage::BeforeSend
                    && self.config.bootstrap
                    && built.bootstrap_safe =>
            {
                match run_bootstrap(
                    &built.session,
                    self.config.namespace.as_deref(),
                    &built.cli_args,
                    built.timeout_ms,
                ) {
                    Ok(response) => annotate_transport(
                        response,
                        json!({
                            "mode": "bootstrap-cli",
                            "reusedConnection": false,
                            "commandReplayed": false,
                        }),
                    ),
                    Err(bootstrap_error) => error_value(format!(
                        "persistent channel unavailable ({error}); bootstrap failed: {bootstrap_error}"
                    )),
                }
            }
            Err(error) if error.stage == FailureStage::BeforeSend && !built.bootstrap_safe => {
                error_value(format!(
                    "{error}; this command contains text/value data and will not be bootstrapped through process arguments—establish the session first with agent-browser"
                ))
            }
            Err(error) => channel_error(error),
        }
    }

    fn build(
        &self,
        name: &str,
        arguments: &Map<String, Value>,
    ) -> Result<BuiltCommand, String> {
        let session = optional_string(arguments, "session")
            .unwrap_or_else(|| self.config.session.clone());
        SessionKey::new(&session, self.config.namespace.as_deref())
            .map_err(|error| error.to_string())?;
        let timeout_ms = optional_u64(arguments, "timeoutMs").unwrap_or(self.config.timeout_ms);
        if !(1..=MAX_TIMEOUT_MS).contains(&timeout_ms) {
            return Err(format!(
                "timeoutMs must be between 1 and {MAX_TIMEOUT_MS}"
            ));
        }

        let (command, cli_args, bootstrap_safe) = match name {
            "agent_browser_open" => {
                let url = bounded_string(arguments, "url", MAX_URL_BYTES)?;
                let normalized = normalize_url(&url)?;
                (
                    json!({"action": "navigate", "url": normalized}),
                    vec!["open".into(), url],
                    true,
                )
            }
            "agent_browser_back" => (json!({"action": "back"}), vec!["back".into()], true),
            "agent_browser_forward" => {
                (json!({"action": "forward"}), vec!["forward".into()], true)
            }
            "agent_browser_reload" => {
                (json!({"action": "reload"}), vec!["reload".into()], true)
            }
            "agent_browser_snapshot" => {
                (json!({"action": "snapshot"}), vec!["snapshot".into()], true)
            }
            "agent_browser_click" => selector_command(arguments, "click", "click", true)?,
            "agent_browser_dblclick" => {
                selector_command(arguments, "dblclick", "dblclick", true)?
            }
            "agent_browser_fill" => {
                let selector = bounded_string(arguments, "selector", MAX_SELECTOR_BYTES)?;
                let value = bounded_string(arguments, "value", MAX_TEXT_BYTES)?;
                (
                    json!({"action": "fill", "selector": selector, "value": value}),
                    vec!["fill".into(), selector, value],
                    false,
                )
            }
            "agent_browser_type" => {
                let selector = bounded_string(arguments, "selector", MAX_SELECTOR_BYTES)?;
                let text = bounded_string(arguments, "text", MAX_TEXT_BYTES)?;
                let clear = optional_bool(arguments, "clear").unwrap_or(false);
                let delay = optional_u64(arguments, "delayMs");
                let mut command = json!({"action": "type", "selector": selector, "text": text});
                let mut cli = vec!["type".into(), selector.clone(), text.clone()];
                if clear {
                    command["clear"] = json!(true);
                    cli.push("--clear".into());
                }
                if let Some(delay) = delay {
                    if delay > 60_000 {
                        return Err("delayMs must be <= 60000".to_string());
                    }
                    command["delay"] = json!(delay);
                    cli.extend(["--delay".into(), delay.to_string()]);
                }
                (command, cli, false)
            }
            "agent_browser_press" => {
                let key = bounded_string(arguments, "key", 256)?;
                (
                    json!({"action": "press", "key": key}),
                    vec!["press".into(), key],
                    true,
                )
            }
            "agent_browser_hover" => selector_command(arguments, "hover", "hover", true)?,
            "agent_browser_focus" => selector_command(arguments, "focus", "focus", true)?,
            "agent_browser_check" => selector_command(arguments, "check", "check", true)?,
            "agent_browser_uncheck" => {
                selector_command(arguments, "uncheck", "uncheck", true)?
            }
            "agent_browser_select" => {
                let selector = bounded_string(arguments, "selector", MAX_SELECTOR_BYTES)?;
                let values = string_or_string_array(arguments, "values", MAX_TEXT_BYTES)?;
                let command_values = if values.len() == 1 {
                    json!(values[0])
                } else {
                    json!(values)
                };
                let mut cli = vec!["select".into(), selector.clone()];
                cli.extend(values.iter().cloned());
                (
                    json!({"action": "select", "selector": selector, "values": command_values}),
                    cli,
                    false,
                )
            }
            "agent_browser_scroll" => {
                let direction = optional_string(arguments, "direction").unwrap_or_else(|| "down".into());
                if !matches!(direction.as_str(), "up" | "down" | "left" | "right") {
                    return Err("direction must be up, down, left, or right".to_string());
                }
                let amount = optional_i64(arguments, "amount").unwrap_or(300);
                if !(-100_000..=100_000).contains(&amount) {
                    return Err("amount must be between -100000 and 100000".to_string());
                }
                let selector = optional_string(arguments, "selector");
                if let Some(selector) = selector.as_deref() {
                    validate_bounded(selector, "selector", MAX_SELECTOR_BYTES)?;
                }
                let mut command = json!({
                    "action": "scroll",
                    "direction": direction,
                    "amount": amount,
                });
                let mut cli = vec!["scroll".into(), direction, amount.to_string()];
                if let Some(selector) = selector {
                    command["selector"] = json!(selector);
                    cli.extend(["--selector".into(), selector]);
                }
                (command, cli, true)
            }
            "agent_browser_get_url" => {
                (json!({"action": "get_url"}), vec!["get".into(), "url".into()], true)
            }
            "agent_browser_get_title" => (
                json!({"action": "get_title"}),
                vec!["get".into(), "title".into()],
                true,
            ),
            "agent_browser_close" => {
                (json!({"action": "close"}), vec!["close".into()], true)
            }
            _ => return Err(format!("unknown fast MCP tool: {name}")),
        };

        Ok(BuiltCommand {
            session,
            timeout_ms,
            command,
            cli_args,
            bootstrap_safe,
        })
    }

    fn command_id(&self) -> String {
        let micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros();
        format!("fast-mcp-{}-{micros}-{}", std::process::id(), self.sequence)
    }
}

fn main() {
    let config = match Config::from_environment_and_args() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("error: {error}");
            std::process::exit(2);
        }
    };
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut output = io::stdout().lock();
    let mut server = Server::new(config);

    loop {
        let line = match read_bounded_line(&mut input) {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(error) => {
                let _ = write_rpc(
                    &mut output,
                    &rpc_error(Value::Null, -32700, error.to_string()),
                );
                break;
            }
        };
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let message: Value = match serde_json::from_slice(&line) {
            Ok(message) => message,
            Err(error) => {
                let _ = write_rpc(
                    &mut output,
                    &rpc_error(Value::Null, -32700, format!("Parse error: {error}")),
                );
                continue;
            }
        };
        if !message.is_object() {
            let _ = write_rpc(
                &mut output,
                &rpc_error(Value::Null, -32600, "Invalid Request"),
            );
            continue;
        }
        if let Some(response) = server.handle(message) {
            if write_rpc(&mut output, &response).is_err() {
                break;
            }
        }
    }
}

fn initialize_result(request: &Value) -> Value {
    let requested = request
        .pointer("/params/protocolVersion")
        .and_then(Value::as_str);
    let protocol = requested
        .filter(|version| SUPPORTED_PROTOCOL_VERSIONS.contains(version))
        .unwrap_or(PROTOCOL_VERSION);
    json!({
        "protocolVersion": protocol,
        "capabilities": {"tools": {"listChanged": false}},
        "serverInfo": {"name": "agent-browser-fast-mcp", "version": env!("CARGO_PKG_VERSION")},
        "instructions": "Low-latency core browser tools over a persistent at-most-once daemon channel. Use agent-browser mcp for the complete compatibility surface.",
    })
}

fn tool_specs() -> Vec<Value> {
    vec![
        tool("agent_browser_fast_status", "Inspect the resident transport without touching the browser.", Map::new(), &[]),
        tool("agent_browser_open", "Navigate the current browser tab.", properties(&[("url", string_schema("URL to open"))]), &["url"]),
        no_arg_tool("agent_browser_back", "Navigate back."),
        no_arg_tool("agent_browser_forward", "Navigate forward."),
        no_arg_tool("agent_browser_reload", "Reload the current page."),
        no_arg_tool("agent_browser_snapshot", "Capture the current semantic browser snapshot."),
        selector_tool("agent_browser_click", "Click a semantic selector or @ref."),
        selector_tool("agent_browser_dblclick", "Double-click a semantic selector or @ref."),
        tool("agent_browser_fill", "Clear and fill a field.", properties(&[("selector", string_schema("Selector or @ref")), ("value", string_schema("Value to fill"))]), &["selector", "value"]),
        tool("agent_browser_type", "Type into a field without exposing text in a child-process argv.", properties(&[("selector", string_schema("Selector or @ref")), ("text", string_schema("Text to type")), ("clear", json!({"type":"boolean"})), ("delayMs", integer_schema(0, 60_000))]), &["selector", "text"]),
        tool("agent_browser_press", "Press a keyboard key.", properties(&[("key", string_schema("Playwright-style key name"))]), &["key"]),
        selector_tool("agent_browser_hover", "Hover a semantic selector or @ref."),
        selector_tool("agent_browser_focus", "Focus a semantic selector or @ref."),
        selector_tool("agent_browser_check", "Check a checkbox or radio control."),
        selector_tool("agent_browser_uncheck", "Uncheck a checkbox."),
        tool("agent_browser_select", "Select one or more option values.", properties(&[("selector", string_schema("Selector or @ref")), ("values", json!({"oneOf":[{"type":"string"},{"type":"array","items":{"type":"string"},"minItems":1}]}))]), &["selector", "values"]),
        tool("agent_browser_scroll", "Scroll the page or a semantic container.", properties(&[("direction", json!({"type":"string","enum":["up","down","left","right"],"default":"down"})), ("amount", integer_schema(-100_000, 100_000)), ("selector", string_schema("Optional selector or @ref"))]), &[]),
        no_arg_tool("agent_browser_get_url", "Read the current page URL."),
        no_arg_tool("agent_browser_get_title", "Read the current page title."),
        no_arg_tool("agent_browser_close", "Close the current browser session."),
    ]
}

fn tool(
    name: &str,
    description: &str,
    mut properties: Map<String, Value>,
    required: &[&str],
) -> Value {
    properties.insert(
        "session".into(),
        string_schema("Optional validated session override"),
    );
    properties.insert(
        "timeoutMs".into(),
        integer_schema(1, MAX_TIMEOUT_MS as i64),
    );
    json!({
        "name": name,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": false,
        }
    })
}

fn no_arg_tool(name: &str, description: &str) -> Value {
    tool(name, description, Map::new(), &[])
}

fn selector_tool(name: &str, description: &str) -> Value {
    tool(
        name,
        description,
        properties(&[("selector", string_schema("Selector or @ref"))]),
        &["selector"],
    )
}

fn properties(entries: &[(&str, Value)]) -> Map<String, Value> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

fn string_schema(description: &str) -> Value {
    json!({"type":"string","description":description})
}

fn integer_schema(minimum: i64, maximum: i64) -> Value {
    json!({"type":"integer","minimum":minimum,"maximum":maximum})
}

fn selector_command(
    arguments: &Map<String, Value>,
    action: &str,
    cli_action: &str,
    bootstrap_safe: bool,
) -> Result<(Value, Vec<String>, bool), String> {
    let selector = bounded_string(arguments, "selector", MAX_SELECTOR_BYTES)?;
    Ok((
        json!({"action": action, "selector": selector}),
        vec![cli_action.to_string(), selector],
        bootstrap_safe,
    ))
}

fn bounded_string(
    arguments: &Map<String, Value>,
    key: &str,
    maximum: usize,
) -> Result<String, String> {
    let value = arguments
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{key} must be a string"))?;
    validate_bounded(value, key, maximum)?;
    Ok(value.to_string())
}

fn validate_bounded(value: &str, key: &str, maximum: usize) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{key} must not be empty"));
    }
    if value.len() > maximum {
        return Err(format!("{key} exceeds its {maximum}-byte limit"));
    }
    if value.contains('\0') {
        return Err(format!("{key} must not contain NUL"));
    }
    Ok(())
}

fn optional_string(arguments: &Map<String, Value>, key: &str) -> Option<String> {
    arguments.get(key).and_then(Value::as_str).map(str::to_string)
}

fn optional_u64(arguments: &Map<String, Value>, key: &str) -> Option<u64> {
    arguments.get(key).and_then(Value::as_u64)
}

fn optional_i64(arguments: &Map<String, Value>, key: &str) -> Option<i64> {
    arguments.get(key).and_then(Value::as_i64)
}

fn optional_bool(arguments: &Map<String, Value>, key: &str) -> Option<bool> {
    arguments.get(key).and_then(Value::as_bool)
}

fn string_or_string_array(
    arguments: &Map<String, Value>,
    key: &str,
    maximum: usize,
) -> Result<Vec<String>, String> {
    let value = arguments
        .get(key)
        .ok_or_else(|| format!("{key} is required"))?;
    let values = if let Some(value) = value.as_str() {
        vec![value.to_string()]
    } else if let Some(values) = value.as_array() {
        if values.is_empty() {
            return Err(format!("{key} must contain at least one value"));
        }
        values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| format!("{key} must contain only strings"))
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        return Err(format!("{key} must be a string or string array"));
    };
    for value in &values {
        validate_bounded(value, key, maximum)?;
    }
    Ok(values)
}

fn normalize_url(url: &str) -> Result<String, String> {
    validate_bounded(url, "url", MAX_URL_BYTES)?;
    if url.chars().any(char::is_control) || url.chars().any(char::is_whitespace) {
        return Err("url must not contain whitespace or control characters".to_string());
    }
    let lower = url.to_ascii_lowercase();
    if lower.starts_with("http://")
        || lower.starts_with("https://")
        || lower.starts_with("about:")
        || lower.starts_with("data:")
        || lower.starts_with("file:")
        || lower.starts_with("chrome-extension://")
        || lower.starts_with("chrome://")
    {
        Ok(url.to_string())
    } else {
        Ok(format!("https://{url}"))
    }
}

fn run_bootstrap(
    session: &str,
    namespace: Option<&str>,
    cli_args: &[String],
    timeout_ms: u64,
) -> Result<Value, String> {
    let executable = agent_browser_executable();
    let mut command = Command::new(&executable);
    command
        .arg("--session")
        .arg(session)
        .arg("--json")
        .args(cli_args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(namespace) = namespace {
        command.arg("--namespace").arg(namespace);
    }
    let mut child = command
        .spawn()
        .map_err(|error| format!("failed to start {}: {error}", executable.display()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "failed to capture bootstrap stdout".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "failed to capture bootstrap stderr".to_string())?;
    let stdout_reader = thread::spawn(move || bounded_read(stdout));
    let stderr_reader = thread::spawn(move || bounded_read(stderr));
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("bootstrap timed out after {timeout_ms} ms"));
            }
            Err(error) => return Err(format!("failed to wait for bootstrap CLI: {error}")),
        }
    };
    let stdout = stdout_reader
        .join()
        .map_err(|_| "bootstrap stdout reader panicked".to_string())??;
    let stderr = stderr_reader
        .join()
        .map_err(|_| "bootstrap stderr reader panicked".to_string())??;
    let stdout_text = String::from_utf8_lossy(&stdout).trim().to_string();
    let stderr_text = String::from_utf8_lossy(&stderr).trim().to_string();
    if !status.success() && stdout_text.is_empty() {
        return Err(if stderr_text.is_empty() {
            format!("bootstrap CLI exited with {status}")
        } else {
            stderr_text
        });
    }
    serde_json::from_str(&stdout_text).map_err(|error| {
        format!(
            "bootstrap CLI returned invalid JSON: {error}; stderr: {}",
            redact_stderr(&stderr_text)
        )
    })
}

fn bounded_read(mut reader: impl Read) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    reader
        .by_ref()
        .take(MAX_BOOTSTRAP_OUTPUT_BYTES + 1)
        .read_to_end(&mut output)
        .map_err(|error| format!("failed to read bootstrap output: {error}"))?;
    if output.len() as u64 > MAX_BOOTSTRAP_OUTPUT_BYTES {
        return Err("bootstrap output exceeded 32 MiB".to_string());
    }
    Ok(output)
}

fn agent_browser_executable() -> PathBuf {
    if let Some(path) = env::var_os("AGENT_BROWSER_CLI") {
        return PathBuf::from(path);
    }
    if let Ok(current) = env::current_exe() {
        let sibling = current.with_file_name(if cfg!(windows) {
            "agent-browser.exe"
        } else {
            "agent-browser"
        });
        if sibling.exists() {
            return sibling;
        }
    }
    PathBuf::from(if cfg!(windows) {
        "agent-browser.exe"
    } else {
        "agent-browser"
    })
}

fn redact_stderr(stderr: &str) -> String {
    let bounded = stderr.chars().take(2_000).collect::<String>();
    if bounded.is_empty() {
        "<empty>".to_string()
    } else {
        bounded
    }
}

fn annotate_transport(response: Value, transport: Value) -> Value {
    match response {
        Value::Object(mut object) => {
            object.insert("_fastTransport".to_string(), transport);
            Value::Object(object)
        }
        other => json!({
            "success": true,
            "data": other,
            "_fastTransport": transport,
        }),
    }
}

fn channel_error(error: ChannelFailure) -> Value {
    json!({
        "success": false,
        "error": error.to_string(),
        "type": if error.stage == FailureStage::DeliveryUnknown {
            "delivery_unknown"
        } else {
            "channel_unavailable"
        },
        "deliveryUnknown": error.stage == FailureStage::DeliveryUnknown,
    })
}

fn error_value(error: impl Into<String>) -> Value {
    json!({"success": false, "error": error.into()})
}

fn tool_result(value: Value, is_error: bool) -> Value {
    let text = serde_json::to_string_pretty(&value)
        .unwrap_or_else(|_| "{\"success\":false,\"error\":\"serialization failed\"}".into());
    json!({
        "content": [{"type": "text", "text": text}],
        "structuredContent": value,
        "isError": is_error,
    })
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn rpc_error(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message.into()},
    })
}

fn write_rpc(writer: &mut impl Write, value: &Value) -> io::Result<()> {
    serde_json::to_writer(&mut *writer, value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    writer.write_all(b"\n")?;
    writer.flush()
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
                "MCP request exceeded 4 MiB",
            ));
        }
        output.extend_from_slice(&buffer[..take]);
        reader.consume(take);
        if output.last() == Some(&b'\n') {
            return Ok(Some(output));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_core_reuses_existing_tool_names() {
        let names = tool_specs()
            .into_iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(str::to_string))
            .collect::<Vec<_>>();
        assert!(names.contains(&"agent_browser_snapshot".to_string()));
        assert!(names.contains(&"agent_browser_click".to_string()));
        assert!(names.contains(&"agent_browser_fill".to_string()));
    }

    #[test]
    fn text_commands_are_never_bootstrap_safe() {
        let server = Server::new(Config {
            session: "default".into(),
            namespace: None,
            timeout_ms: 1000,
            bootstrap: true,
        });
        let fill = server
            .build(
                "agent_browser_fill",
                &properties(&[
                    ("selector", json!("@e1")),
                    ("value", json!("secret")),
                ]),
            )
            .unwrap();
        assert!(!fill.bootstrap_safe);
    }

    #[test]
    fn url_normalization_matches_cli_navigation() {
        assert_eq!(normalize_url("example.com").unwrap(), "https://example.com");
        assert_eq!(normalize_url("about:blank").unwrap(), "about:blank");
        assert!(normalize_url("https://example.com/a b").is_err());
    }

    #[test]
    fn tool_schema_keeps_session_optional() {
        let snapshot = tool_specs()
            .into_iter()
            .find(|tool| tool["name"] == "agent_browser_snapshot")
            .unwrap();
        assert_eq!(
            snapshot.pointer("/inputSchema/properties/session/type"),
            Some(&json!("string"))
        );
    }
}
