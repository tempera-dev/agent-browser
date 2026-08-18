# Fast control channel

`agent-browser-fast-channel` is an internal, versioned JSONL transport for latency-sensitive orchestrators such as `tempera-use`.

It does **not** replace the normal `agent-browser` CLI. The CLI remains responsible for installation, launch configuration, policy, interactive help, daemon startup, and compatibility. The fast channel is used after a session daemon exists.

## Why it exists

The daemon already supports multiple newline-delimited commands over one connection. A long-lived orchestrator should not pay for a new child CLI process, process-output reader threads, polling, and a fresh socket handshake for every observation or action.

The fast channel keeps one connection per `(namespace, session)` and reconnects at most once after a broken transport. It never loops or blindly replays a command.

## Start a session

```bash
agent-browser --session phone-handoff open about:blank
```

Then start the resident channel:

```bash
agent-browser-fast-channel
```

Send one JSON object per line:

```json
{"id":"1","session":"phone-handoff","command":{"action":"snapshot"}}
{"id":"2","session":"phone-handoff","command":{"action":"get_url"}}
```

Every response uses `agent.browser.fast-channel/v1` and includes measured `roundTripMicros`, whether the connection was reused, and whether a one-time reconnect occurred.

## Local operations

The following operations do not touch the browser:

```json
{"id":"health","operation":"ping","session":"phone-handoff"}
{"id":"state","operation":"status","session":"phone-handoff"}
{"id":"close","operation":"close","session":"phone-handoff"}
```

`close` drops only the fast-channel connection. It does not close the browser or daemon.

## Safety boundaries

- Session names are validated before filesystem or port resolution.
- Namespaces are normalized with the same layout as the daemon.
- Requests are capped at 4 MiB; responses at 32 MiB.
- Timeouts are bounded to 1–600,000 ms.
- The channel does not persist, log, or reinterpret command payloads.
- A failed connection is recreated once. A failed daemon response is returned as-is.
- This transport does not grant additional browser authority. Policy and action semantics remain in the daemon and the upstream orchestrator.

## Benchmark method

Measure warm commands against the same running daemon and browser state:

1. Establish one session with the normal CLI.
2. Discard at least five warm-up commands.
3. Run at least 100 read-only commands through the normal MCP/CLI path and the fast channel.
4. Record raw wall-clock samples, median, p95, p99, failures, payload sizes, and host details.
5. Compare identical daemon actions. Do not claim a multiplier from process-startup-only microbenchmarks.

The fast channel reports its own transport round trip, but no performance claim should be published until the hosted and real-host benchmark artifacts exist.
