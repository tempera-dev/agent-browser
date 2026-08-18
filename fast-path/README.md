# Tempera Browser Fast Path

This crate is a bounded JSONL gateway for Tempera Browser's canonical daemon. It accelerates high-frequency agent clients without becoming a second source of browser truth.

## What it changes

- Keeps one upstream daemon connection hot for each downstream connection.
- Enables `TCP_NODELAY` on both sides.
- Coalesces concurrent identical observation requests.
- Micro-caches read-only observations for at most 8 ms by default.
- Invalidates all cached observations before any mutating request.
- Implements a client-side `actObserve` envelope that performs an action and the following observation over the same upstream connection.
- Reconnects once after a broken upstream connection.
- Bounds every JSONL request and response.

The gateway never caches actions, never reorders mutations, never invents state, and never extends the observation cache beyond 100 ms.

## Run

```bash
cargo run --manifest-path fast-path/Cargo.toml --release -- \
  --listen 127.0.0.1:7419 \
  --upstream 127.0.0.1:7420 \
  --observe-ttl-ms 8
```

Environment equivalents:

```text
TEMPERA_BROWSER_FASTPATH_LISTEN
TEMPERA_BROWSER_FASTPATH_UPSTREAM
TEMPERA_BROWSER_FASTPATH_TTL_MS
```

## Fused request

```json
{
  "name": "actObserve",
  "arguments": {
    "actionRequest": {
      "id": "action-1",
      "sessionId": "s1",
      "command": {"name": "tap", "arguments": {"selector": "@e7"}}
    },
    "observeRequest": {
      "id": "observe-1",
      "sessionId": "s1",
      "command": {"name": "snapshot"}
    }
  }
}
```

Both nested requests remain canonical daemon commands. The action response is returned even when the subsequent observation fails.

## Performance contract

This is useful only when measured. The initial budgets are:

- Gateway overhead at p50: under 250 microseconds on loopback.
- Gateway overhead at p99: under 2 milliseconds on loopback.
- Cache invalidation: synchronous before forwarding a mutation.
- Observation TTL: 8 ms default, 100 ms hard maximum.
- No mutation caching or retries after a response may have been executed.

The single reconnect retry applies only when the connection fails before a response is received. Callers must continue to use action IDs and revision/state guards for at-most-once semantics.
