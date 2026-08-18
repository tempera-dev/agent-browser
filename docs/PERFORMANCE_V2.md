# Browser Runtime Performance V2

Tempera Browser should optimize the complete **observe → decide → act → verify** loop, not a single command in isolation.

## Hot-path architecture

```text
agent client
   │
   ▼
fast-path gateway
   ├─ persistent JSONL connection
   ├─ single-flight identical observations
   ├─ bounded 8 ms observation cache
   └─ fused act-observe envelope
   │
   ▼
canonical per-session daemon
   │
   ├─ persistent CDP/BiDi target connection
   ├─ document-scoped injected semantic runtime
   ├─ event-driven readiness
   └─ versioned action receipts
   │
   ▼
Chromium / browser target
```

The gateway is optional. The daemon remains the authority for target state, action validation, policy, receipts, replay, and evidence.

## Required engine changes

### 1. Never respawn for a hot command

Browser startup, target discovery, WebSocket setup, script injection, and storage loading belong to session creation or reconnect—not each observation.

### 2. Incremental semantic snapshots

A document-scoped runtime should maintain:

- monotonically increasing document revision;
- stable element references scoped to that revision epoch;
- a compact node table;
- mutation deltas since a caller-provided revision;
- a state hash over the canonical compact representation.

Full snapshots remain available for recovery and evidence. Normal turns should request a delta.

### 3. Fused act-observe

One daemon command should:

1. validate expected revision/state hash;
2. execute one typed action;
3. wait on a bounded target event or semantic mutation;
4. return the receipt and resulting snapshot/delta.

This removes an avoidable process/network round trip and prevents clients from observing an unrelated intermediate state.

### 4. Event-driven waiting

Replace fixed sleeps with bounded waits on:

- DOM mutation revision;
- navigation lifecycle events;
- target creation/destruction;
- network-idle policy where explicitly required;
- selector appearance/disappearance;
- animation-frame stabilization for visual interactions.

Every wait must retain a deadline and a deterministic timeout result.

### 5. Payload discipline

- Semantic state by default; screenshots only by explicit escalation.
- Compact node fields and interned repeated strings.
- Deltas after the first snapshot.
- No base64 images in daemon history unless evidence retention is explicitly enabled.
- Bounded history windows summarized outside the control hot path.

### 6. Parallelize only independent work

Safe overlap includes target metadata, policy preflight, and model preparation while waiting for state. Actions, revision transitions, and receipt persistence stay ordered per session.

## Performance budgets

Reference budgets must be published per host/browser build; these are gates, not universal claims.

| Operation | Initial local target |
|---|---:|
| Cached identical observation gateway overhead p50 | < 0.25 ms |
| Gateway overhead p99 | < 2 ms |
| Daemon semantic observation, warm document p50 | < 10 ms |
| Revision validation | < 0.10 ms |
| Fused action-to-result overhead excluding page work p50 | < 5 ms |
| Unnecessary screenshot rate on semantic fixtures | 0% |
| Stale action side effects | 0 |

## Benchmark matrix

Run the same fixture suite through:

- direct daemon;
- fast-path gateway;
- full snapshot;
- delta snapshot;
- sequential action then observe;
- fused act-observe;
- one, eight, and thirty-two concurrent read-only clients;
- warm and reconnect paths.

Report p50, p90, p95, p99, allocations where available, bytes transferred, success rate, stale rejection correctness, and CPU time. A speed claim is invalid when verifier success regresses.

## Android parity

The Android-specific browser uses an instrumented WebView DOM bridge as its primary browser path and the Tempera Android Accessibility bridge as fallback. It must emit the same conceptual revision, state-hash, stable-reference, action, receipt, and fused act-observe contracts so the higher-level browser planner does not fork by platform.
