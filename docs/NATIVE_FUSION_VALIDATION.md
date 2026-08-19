# Native fusion validation

This branch validates the second browser runtime-speed tranche against the canonical daemon rather than treating the optional gateway as a second state authority.

Required invariants:

- mutating requests are at-most-once after transmission begins;
- read-only observations may reconnect and replay once;
- fused action + snapshot executes under one canonical daemon state-lock acquisition;
- digest deltas are transport hints only and never authorization or stale-state guards;
- a mutation invalidates the prior observation identity before execution;
- a reconnect cannot claim an unchanged delta until that connection has observed the same digest;
- old daemons may fall back only after rejecting the internal fused command before any mutation executes;
- latency claims require equal verifier success and zero stale side effects.

The canonical CI matrix is the source of truth for whether this tranche is shippable. The split validation jobs deliberately prove the daemon primitive and gateway routing separately before the temporary patch workflows are removed.
