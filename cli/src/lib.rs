//! Reusable agent-browser runtime contracts for latency-sensitive adapters.
//!
//! The main CLI remains the product and lifecycle surface. This library keeps
//! only transport primitives that sibling binaries can reuse without importing
//! browser-rendering or orchestration policy into another repository.

pub mod fast_transport;
