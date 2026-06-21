//! trains-ao — AgentOrchestrator adapter for `trains-core`.
//!
//! Maps AO inbox JSON envelopes → `trains_core::Input`, and
//! `trains_core::Output` → AO outbox JSON envelopes.  The intent is
//! that TRAINS can run as an AO node alongside the rest of the
//! agentic orchestrator: the application layer broadcasts data via
//! AO envelopes, deliveries surface as outbox messages.
//!
//! This crate is intentionally **transport-agnostic**: it does not
//! own a `RingTransport`. The AO runtime owns the transport and
//! forwards train wire-bytes to/from this adapter via the inbox/
//! outbox channels.  Use [`adapter::AoTrainsNode::feed`] to push
//! envelopes in and collect output envelopes in return.

pub mod adapter;
pub mod envelope;

pub use adapter::AoTrainsNode;
pub use envelope::{Envelope, EnvelopeError, EnvelopeKind};
