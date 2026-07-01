//! wallet-fedimint identity newtypes (spec §3).
//!
//! Pure wrappers — no fedimint SDK in this step. The doc lines record how each value
//! parses into its fedimint counterpart in a LATER step, so the intent is unambiguous
//! when the SDK lands; nothing here pulls fedimint in yet.

/// A fedimint operation's 32-byte identity. Bridges `fedimint_core::core::OperationId`
/// (later step). The deterministic op-id is the client's own send-dedup anchor, so it
/// is the durable handle we record in a [`crate::move_protocol::MoveRecord`].
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct OperationId(pub [u8; 32]);

/// A Lightning payment preimage (32 bytes) — proof a send leg settled.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Preimage(pub [u8; 32]);

/// A gateway endpoint URL. Parses to a fedimint `SafeUrl` via `SafeUrl::parse(&self.0)`
/// in a later step. Pinned in the durable intent so a resumed move never reselects a
/// different gateway after a crash (spec §3.1/§4).
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct GatewayUrl(pub String);

/// A BOLT11 invoice string. Parses to a `Bolt11Invoice` via `FromStr` in a later step.
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Invoice(pub String);
