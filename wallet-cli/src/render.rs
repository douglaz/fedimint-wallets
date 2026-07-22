//! The FROZEN two-phase stdout/stderr contracts (spec §6a.7), single-sourced so client mode and
//! `--standalone` render byte-identically. Ported mechanically from the current `main.rs` print
//! sites (the smoke gates parse these shapes):
//!
//! - phase-1 (pay/move/join): a `<word> <operation key>` line on STDOUT + `key: <operation key>`
//!   on STDERR. `word` is `started` on a fresh admission; `--standalone` (which sees the full
//!   `DecidedOp`) additionally renders `already-in-flight` / `already-paid`. Client mode's `202`
//!   carries only the key, so it always renders `started` — the daemon discards the decide's
//!   dedup/status and cannot convey it (do NOT add an endpoint field to recover it).
//! - receive/direct-inflow: the BOLT11 invoice on STDOUT (the payable result) + `key:` on STDERR.
//! - approve: the federation id on STDOUT + `key:` on STDERR.
//! - await-*: a terminal word on STDOUT (`claimed` / `success` / `done`), or `failed: <error>`;
//!   a terminal failure exits 3 (the durable-failure taxonomy layer), carrying the operation key.
//!
//! CONTRACT CHANGES forced by the async daemon model (deliberate; step-7 ports the smokes to them):
//! the operation KEY replaces the fedimint operation-id everywhere (the id does not exist at
//! `202`/admit time); await-* are keyed by that operation key (no `--fed`); `await-send` prints
//! `success` WITHOUT a preimage (the wire `OperationView` carries none); `move`/`join` are async
//! (a phase-1 line, then `await-*`) where the old standalone was synchronous.

use crate::exit::CliExit;
use wallet_api::OperationStatusDto;

/// Which await verb is rendering — selects the success word only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AwaitVerb {
    Receive,
    Send,
    Move,
}

impl AwaitVerb {
    /// The stdout word for a Succeeded terminal (the frozen shapes: `claimed`/`success`/`done`).
    fn success_word(self) -> &'static str {
        match self {
            AwaitVerb::Receive => "claimed",
            AwaitVerb::Send => "success",
            AwaitVerb::Move => "done",
        }
    }

    /// The operation kinds this await verb may render. A valid key from the WRONG verb (a
    /// receive key handed to `await-send`) must not print `success` — automation gating on the
    /// stdout word would report a payment that never happened.
    fn accepts_kind(self, kind: &str) -> bool {
        match self {
            AwaitVerb::Receive => kind == "receive",
            AwaitVerb::Send => kind == "pay",
            // `await-move` covers move/direct-inflow/join (an agent evacuation is a move row;
            // join is async with the same phase-1-then-await contract — smoke_devimint awaits
            // its join key here).
            AwaitVerb::Move => matches!(kind, "move" | "evacuation" | "direct-inflow" | "join"),
        }
    }

    /// The verb name for the kind-mismatch diagnostic.
    fn name(self) -> &'static str {
        match self {
            AwaitVerb::Receive => "await-receive",
            AwaitVerb::Send => "await-send",
            AwaitVerb::Move => "await-move",
        }
    }
}

/// Print a phase-1 result line (`<word> <key>` to stdout) plus the `key:` handle to stderr.
/// Used by pay/move/join. `word` is chosen by [`phase1_word`] (standalone) or fixed to `started`
/// (client mode). Diagnostics stay on stderr so `X=$(wallet-cli pay …)` captures only the line.
pub fn print_phase1(word: &str, key: &str) {
    println!("{word} {key}");
    eprintln!("key: {key}");
}

/// Print a bare stdout value (an invoice, a federation id) plus the `key:` handle to stderr.
/// Used by receive/direct-inflow (the invoice is the payable result) and approve (the fed id).
pub fn print_value_with_key(value: &str, key: &str) {
    println!("{value}");
    eprintln!("key: {key}");
}

/// The phase-1 word for a `--standalone` decide, which sees the whole `DecidedOp`: a fresh
/// admission (or a manual retry of a terminal-failed op) is `started`; a re-submit that attached
/// to a live in-flight intent is `already-in-flight`; a re-submit of an already-`Done` op is the
/// verb's `done_word` (`already-paid` for pay). Mirrors the old standalone pay's three-way line.
pub fn phase1_word(is_done: bool, deduplicated: bool, done_word: &'static str) -> &'static str {
    if is_done {
        done_word
    } else if deduplicated {
        "already-in-flight"
    } else {
        "started"
    }
}

/// Render an await verb's terminal state: the success word (or `failed: <error>`) on stdout, and
/// the exit outcome. A journaled terminal FAILURE is the durable-failure taxonomy layer (exit 3),
/// carrying the operation key so the caller can `show <key>`/inspect history.
pub fn await_terminal(
    verb: AwaitVerb,
    kind: &str,
    status: OperationStatusDto,
    error: Option<&str>,
    key: &str,
) -> Result<(), CliExit> {
    // Kind gate BEFORE any stdout: a mixed-up key must never print the other verb's success
    // word (both modes route through here, so client and standalone reject identically).
    if !verb.accepts_kind(kind) {
        return Err(CliExit::Usage(anyhow::anyhow!(
            "operation {key} is a `{kind}` operation, not awaitable with `{}`",
            verb.name()
        )));
    }
    match status {
        OperationStatusDto::Succeeded => {
            println!("{}", verb.success_word());
            Ok(())
        }
        OperationStatusDto::Failed => {
            let reason = error.unwrap_or("(no diagnostic)");
            // stdout keeps the scriptable `failed: …` line (its current shape); the process still
            // exits 3 so a caller gating on the exit code never mistakes it for success.
            println!("failed: {reason}");
            Err(CliExit::Failed(format!("operation {key} failed: {reason}")))
        }
        // await_terminal is only called once the operation is terminal; a non-terminal status here
        // is a caller bug. Surface it rather than silently claiming success.
        OperationStatusDto::Started | OperationStatusDto::Awaiting => Err(CliExit::Usage(
            anyhow::anyhow!("operation {key} is not terminal yet (status {status:?})"),
        )),
    }
}
