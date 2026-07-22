//! The process exit-code taxonomy (spec §6a.7): the §6a.6 error layers map to distinct codes so a
//! scripted caller can tell a decide-time refusal from a driver failure from an unreachable
//! daemon. The step-7 gates build on these — they are FROZEN once written.
//!
//! 0 success · 1 usage/other · 2 REFUSED (decide-time; nothing journaled) · 3 FAILED (driver
//! terminal; message carries the operation key) · 4 TRANSPORT (daemon unreachable/timeout —
//! includes the not-running case) · 5 AUTH (401 bad/missing token).

/// A terminal CLI outcome carrying its exit code + a stderr message. `Ok(())` is exit 0.
#[derive(Debug)]
pub enum CliExit {
    /// A usage error, a bad argument, or any uncategorized failure → exit 1.
    Usage(anyhow::Error),
    /// A decide-time refusal (`RefuseReason`): nothing was journaled, safe to retry → exit 2.
    Refused(String),
    /// A journaled operation's terminal failure; the message carries the operation key → exit 3.
    Failed(String),
    /// The daemon is unreachable, a request/await deadline elapsed, or the client is not
    /// initialized (missing pointer/token) → exit 4.
    Transport(String),
    /// A missing or invalid bearer token (HTTP 401) → exit 5.
    Auth(String),
}

impl CliExit {
    /// The pinned exit code for this outcome.
    pub fn code(&self) -> i32 {
        match self {
            CliExit::Usage(_) => 1,
            CliExit::Refused(_) => 2,
            CliExit::Failed(_) => 3,
            CliExit::Transport(_) => 4,
            CliExit::Auth(_) => 5,
        }
    }

    /// The stderr message (a leading tag makes the taxonomy layer visible in logs).
    pub fn message(&self) -> String {
        match self {
            CliExit::Usage(error) => format!("{error:#}"),
            CliExit::Refused(message) => format!("refused: {message}"),
            CliExit::Failed(message) => format!("failed: {message}"),
            CliExit::Transport(message) => message.clone(),
            CliExit::Auth(message) => format!("auth error: {message}"),
        }
    }
}

impl From<anyhow::Error> for CliExit {
    /// A bare `anyhow` failure is a usage/other error (exit 1); the taxonomy variants are
    /// constructed explicitly where the layer is known.
    fn from(error: anyhow::Error) -> Self {
        CliExit::Usage(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_pinned_per_taxonomy_layer() {
        assert_eq!(CliExit::Usage(anyhow::anyhow!("x")).code(), 1);
        assert_eq!(CliExit::Refused("x".to_owned()).code(), 2);
        assert_eq!(CliExit::Failed("x".to_owned()).code(), 3);
        assert_eq!(CliExit::Transport("x".to_owned()).code(), 4);
        assert_eq!(CliExit::Auth("x".to_owned()).code(), 5);
    }
}
