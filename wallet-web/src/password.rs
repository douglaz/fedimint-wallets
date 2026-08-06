//! Bounded Argon2id provisioning and fail-closed PHC validation (§6c.2).

use anyhow::{anyhow, ensure, Result};
use argon2::password_hash::{PasswordHasher as _, SaltString};
use argon2::{Algorithm, Argon2, Params, PasswordHash, Version};

/// Init uses exactly these costs; startup refuses anything lower.
pub const MIN_M_COST: u32 = 19456;
pub const MIN_T_COST: u32 = 2;
pub const MIN_P_COST: u32 = 1;
const MIN_SALT_BYTES: usize = 8;

pub const MIN_PASSWORD_CHARS: usize = 12;
pub const MAX_PASSWORD_BYTES: usize = 1024;

/// Private field + sole constructor make the hash input bound a type-level invariant. It also
/// deliberately implements no printing or serialization trait.
pub struct ValidatedPassword(String);

impl ValidatedPassword {
    pub fn new(raw: String) -> Result<Self> {
        // Check bytes first so an unbounded input is never walked before refusal.
        ensure!(
            raw.len() <= MAX_PASSWORD_BYTES,
            "password is {} bytes; the maximum is {MAX_PASSWORD_BYTES} (an unbounded password is \
             a CPU denial of service against a memory-hard hash)",
            raw.len()
        );
        ensure!(
            raw.chars().count() >= MIN_PASSWORD_CHARS,
            "password is shorter than the {MIN_PASSWORD_CHARS}-character minimum; it is the only \
             control in front of seed-recovery-level capability (ADR-0028)"
        );
        Ok(Self(raw))
    }
}

/// Hash only a validated password at the pinned Argon2id parameters.
pub fn hash(password: &ValidatedPassword) -> Result<String> {
    let params = Params::new(MIN_M_COST, MIN_T_COST, MIN_P_COST, None)
        .map_err(|error| anyhow!("pinned argon2id parameters rejected: {error}"))?;
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let salt = SaltString::generate(&mut argon2::password_hash::rand_core::OsRng);
    let hashed = argon
        .hash_password(password.0.as_bytes(), &salt)
        .map_err(|error| anyhow!("hashing the password failed: {error}"))?;
    Ok(hashed.to_string())
}

/// Refuse malformed, unverifiable, obsolete, wrong-family, or under-cost PHC strings.
pub fn validate_phc(phc: &str) -> Result<()> {
    let parsed = PasswordHash::new(phc).map_err(|error| {
        anyhow!("password_hash is not a valid PHC string ({error}); re-run `wallet-web init`")
    })?;
    ensure!(
        parsed.algorithm == Algorithm::Argon2id.ident(),
        "password_hash uses {}, but only argon2id is accepted (§6c.2); re-run `wallet-web init`",
        parsed.algorithm
    );
    ensure!(
        parsed.version.unwrap_or(u32::from(Version::V0x13)) == u32::from(Version::V0x13),
        "password_hash uses an obsolete argon2 version; re-run `wallet-web init`"
    );
    let salt = parsed
        .salt
        .ok_or_else(|| anyhow!("password_hash carries no salt; no password could verify"))?;
    let mut salt_bytes = [0_u8; 64];
    let salt_bytes = salt
        .decode_b64(&mut salt_bytes)
        .map_err(|error| anyhow!("password_hash salt is not valid base64 ({error})"))?;
    ensure!(
        salt_bytes.len() >= MIN_SALT_BYTES,
        "password_hash salt is {} bytes; argon2 requires at least {MIN_SALT_BYTES}",
        salt_bytes.len()
    );
    ensure!(
        parsed.hash.is_some(),
        "password_hash carries no hash output; no password could verify against it"
    );
    let params = Params::try_from(&parsed)
        .map_err(|error| anyhow!("password_hash has unreadable argon2 parameters: {error}"))?;
    ensure!(
        params.m_cost() >= MIN_M_COST,
        "password_hash memory cost m={} is below the pinned minimum m={MIN_M_COST}",
        params.m_cost()
    );
    ensure!(
        params.t_cost() >= MIN_T_COST,
        "password_hash time cost t={} is below the pinned minimum t={MIN_T_COST}",
        params.t_cost()
    );
    ensure!(
        params.p_cost() >= MIN_P_COST,
        "password_hash parallelism p={} is below the pinned minimum p={MIN_P_COST}",
        params.p_cost()
    );
    Ok(())
}

#[cfg(test)]
pub(crate) fn test_phc(algorithm: Algorithm, m_cost: u32, t_cost: u32, p_cost: u32) -> String {
    let params = Params::new(m_cost, t_cost, p_cost, None).expect("test params");
    let argon = Argon2::new(algorithm, Version::V0x13, params);
    let salt = SaltString::from_b64("d2FsbGV0d2Vic2FsdA").expect("test salt");
    argon
        .hash_password(b"correct horse battery", &salt)
        .expect("test hash")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_under_twelve_characters_is_rejected() {
        assert!(ValidatedPassword::new("elevenchars".to_owned()).is_err());
        ValidatedPassword::new("twelvechars!".to_owned()).expect("12 characters is the minimum");
    }

    /// `hash` cannot be called here because construction fails; the private field proves no other
    /// call site can bypass that refusal.
    #[test]
    fn oversized_input_is_rejected_and_can_reach_no_hashing() {
        assert!(ValidatedPassword::new("a".repeat(MAX_PASSWORD_BYTES + 1)).is_err());
        ValidatedPassword::new("a".repeat(MAX_PASSWORD_BYTES)).expect("1024 bytes is allowed");
    }

    #[test]
    fn init_hashes_argon2id_at_the_pinned_parameters() -> Result<()> {
        let password = ValidatedPassword::new("a-long-enough-password".to_owned())?;
        let phc = hash(&password)?;
        let parsed = PasswordHash::new(&phc).expect("init produced a parseable PHC string");
        assert_eq!(parsed.algorithm, Algorithm::Argon2id.ident());
        assert_eq!(parsed.version, Some(Version::V0x13.into()));
        let params = Params::try_from(&parsed).expect("params");
        assert_eq!(
            (params.m_cost(), params.t_cost(), params.p_cost()),
            (MIN_M_COST, MIN_T_COST, MIN_P_COST)
        );
        // Two hashes of the same password differ: the salt is freshly generated per hash.
        assert_ne!(phc, hash(&password)?);
        validate_phc(&phc)?;
        Ok(())
    }
}
