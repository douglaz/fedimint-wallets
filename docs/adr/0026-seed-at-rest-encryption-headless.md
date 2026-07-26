---
status: accepted
---
# Seed-at-rest encryption for the headless daemon (Phase 7 kickoff)

## Context

The wallet's root secret — the 12-word BIP-39 seed — is persisted as **plaintext**
entropy inside `client.db`: the join and restore paths call
`Client::store_encodable_client_secret(db, mnemonic.to_entropy())`
(`wallet-daemon/src/main.rs`) with no application-level encryption. Anyone who can
read the daemon's data directory owns the funds. This is the single biggest
real-sats risk for the pilot (codex + fable rated it P1), and closing it is a
concrete differentiator: a survey of comparable custodial-feel wallets found none
encrypt the seed at rest.

[ADR-0003](./0003-recovery-silent-backup.md) covers the **mobile** backup story
(Android Block Store, end-to-end encrypted under the device lockscreen key). It does
not apply to the pilot, which is a **headless `walletd` daemon** on a Linux host /
k8s — there is no device lockscreen or Keystore. Encryption-at-rest for the headless
daemon is a separate problem, and this ADR kicks it off.

## Interim mitigation (in place — NOT the fix)

Until the real fix lands, the pilot relies on operational controls documented in the
real-sats pilot runbook:

- Host **full-disk encryption** plus strict single-user isolation on the walletd host.
- A hard **balance ceiling** (~150k sats total across federations) — genuinely
  willing-to-lose. Do not raise the ceiling until seed-at-rest encryption ships.

These reduce but do not remove the risk: a running process, a root user, a host
compromise, or a snapshot/backup of the data directory all still expose the plaintext
seed.

## Options for the real fix (headless)

The daemon must be able to decrypt the seed at start (it needs the root secret to
derive per-federation secrets), yet the seed must not be readable from the data
directory alone. The key therefore has to come from **outside** the encrypted store.
Two shapes:

1. **Operator passphrase-derived key.** The operator supplies a passphrase at daemon
   start (env var from a mounted secret, an interactive prompt, or a file); derive a
   symmetric key with a memory-hard KDF (Argon2id) and encrypt the stored entropy with
   an AEAD (e.g. XChaCha20-Poly1305). *Pro:* no external dependency; standard; works on
   any host. *Con:* the passphrase must reach the process at every (re)start — and a
   plaintext k8s Secret or env var is itself at-rest data on the cluster, so the real
   protection is only as strong as where the passphrase is sourced from. The daemon's
   crash-restart model must not silently defeat it.

2. **KMS/HSM-backed key.** Encrypt the seed with a data key wrapped by an external
   KMS/HSM (cloud KMS, Vault transit, a YubiHSM); the daemon calls the service at start
   to unwrap. *Pro:* the key never lands on the host; strong audit + rotation. *Con:*
   adds an external dependency and infra the pilot does not otherwise need, plus a
   network round-trip in the startup path.

## Decision

**Kick off Phase 7 with this ADR and DEFER the build.** Keep the interim mitigation and
the capped ceiling for the short pilot. Do not ship real funds beyond the ceiling until
encryption-at-rest lands.

**Recommendation to ratify in the implementation bead:** for the headless pilot, prefer
the **operator passphrase-derived key (option 1)**, with the passphrase sourced from an
external secret manager or an interactive unseal — not a plaintext k8s Secret — so that
automated restarts do not re-expose the key. Adopt KMS/HSM (option 2) instead where that
infrastructure already exists and an external dependency in the startup path is
acceptable. Design constraints for whichever is chosen:

- AEAD over the stored entropy; a memory-hard KDF if passphrase-based.
- `walletd mnemonic` export and `restore-mnemonic` keep working (decrypt on demand).
- A clear first-run / unseal UX, and a defined behaviour when the passphrase/key source
  is unavailable at start (fail closed, do not fall back to plaintext).
- A one-time re-encrypt of the existing plaintext store on upgrade (greenfield — a
  migration step, not a serde compat layer).

## Consequences

- The pilot stays capped at the willing-to-lose ceiling until the build lands; this ADR
  does not itself reduce the plaintext-at-rest exposure.
- Whichever mechanism is chosen, the seed's protection reduces to the protection of the
  passphrase/key source — the honest security boundary to state to operators.
- A follow-up implementation bead carries the ratified choice; recovery of PENDING
  operations (not just ecash) and the `mnemonic`/`restore-mnemonic` paths must keep
  working under encryption.
