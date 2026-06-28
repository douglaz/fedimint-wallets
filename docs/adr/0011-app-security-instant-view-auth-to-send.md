---
status: accepted
---
# App security: hardware-backed key, instant view, biometric-to-send

The local database (holding the seed and ecash) is encrypted with a key in the
Android Keystore (hardware-backed, non-extractable). The key is **usable by the
app for reading**, so the wallet opens instantly to the balance (WoS-like), but
the **send action requires a fresh biometric / lockscreen authentication**. "No
lock" and "lock on open" are offered as settings for users at either extreme.

## Consequences

- Threat model: protected against offline DB extraction / device cloning (the key
  never leaves hardware; the device is required). NOT protected against *viewing*
  the balance on an already-unlocked phone, but such a viewer cannot SPEND without
  passing the biometric gate.
- Instant view keeps the WoS feel; the single biometric tap is paid only at the
  moment of spending, where users tolerate it.
- The seed never leaves `wallet-core` for the UI except on an explicit, separately
  authenticated "export seed" action.
