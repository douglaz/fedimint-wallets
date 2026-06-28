---
status: accepted
---
# No synced social contacts; local recents only

Sending uses scan/paste (Lightning address, invoice, LNURL) plus an on-device,
never-synced list of recent recipients. We deliberately do NOT build a synced
social/contacts layer (e.g. Nostr graph sync, as ecash-app and vipr do), because
it would leak the user's payment/social graph to whoever serves it — exactly the
exposure a privacy wallet exists to avoid. Local recents stay on the device.

## Consequences

- "Pay the same person again" is a one-tap local-recents action, fully private.
- No backend learns who a user transacts with.
- A social/contacts layer, if ever added, must be an explicit opt-in, never a
  default, and is out of scope indefinitely.
