---
status: accepted
---
# Recovery: silent backup, no seed-phrase ceremony

This is a spending wallet with small, ephemeral balances (see
[ADR-0001](./0001-allocator-purpose-resilience-not-solvency.md)), and the target
user expects WoS/Blink-grade simplicity (no seed ceremony). Recovery is therefore
**silent by default**: the seed (and the user's joined federation IDs) are saved
via Android **Block Store / Restore Credentials**, which stores them in the
user's Google account, end-to-end encrypted with the device lockscreen key
(Android 9+) so Google holds only ciphertext it cannot read, and restores them
automatically during new-device setup with no sign-in screen. Balances are then
rebuilt from the seed via Fedimint recovery. No 12-word write-down is shown at
onboarding; a manual seed-phrase export is offered as an opt-in for power users.

## Consequences

- E2E backup requires the user to have a device lockscreen set. Without one, the
  backup is not E2E (or may not occur); surface this for users with no lockscreen.
- Recovery is tied to the user's Google account and to Android (Android-only
  product). Losing Google-account access loses the silent backup; the manual
  export is the escape hatch.
- Use Block Store's explicit/immediate writes (back up the seed the moment it is
  generated), avoiding Auto Backup's ~24h timing window.
- Do NOT use the Drive REST API for this (it requires an OAuth/Drive consent
  screen, which breaks the zero-interaction goal).
