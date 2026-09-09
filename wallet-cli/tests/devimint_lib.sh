#!/usr/bin/env bash
# Sourceable helpers for the devimint smokes (`source "$(dirname "${BASH_SOURCE[0]}")/devimint_lib.sh"`).
#
# register_lnv2_gateway <gateway_url> <invite_code>
#   Add <gateway_url> to the lnv2 VETTED list of EVERY guardian of the federation <invite_code>
#   names. devimint never registers its LDK gateway into that list (runbook §4), and automated
#   routing resolves from that list and nothing else (ADR-0030), so every smoke registers the
#   gateway right after bring-up and then runs unpinned. Registration is a per-guardian
#   authenticated write with NO liveness check of the URL (the responsiveness gate registers a
#   never-responding double on purpose).
#
#   The exact invocation, verified against the pinned checkout
#   (~/p/fedimint-72b1e5beadc5a31a33ebc751764cb2f840a63b5e):
#
#     fedimint-cli --data-dir <joined client> --our-id <peer> --password pass \
#       module lnv2 gateways add <gateway_url>
#
#   - `gateways add` needs `admin_auth` (modules/fedimint-lnv2-client/src/cli.rs:117-123) and
#     sends ONE admin request to the client's admin peer (modules/fedimint-lnv2-client/src/api.rs:130-136
#     -> `request_admin` -> `self_peer()`, fedimint-api-client/src/api/mod.rs:372-386) — hence the
#     loop over every peer id.
#   - `--our-id` / `--password` are global fedimint-cli options (fedimint-cli/src/cli.rs:41-46;
#     env FM_OUR_ID / FM_PASSWORD_API). `client_open` turns them into admin creds
#     (fedimint-cli/src/lib.rs:898-902) -> `FederationApi::new(.., Some(peer_id), Some(auth))`
#     (fedimint-client/src/client/builder.rs:625-630) -> the lnv2 module's `admin_auth`
#     (builder.rs:810).
#   - The guardian password is devimint's FM_PASSWORD_API = "pass" (devimint/src/vars.rs:294) and
#     the guardian count is FM_FED_SIZE (devimint/src/vars.rs:143); every Global var is exported
#     into the `--exec` environment (devimint/src/cli.rs:177-181).
#   - devimint's default `fedimint-cli` client (FM_CLIENT_DIR) is joined to fed A only, so the
#     helper joins a throwaway client to <invite_code> (`join-federation`, fedimint-cli/src/cli.rs:94)
#     under FM_TEST_DIR, which devimint tears down with the fixture. The same client prints the
#     resulting `gateways list` (threshold union across peers) as evidence.
register_lnv2_gateway() {
  local url="$1" invite="$2" dir peer out
  local peers="${FM_FED_SIZE:?FM_FED_SIZE not set — run inside \`devimint dev-fed --exec\`}"
  dir=$(mktemp -d -p "${FM_TEST_DIR:-${TMPDIR:-/tmp}}" lnv2-admin.XXXXXX) || return 1
  if ! out=$(fedimint-cli --data-dir "$dir" join-federation "$invite" 2>&1); then
    echo "FAIL: fedimint-cli join-federation for lnv2 registration: $out" >&2
    return 1
  fi
  for ((peer = 0; peer < peers; peer++)); do
    if ! out=$(fedimint-cli --data-dir "$dir" --our-id "$peer" --password "${FM_PASSWORD_API:-pass}" \
                 module lnv2 gateways add "$url" 2>&1); then
      echo "FAIL: lnv2 gateways add $url on guardian $peer: $out" >&2
      return 1
    fi
  done
  out=$(fedimint-cli --data-dir "$dir" module lnv2 gateways list 2>/dev/null) || {
    echo "FAIL: lnv2 gateways list after registering $url" >&2; return 1; }
  grep -Fq -- "$url" <<<"$out" || {
    echo "FAIL: $url missing from the vetted list after registration: $out" >&2; return 1; }
  echo "registered lnv2 gateway $url on $peers guardian(s); vetted list: $out"
}
