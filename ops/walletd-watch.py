#!/usr/bin/env python3
"""Poll a running walletd and alert on the failures its own endpoints cannot page you about.

Every incident this wallet has had was fail-CLOSED and correct to refuse, and invisible anyway:

  * a funding shortfall parked below the route floor for 27 days (`decisions: []`, no log line);
  * three undecodable ledger rows that disabled automated probing for weeks (a `warn!` per read);
  * a partial federation view that skips the whole automated cycle while `scheduler_alive` stays
    `true`, because the scheduler loop is genuinely healthy.

None of those change a balance, none crash the process, and none fail a liveness probe. This
script exists because liveness is not readiness, and nothing else pages.

Usage:

    export WALLETD_URL=http://127.0.0.1:9736
    export WALLETD_TOKEN="$(cat /secrets/token)"     # or WALLETD_TOKEN_FILE=/secrets/token
    ops/walletd-watch.py                             # one pass; exit 0 quiet, 1 alert, 2 unreachable

Exit code is the alerting contract: run it from cron and let cron mail non-zero output, or pass
--webhook to POST the same text somewhere. `--state` remembers the last observation so a standing
problem pages on transition rather than every pass; delete that file to force a re-page.

Deliberately dependency-free (stdlib only) so it can run anywhere kubectl or curl can.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

# A deferred funding goal below the PROTOCOL floor can be permanent and benign: nothing drains a
# standby, and no exact-net inflow smaller than the floor can close the remainder. Alerting on it
# forever would train the operator to ignore the deferred list — which is the one signal that
# would have caught the original 27-day outage. Route-floor deferrals are never suppressed.
BENIGN_DEFERRAL_FLOOR_SOURCE = "protocol_min_move"


def get(url: str, token: str, path: str, timeout: float):
    req = urllib.request.Request(
        url.rstrip("/") + path, headers={"Authorization": f"Bearer {token}"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def check(health: dict, status: dict, alerts: list[str], notes: list[str]) -> None:
    if not health.get("scheduler_alive", False):
        alerts.append("scheduler_alive=false — the scheduler task is not running")

    if "automation_ready" not in health:
        # The deployed build may predate the field. Absence is UNKNOWN, never healthy: this is
        # exactly the state in which a silent fence is undetectable.
        notes.append(
            "automation_ready absent — daemon predates the readiness signal; suppression "
            "is NOT observable on this build. Upgrade to page on it."
        )
    elif not health["automation_ready"]:
        blocked = health.get("automation_blocked") or {}
        alerts.append(
            "automation_ready=false — the scheduler is alive but refusing to plan "
            f"[{blocked.get('reason', 'unknown')}] {blocked.get('detail', '')}".rstrip()
        )

    deferred = status.get("deferred")
    if deferred is None:
        notes.append("status.deferred absent — daemon predates deferred-goal reporting")
    else:
        for goal in deferred:
            line = (
                f"funding goal deferred: dest={goal.get('dest', '?')[:16]} "
                f"want={goal.get('want_msat')} floor={goal.get('floor_msat')} "
                f"({goal.get('floor_source')})"
            )
            if goal.get("floor_source") == BENIGN_DEFERRAL_FLOOR_SOURCE:
                notes.append(line + " — below the protocol floor; may be permanent")
            else:
                alerts.append(line)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", default=os.environ.get("WALLETD_URL", "http://127.0.0.1:9736"))
    ap.add_argument("--state", default=os.environ.get("WALLETD_WATCH_STATE"))
    ap.add_argument("--webhook", default=os.environ.get("WALLETD_WATCH_WEBHOOK"))
    ap.add_argument("--timeout", type=float, default=10.0)
    ap.add_argument("--always-report", action="store_true", help="print even when unchanged")
    args = ap.parse_args()

    token = os.environ.get("WALLETD_TOKEN")
    if not token and os.environ.get("WALLETD_TOKEN_FILE"):
        token = open(os.environ["WALLETD_TOKEN_FILE"]).read().strip()
    if not token:
        print("walletd-watch: set WALLETD_TOKEN or WALLETD_TOKEN_FILE", file=sys.stderr)
        return 2

    try:
        health = get(args.url, token, "/v1/health", args.timeout)
        balance = get(args.url, token, "/v1/balance", args.timeout)
        # `status` runs live route probes, so it is the slowest call and the one most likely to
        # time out on a degraded network. A failure here is itself worth reporting, not fatal.
        try:
            status = get(args.url, token, "/v1/status", args.timeout * 3)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            status = {}
            health.setdefault("_status_error", str(e))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        print(f"ALERT walletd unreachable at {args.url}: {e}")
        return 2

    alerts: list[str] = []
    notes: list[str] = []
    if "_status_error" in health:
        alerts.append(f"/v1/status failed: {health['_status_error']}")
    check(health, status, alerts, notes)

    total = balance.get("total")
    observation = {
        "total": total,
        "alerts": sorted(alerts),
        "scheduler_alive": health.get("scheduler_alive"),
        "automation_ready": health.get("automation_ready"),
    }

    previous = None
    if args.state and os.path.exists(args.state):
        try:
            previous = json.load(open(args.state))
        except (OSError, json.JSONDecodeError):
            previous = None
    if args.state:
        try:
            with open(args.state, "w") as fh:
                json.dump(observation, fh)
        except OSError as e:
            notes.append(f"could not persist state to {args.state}: {e}")

    changed = previous is None or previous != observation
    if previous and previous.get("total") != total:
        notes.append(f"balance changed: {previous.get('total')} -> {total} msat")

    if not alerts and not changed and not args.always_report:
        return 0

    lines = [f"walletd {args.url}  total={total} msat"]
    lines += [f"  ALERT {a}" for a in alerts]
    lines += [f"  note  {n}" for n in notes]
    text = "\n".join(lines)
    print(text)

    if args.webhook and (alerts or changed):
        try:
            urllib.request.urlopen(
                urllib.request.Request(
                    args.webhook,
                    data=json.dumps({"text": text}).encode(),
                    headers={"Content-Type": "application/json"},
                ),
                timeout=args.timeout,
            )
        except (urllib.error.URLError, TimeoutError) as e:
            print(f"  note  webhook delivery failed: {e}")

    return 1 if alerts else 0


if __name__ == "__main__":
    sys.exit(main())
