#!/usr/bin/env python3
"""Misbehaving-gateway test double (phase 6a step 7, spec §6a.9 responsiveness gate).

Accepts every TCP connection, logs it with a CLOCK_REALTIME nanosecond timestamp,
then NEVER responds — the transport-level stand-in for a gateway that accepts a
contract and never provides the preimage. Every driver that talks to it hangs on
network IO exactly like an hours-long hold invoice (ADR-0024's forcing condition).

MEASUREMENT DISCIPLINE: the timestamp is taken in the accept loop the moment
accept() returns, and the accept loop does NOTHING else (no recv, no thread-start
bookkeeping beyond the spawn) — an earlier version timestamped after recv() inside
handler threads and its accept-loop stalls produced burst artifacts that
misattributed 31 concurrent arrivals to one 1.6 ms window. The kernel completes
handshakes from the backlog without userspace accept, so accept-time is already an
UPPER bound on when the client's connect succeeded.

Usage: hang_gateway.py <port> <log-path>
"""
import socket
import sys
import threading
import time


def park(conn: socket.socket) -> None:
    try:
        # Read (and discard) whatever arrives so the client's write completes, then
        # hold the socket open forever without responding.
        conn.recv(4096)
        while True:
            time.sleep(3600)
    except Exception:
        pass


def main() -> None:
    port, log_path = int(sys.argv[1]), sys.argv[2]
    log = open(log_path, "a", buffering=1)
    srv = socket.create_server(("127.0.0.1", port), backlog=128)
    print(f"hang-gateway listening on 127.0.0.1:{port}", flush=True)
    n = 0
    while True:
        conn, _ = srv.accept()
        ts = time.time_ns()
        n += 1
        log.write(f"{ts} conn-{n}\n")
        threading.Thread(target=park, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
