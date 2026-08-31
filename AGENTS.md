# AGENTS.md

Instructions for coding agents working in this repository.

This is a **multi-federation Bitcoin wallet running in production and holding real
sats**. Read [README.md](./README.md) for what is built, and
[docs/roadmap-to-v1.md](./docs/roadmap-to-v1.md) for what is next. The ADRs under
[docs/adr/](./docs/adr/) are canonical wherever they conflict with older text.

<!-- agent-discipline-v1 -->
## Working agreement

These rules are here because agents get them wrong by default, not because they
are good general advice.

**Edits must be verified, not assumed.** Prefer your harness's edit tool: it
fails loudly when the target text does not match. `sed -i` and `str.replace()`
do the opposite — a pattern that matches nothing changes nothing, exits 0, and
prints whatever success message you wrote. Batching several edits into one
scripted call is the usual reason this happens, and the saved tool calls are not
worth it. If you do script an edit, assert the target exists before replacing and
make the success message conditional on that assert, then grep the file
afterwards for both the new text and the absence of the old. Prose and markdown
are where this bites hardest: nothing compiles a README, so a silently skipped
edit survives and gets reported as done.

The check does not stop at the file. `nothing to commit, working tree clean` reads
exactly the same whether the work was already committed or was reverted underneath
you by another process holding the tree — so never take that message as proof a
commit happened. Take the exit code instead (`git commit` exits **1** on an empty
commit), and for anything you care about also look inside the commit, since a
partial loss commits cleanly at exit 0:

```
git add -- <every path this change touched>   # not `git add -A`: on a dirty tree it
git diff --cached                            # sweeps in unrelated work — and READ this,
                                             # since staging a path that was already
                                             # dirty takes the other agent's hunks too
git commit -m "<msg>" || { echo "commit produced nothing"; exit 1; }
git show --stat --format= HEAD               # all the paths you meant, and only those
```

Then confirm the *content* landed, per file, by the check that fits it — a file that
gained content must contain a distinctive new phrase; a file you removed lines from must
still exist **and** hold the expected remaining count of the deleted phrase; a deleted path
must be absent. A file that BOTH gained and lost content needs both of the first two — the
added phrase passing says nothing about whether the removal survived. One does not
substitute for another, and each has a way to
lie: `grep` defaults to regex (use `-Fq --`), `git show ... | grep` returns 141 under
`pipefail` when grep exits early (capture to a file first), `grep -c` counts lines rather
than occurrences, and demanding *zero* occurrences rejects a correct partial removal.

```bash
_chk=$(mktemp) || { echo "cannot create the scratch file — do NOT report the commit verified"; exit 1; }
trap 'rm -f "$_chk"' EXIT
# ...the three loops, using `grep -Fq --` / `grep -Fo | wc -l || true` on a captured file.
```

A clean `git status` is neither check.

The same tools also corrupt without failing. In a `sed` replacement string `&`
means "the whole match", so substituting a value containing `&&` — any shell
command that chains, which is most of them — silently doubles it and reports
success. Substitute with something that treats the replacement as a literal, and
grep for the result afterwards.

**Never pipe a gate through `tail`, `head`, or `grep`.** A pipeline's exit status
is the last command's, and `tail` always succeeds, so a failing build reports
exit 0. Redirect and capture the real code:

```
<gate> > /tmp/gate.log 2>&1; echo "EXIT=$?"
```

Then read the log. Note the `;` — not `|`.

**"Passing", "clean", "working", "verified", and "done" require a command and an
exit code.** If you cannot show one, say what you actually observed instead. This
is the single most common way an agent reports success it did not have.

**A claim about what a tool does needs a run, not a recollection.** Prose asserting
observable behaviour — an exit code, which stream output went to, which versions were
tested — is as capable of being wrong as code, and nothing compiles it. Record enough that
a reader can re-run it and disagree: the command, the versions, the mode where the mode
changes the answer, and the observed result with the streams *separated by redirection*
rather than labelled by hand. Two things that are not records: a bare "Measured on
git 2.54.0", which names no command or result, and "fails silently", which names no
version or mode. And observing is not explaining — a rerun gives you the exit code and the
streams, never the *why*; "X fails because Y" needs Y varied on its own with the outcome
changing, or the documentation cited. Absent that, write what you saw and leave the cause
out. A claim you cannot run — a version you do not have — is not yours to assert: say it
is unmeasured and narrow it to a possibility.

**Reviewers read code; they do not run it.** A clean review — human, bot, or
model — is not a passing build. Run the gate yourself before calling anything
done.

**A test that has never failed has proven nothing.** When you add one for a bug,
watch it go red against the unfixed code first. A test asserting behaviour that
was already correct is indistinguishable from a test asserting nothing.

Three things decide whether that red run means anything. Break the **production
behaviour**, never the test's expected value or its setup — those redden any
assertion, including one that never reaches the behaviour. **Read the failure**: it
must name the assertion pinning what you broke, not an unrelated panic. And run **one
mutation per property** the test claims, since reddening the first of two leaves the
second untested while looking verified. If the test drives anything live — a real
database, a running service, real money — do the red run in a disposable environment
or not at all: a deliberately broken build can perform the harmful operation before
any assertion notices.

Gate for this repo: `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
<!-- end-agent-discipline -->

## Project-specific notes

**The gate needs a nix devshell.** A bare `cargo` invocation fails on missing native
dependencies. This repo's own flake is sufficient — an earlier note claiming the sibling
`fedimint` checkout's flake was required is wrong, though
`nix develop /home/master/p/fedimint -c ...` also works if you have that checkout.

**Money-adjacent code gets more than the gate.** Changes touching the allocator, the
executor, the move protocol, the journal, or the operation ledger move real funds. They
take a careful implementation, an adversarial review panel rather than a single reviewer,
and — where the change alters a money path — a live devimint gate. Unit tests passing is
necessary and not sufficient; see [docs/devimint-runbook.md](./docs/devimint-runbook.md).

**This repo is greenfield and does not carry compatibility shims** — with one deliberate
exception. `Policy`, `Action`, the move records, and **ledger rows** (`OperationRecord` and
every `OperationKind` variant) ride **live production stores** written by the running daemon,
so new fields on those types use `#[serde(default)]` (with a *named* default function for
numeric fields, since a bare default yields zero) so an existing row still decodes. A move
record cannot be re-created by re-running a command. Do not remove these as cleanup; every
other kind of back-compat shim is still unwelcome.

Forward compatibility is only half of it. A persisted type must ALSO stay readable by the
PREVIOUS build, or a bad deploy cannot be rolled back — so **never put
`#[serde(deny_unknown_fields)]` on a type that is written to a live store**. It is correct on a
request DTO (a typo'd field would otherwise silently take the shipped default) and a downgrade
fence on a row. `Policy` was both at once and carried the attribute, which meant one `PUT
/v1/policy` after an upgrade would stop the older binary from reading its own policy row at
startup (br-c3j). The fix pattern: keep the stored type permissive and enforce strictness in the
handler against a key set derived from the type itself, so the wire contract cannot drift. Every
other `deny_unknown_fields` in `wallet-api` is a request-only `*Request` DTO; keep it that way.

The `serde(default)` rule applies to a field added to an ALREADY-SHIPPED variant, not just to a
new type — that distinction is what br-yjg cost. `RefusalDiagnostics`' own later fields carried the attribute
correctly while the `diagnostics` key that introduced them did not, which permanently killed
three rows on the funded wallet. Ledger rows are append-only audit evidence the runbook forbids
deleting, so an undecodable one can never be repaired in place, and `probe_budget_ledger_rows`
fails closed on any skipped row — the blast radius is a disabled subsystem, not a display gap.
When you add such a field, pin it with a test that strips the key from a persisted row and
re-reads it.

**Work is tracked in beads** (`.beads/issues.jsonl` is the tracked truth; `.beads/beads.db`
is a gitignored cache). Never hand-edit the JSONL — the cache will silently revert it on the
next `br` write. Use `br` commands, and after any closure run an explicit
`br sync --flush-only` and check its exit code: the automatic flush after `br close`
swallows its own error. `br --help` covers the CLI; the generic block `br agents --add`
generates is deliberately NOT kept here, because its session protocol ends in
`git commit && git push`, which in this repo would put unreviewed money-path code on a
branch without the panel or the devimint gate above.

**Nothing lands on the default branch without review** — including bookkeeping. Beads
closures and doc updates ride a branch and a PR like everything else.

**The generated beads block below is deliberately EMPTY, and the markers are load-bearing.**
`br agents --add` re-injects the generic block only when it finds no markers, so deleting them
brings back a session protocol ending in `git commit && git push` — which here means unreviewed
money-path code on a branch with no panel and no devimint gate. Keeping empty markers gives
`br agents --update` something to update in place instead. This explanation lives OUTSIDE the
markers on purpose: anything between them is `br`'s to overwrite, so a note kept inside would be
destroyed by the regeneration it exists to warn about. If you do regenerate, read what it writes
against the working agreement above before committing it.

<!-- br-agent-instructions-v1 -->
<!-- end-br-agent-instructions -->
