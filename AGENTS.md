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

**Reviewers read code; they do not run it.** A clean review — human, bot, or
model — is not a passing build. Run the gate yourself before calling anything
done.

**A test that has never failed has proven nothing.** When you add one for a bug,
watch it go red against the unfixed code first. A test asserting behaviour that
was already correct is indistinguishable from a test asserting nothing.

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
exception. `Policy`, `Action`, and the move records ride **live production stores** written
by the running daemon, so new fields on those types use `#[serde(default)]` (with a *named*
default function for numeric fields, since a bare default yields zero) so an existing row
still decodes. A move record cannot be re-created by re-running a command. Do not remove
these as cleanup; every other kind of back-compat shim is still unwelcome.

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

<!-- br-agent-instructions-v1 -->
The generated beads block is deliberately EMPTY. These markers are kept so `br agents`
has an in-place target: delete them and the next `br agents --add` sees no block and
re-injects the generic one, whose session protocol ends in `git commit && git push` —
which in this repo means unreviewed money-path code on a branch with no panel and no
devimint gate. The repo-specific beads guidance is the paragraph directly above; the CLI
reference it would add is available from `br --help`. If you regenerate this block, read
what it writes against the working agreement above before committing it.
<!-- end-br-agent-instructions -->

