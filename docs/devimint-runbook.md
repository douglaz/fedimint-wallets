# devimint runbook — build, run, and drive the money path

Operational notes from standing up fedimint + devimint and validating the cross-fed move
(2026-06-29). This is the HOW-TO that backs the model in
[fedimint-mechanics.md](./fedimint-mechanics.md) and the original Phase 1 harness
(formerly tracked as TODO T4; the live backlog is now in `br`).
The wallet client's Fedimint revision is derived from all three manifests that carry the fork
pin below. Build and run devimint from that exact revision: another checkout can be
protocol-incompatible with the wallet client.

## 1. Build from the exact pin
This runbook requires Bash >= 4.2 (for `[[ -v ]]`), Git >= 2.36 (the
`git worktree list --porcelain -z` support introduced by git/git d97eb302),
and Nix >= 2.18 with `nix-command` and `flakes` enabled.
It was tested with Bash 5.3, Git 2.55, and Nix 2.34. Nix 2.18 documents the
stable `-i` and `-k` forms used below; the long option spellings differ by
version.

Run §1 and §2 in the same dedicated, non-production shell: §2 deliberately
reuses the exported pin/worktree variables and shell functions defined here.
Do not run §1 as a child script. These fail-closed blocks set `-euo pipefail`,
and an explicit refusal can close that shell; after any refusal, open a fresh
shell and replay §1 from the top before attempting §2.
The two source repositories must already exist at `WALLETS_REPO` and
`FEDIMINT_REPO`. For a new machine, create the latter with
`git clone --no-checkout https://github.com/douglaz/fedimint.git ~/p/fedimint`;
the commands below still fetch and select the exact manifest-derived commit.

```bash
set -euo pipefail
export WALLETS_REPO=~/p/fedimint-wallets
export FEDIMINT_REPO=~/p/fedimint

# Refuse repository-routing variables before the first Git command. Pager, prompt-only, and
# trace settings are harmless; every other GIT_* variable is treated as a possible redirect.
for variable in $(compgen -v GIT_ || true); do
  case "$variable" in
    GIT_PAGER|GIT_PS1_*|GIT_TERMINAL_PROMPT|GIT_TRACE|GIT_TRACE2|GIT_TRACE2_EVENT|\
    GIT_TRACE2_PERF) continue ;;
  esac
  echo "refusing ambient Git repository override: $variable" >&2
  exit 1
done

git -C "$WALLETS_REPO" rev-parse --git-dir >/dev/null 2>&1 || {
  echo "refusing: WALLETS_REPO is not a Git checkout: $WALLETS_REPO" >&2
  exit 1
}
git -C "$FEDIMINT_REPO" rev-parse --git-dir >/dev/null 2>&1 || {
  echo "refusing: FEDIMINT_REPO is not a Git checkout; clone https://github.com/douglaz/fedimint.git to $FEDIMINT_REPO" >&2
  exit 1
}

refuse_cargo_config_for_dir() {
  (
    set -euo pipefail
    local directory="$1"
    if [[ -z "${HOME:-}" || "${HOME:-}" != /* ]]; then
      echo "refusing unset, empty, or relative HOME before resolving Cargo configuration" >&2
      exit 1
    fi
    if [[ -v CARGO_HOME ]] && [[ -z "$CARGO_HOME" || "$CARGO_HOME" != /* ]]; then
      echo "refusing empty or relative CARGO_HOME because Cargo resolves it from each build directory: $CARGO_HOME" >&2
      exit 1
    fi
    local cargo_home="${CARGO_HOME:-$HOME/.cargo}"
    local candidate parent
    directory="$(realpath -e "$directory")"
    for candidate in "$cargo_home/config" "$cargo_home/config.toml"; do
      if [[ -e "$candidate" || -L "$candidate" ]]; then
        echo "refusing Cargo configuration that can override sources: $candidate" >&2
        exit 1
      fi
    done
    while :; do
      for candidate in "$directory/.cargo/config" "$directory/.cargo/config.toml"; do
        if [[ -e "$candidate" || -L "$candidate" ]]; then
          echo "refusing Cargo configuration that can override sources: $candidate" >&2
          exit 1
        fi
      done
      parent="$(dirname "$directory")"
      [[ "$parent" != "$directory" ]] || break
      directory="$parent"
    done
  )
}

refuse_git_replacements() {
  local repository="$1"
  local replacements
  replacements="$(GIT_NO_REPLACE_OBJECTS=1 git -C "$repository" \
    for-each-ref --format='%(refname)' refs/replace)" || {
      echo "refusing: cannot inspect Git replacement refs in $repository" >&2
      return 1
    }
  if [[ -n "$replacements" ]]; then
    echo "refusing Git replacement refs in exact-source repository: $repository" >&2
    return 1
  fi
}

refuse_hidden_index_flags() {
  local worktree="$1"
  local flags
  flags="$(GIT_NO_REPLACE_OBJECTS=1 git -C "$worktree" ls-files -v)" || {
    echo "refusing: cannot inspect exact-source worktree index flags: $worktree" >&2
    return 1
  }
  if grep -Eq '^[a-zS] ' <<<"$flags"; then
    echo "refusing assume-unchanged or skip-worktree index flags: $worktree" >&2
    return 1
  fi
}

refuse_ambient_rust_build_overrides() {
  (
    set -euo pipefail
    local variable
    for variable in $(compgen -v GIT_ || true); do
      case "$variable" in
        GIT_PAGER|GIT_PS1_*|GIT_TERMINAL_PROMPT|GIT_TRACE|GIT_TRACE2|GIT_TRACE2_EVENT|\
        GIT_TRACE2_PERF) continue ;;
      esac
      echo "refusing ambient Git repository override: $variable" >&2
      exit 1
    done
    for variable in $(compgen -v CARGO_ || true); do
      case "$variable" in
        CARGO_HOME|CARGO_TARGET_DIR|CARGO_BUILD_TARGET_DIR) continue ;;
      esac
      echo "refusing ambient Cargo build override: $variable" >&2
      exit 1
    done
    for variable in RUSTC RUSTC_BOOTSTRAP RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER \
                    RUSTDOC RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTDOCFLAGS \
                    RUSTUP_TOOLCHAIN; do
      if [[ -v "$variable" ]]; then
        echo "refusing ambient Rust build override: $variable" >&2
        exit 1
      fi
    done
  )
}

run_exact_nix_develop() (
  set -euo pipefail
  if [[ -v HOME ]]; then export HOME; fi
  if [[ -v USER ]]; then export USER; fi
  if [[ -v TERM ]]; then export TERM; fi
  if [[ -v CARGO_HOME ]]; then export CARGO_HOME; fi
  if [[ -v WALLETS_REPO ]]; then export WALLETS_REPO; fi
  if [[ -v FEDIMINT_REPO ]]; then export FEDIMINT_REPO; fi
  if [[ -v FEDIMINT_WORKTREE ]]; then export FEDIMINT_WORKTREE; fi
  if [[ -v SMOKE_SCRIPT ]]; then export SMOKE_SCRIPT; fi
  command nix develop -i \
    -k HOME \
    -k USER \
    -k TERM \
    -k CARGO_HOME \
    -k WALLETS_REPO \
    -k FEDIMINT_REPO \
    -k FEDIMINT_WORKTREE \
    -k SMOKE_SCRIPT \
    "$@"
)

run_exact_cargo() (
  set -euo pipefail
  local exact_cargo_home
  exact_cargo_home="$(mktemp -d)"
  trap 'rm -rf "$exact_cargo_home"' EXIT
  CARGO_HOME="$exact_cargo_home" run_exact_nix_develop -c cargo "$@"
)

reset_exact_target_dir() (
  set -euo pipefail
  if [[ "$#" -ne 1 ]]; then
    echo "usage: reset_exact_target_dir /approved/repository/target-nix" >&2
    exit 1
  fi
  if [[ -z "${WALLETS_REPO:-}" ]]; then
    echo "refusing target reset: WALLETS_REPO is unset or empty" >&2
    exit 1
  fi
  local target_dir="$1"
  local wallets_target="$WALLETS_REPO/target-nix"
  local fedimint_target=""
  local approved=0
  if [[ -v FEDIMINT_WORKTREE ]]; then
    if [[ -z "$FEDIMINT_WORKTREE" ]]; then
      echo "refusing target reset: FEDIMINT_WORKTREE is set but empty" >&2
      exit 1
    fi
    fedimint_target="$FEDIMINT_WORKTREE/target-nix"
  fi
  if [[ "$target_dir" == "$wallets_target" ]] ||
     [[ -n "$fedimint_target" && "$target_dir" == "$fedimint_target" ]]; then
    approved=1
  fi
  if [[ "$approved" -ne 1 ]]; then
    echo "refusing unapproved target reset: $target_dir" >&2
    exit 1
  fi
  if [[ -L "$target_dir" ]]; then
    echo "refusing target reset through symlink: $target_dir" >&2
    exit 1
  fi
  if [[ -e "$target_dir" && ! -d "$target_dir" ]]; then
    echo "refusing target reset of non-directory: $target_dir" >&2
    exit 1
  fi
  rm -rf -- "$target_dir"
)

# Every Fedimint package dependency in every workspace member must use the fork with exactly
# one 40-hex rev. The three current pin-carrying members must continue to carry a fork dependency,
# and every member is scanned so a newly added or repinned Fedimint dependency cannot escape.
# The root manifest may carry no fork dependency, but any it does carry must agree; root patches
# and repository-local Cargo source replacement are refused because they can override the pin.
# Parse TOML structurally so both inline dependencies and `[dependencies.<name>]` tables work.
derive_fedimint_rev() {
  # The wallets flake does not currently source this file. Refuse it as a precaution so this
  # reproducible recipe never relies on repository-local shell customization.
  if [[ -e "$WALLETS_REPO/.shrc.local" || -L "$WALLETS_REPO/.shrc.local" ]]; then
    echo "refusing wallets repository-local .shrc.local as a reproducibility precaution: $WALLETS_REPO/.shrc.local" >&2
    return 1
  fi
  refuse_cargo_config_for_dir "$WALLETS_REPO" || return 1
  refuse_ambient_rust_build_overrides || return 1
  (
    cd "$WALLETS_REPO"
    run_exact_nix_develop -c python3 -I - \
    "$WALLETS_REPO/Cargo.toml" <<'PY'
from pathlib import Path
import re
import sys
import tomllib
from urllib.parse import parse_qs, urlsplit

rev = re.compile(r"[0-9a-fA-F]{40}")

def is_fork_url(value):
    if not isinstance(value, str):
        return False
    # Only accept the canonical HTTPS and SSH spellings. In particular, do not
    # turn a local path or another Git transport into a GitHub URL by extracting
    # just a host and path from it.
    candidate = value
    if candidate != candidate.strip():
        return False
    repo_path = re.compile(r"douglaz/fedimint(?:\.git)?", re.IGNORECASE)
    if "://" not in candidate:
        scp = re.fullmatch(r"git@([^/:@]+):([^?#]+)", candidate)
        return bool(
            scp
            and scp.group(1).casefold() == "github.com"
            and repo_path.fullmatch(scp.group(2))
        )

    try:
        parsed = urlsplit(candidate)
        port = parsed.port
    except ValueError:
        return False
    if parsed.query or parsed.fragment or port is not None:
        return False
    if parsed.scheme.casefold() == "https":
        return bool(
            parsed.netloc.casefold() == "github.com"
            and parsed.path
            and repo_path.fullmatch(parsed.path.removeprefix("/"))
            and parsed.path.startswith("/")
        )
    if parsed.scheme.casefold() == "ssh":
        return bool(
            parsed.netloc == "git@github.com"
            and parsed.path
            and repo_path.fullmatch(parsed.path.removeprefix("/"))
            and parsed.path.startswith("/")
        )
    return False

def dependency_pins(specification, dependency, manifest, path):
    package = dependency
    if isinstance(specification, dict):
        package = specification.get("package", dependency)
    normalized_package = (
        package.replace("_", "-").casefold() if isinstance(package, str) else ""
    )
    is_fedimint_package = (
        normalized_package == "fedimint"
        or normalized_package.startswith("fedimint-")
    )
    git = specification.get("git") if isinstance(specification, dict) else None
    if is_fedimint_package and not is_fork_url(git):
        raise SystemExit(
            f"refusing non-pinned Fedimint dependency: "
            f"{manifest}:{'.'.join(path)} must use github.com/douglaz/fedimint"
        )
    if not is_fork_url(git):
        return []
    candidate = specification.get("rev")
    if not isinstance(candidate, str) or not rev.fullmatch(candidate):
        raise SystemExit(
            f"refusing ambiguous Fedimint pin: {manifest}:{'.'.join(path)}: "
            "fork dependency needs exactly one 40-hex rev"
        )
    return [candidate.lower()]

def manifest_dependency_pins(document, manifest):
    pins = []
    dependency_tables = (
        "dependencies",
        "dev-dependencies",
        "build-dependencies",
        "dev_dependencies",
        "build_dependencies",
    )

    def scan_tables(container, prefix):
        if not isinstance(container, dict):
            return
        for table_name in dependency_tables:
            table = container.get(table_name, {})
            if not isinstance(table, dict):
                raise SystemExit(
                    f"refusing malformed Cargo dependency table: "
                    f"{manifest}:{'.'.join((*prefix, table_name))}"
                )
            for dependency, specification in table.items():
                if isinstance(specification, dict) and specification.get("workspace") is True:
                    inherited = workspace_dependencies.get(dependency)
                    if inherited is None:
                        raise SystemExit(
                            f"refusing unresolved workspace dependency: "
                            f"{manifest}:{'.'.join((*prefix, table_name, str(dependency)))}"
                        )
                    specification = inherited
                pins.extend(
                    dependency_pins(
                        specification,
                        str(dependency),
                        manifest,
                        (*prefix, table_name, str(dependency)),
                    )
                )

    scan_tables(document, ())
    workspace = document.get("workspace", {})
    if isinstance(workspace, dict):
        dependencies = workspace.get("dependencies", {})
        if not isinstance(dependencies, dict):
            raise SystemExit(
                f"refusing malformed Cargo dependency table: "
                f"{manifest}:workspace.dependencies"
            )
        for dependency, specification in dependencies.items():
            pins.extend(
                dependency_pins(
                    specification,
                    str(dependency),
                    manifest,
                    ("workspace", "dependencies", str(dependency)),
                )
            )
    target = document.get("target", {})
    if not isinstance(target, dict):
        raise SystemExit(f"refusing malformed Cargo target table: {manifest}:target")
    for selector, target_tables in target.items():
        scan_tables(target_tables, ("target", str(selector)))
    return pins

root_manifest = Path(sys.argv[1]).resolve()
with root_manifest.open("rb") as source:
    root = tomllib.load(source)
if root.get("patch") or root.get("replace"):
    raise SystemExit(
        f"refusing Cargo source override: {root_manifest} contains "
        "a [patch] or [replace] table"
    )

workspace = root.get("workspace")
if not isinstance(workspace, dict):
    raise SystemExit(f"refusing root manifest without [workspace]: {root_manifest}")
workspace_dependencies = workspace.get("dependencies", {})
if not isinstance(workspace_dependencies, dict):
    raise SystemExit(f"refusing malformed workspace.dependencies: {root_manifest}")

pins_by_manifest = {}
root_pins = manifest_dependency_pins(root, str(root_manifest))
if root_pins:
    pins_by_manifest[str(root_manifest)] = root_pins

members = workspace.get("members")
if not isinstance(members, list) or not all(isinstance(item, str) for item in members):
    raise SystemExit(f"refusing malformed workspace.members: {root_manifest}")
root_dir = root_manifest.parent
member_manifests = []
for pattern in members:
    matches = sorted(root_dir.glob(pattern))
    if not matches:
        raise SystemExit(f"refusing unmatched workspace member pattern: {pattern}")
    for member in matches:
        manifest = member if member.name == "Cargo.toml" else member / "Cargo.toml"
        if not manifest.is_file():
            raise SystemExit(f"refusing workspace member without Cargo.toml: {member}")
        resolved = manifest.resolve()
        if resolved not in member_manifests:
            member_manifests.append(resolved)

required_pin_carriers = {
    (root_dir / name / "Cargo.toml").resolve()
    for name in ("wallet-fedimint", "wallet-cli", "wallet-daemon")
}
if not required_pin_carriers.issubset(set(member_manifests)):
    raise SystemExit("refusing workspace missing a required Fedimint pin-carrying member")

for manifest in member_manifests:
    with manifest.open("rb") as source:
        pins = manifest_dependency_pins(tomllib.load(source), str(manifest))
    if manifest in required_pin_carriers and not pins:
        raise SystemExit(
            f"refusing absent fork dependency: {manifest} has no "
            "douglaz/fedimint dependency"
        )
    if pins:
        pins_by_manifest[str(manifest)] = pins

unique_pins = {pin for pins in pins_by_manifest.values() for pin in pins}
if len(unique_pins) != 1:
    detail = "\n".join(
        f"  {manifest}: {', '.join(sorted(set(pins)))}"
        for manifest, pins in pins_by_manifest.items()
    )
    raise SystemExit(
        "refusing Fedimint pin disagreement/ambiguity across manifests:\n" + detail
    )
selected_pin = unique_pins.pop()

# `--locked` below makes Cargo consume this exact resolved graph. Check every Fedimint package
# in it so transitive and non-workspace path dependencies cannot introduce a second source.
lockfile = root_dir / "Cargo.lock"
with lockfile.open("rb") as source:
    lock = tomllib.load(source)
locked_fedimint = 0
allowed_external_fedimint = {
    # Independent cryptography crate published on crates.io; not part of the SDK workspace.
    "fedimint-threshold-crypto": "registry+https://github.com/rust-lang/crates.io-index",
}
for package in lock.get("package", []):
    name = package.get("name", "")
    normalized_name = name.replace("_", "-").casefold() if isinstance(name, str) else ""
    if normalized_name != "fedimint" and not normalized_name.startswith("fedimint-"):
        continue
    source = package.get("source")
    if (
        normalized_name in allowed_external_fedimint
        and allowed_external_fedimint[normalized_name] == source
    ):
        continue
    locked_fedimint += 1
    if not isinstance(source, str) or not source.startswith("git+"):
        raise SystemExit(
            f"refusing non-pinned Fedimint package in Cargo.lock: {name} source={source!r}"
        )
    source_and_query, separator, resolved_commit = source[4:].partition("#")
    parsed = urlsplit(source_and_query)
    requested_revs = parse_qs(parsed.query).get("rev", [])
    source_without_query = parsed._replace(query="", fragment="").geturl()
    if (
        not separator
        or not is_fork_url(source_without_query)
        or [item.casefold() for item in requested_revs] != [selected_pin]
        or resolved_commit.casefold() != selected_pin
    ):
        raise SystemExit(
            f"refusing Fedimint Cargo.lock source disagreement: "
            f"{name} source={source!r}, expected rev {selected_pin}"
        )
if locked_fedimint == 0:
    raise SystemExit(f"refusing Cargo.lock without any Fedimint package: {lockfile}")
print(selected_pin)
PY
  )
}
FEDIMINT_REV="$(derive_fedimint_rev)"
if ! [[ "$FEDIMINT_REV" =~ ^[[:xdigit:]]{40}$ ]]; then
  echo "refusing ambiguous Fedimint pin derivation" >&2
  exit 1
fi
export FEDIMINT_WORKTREE=~/p/fedimint-$FEDIMINT_REV

verify_pinned_worktree() {
  local worktree_list resolved_path record_path="" record_head="" record_detached=0
  local record_resolved matched=0 matched_head="" matched_detached=0 field symlink_path

  symlink_path="${FEDIMINT_WORKTREE%/}"
  symlink_path="${symlink_path:-/}"
  if [[ -L "$symlink_path" ]]; then
    echo "refusing Fedimint worktree symlink (including dangling symlink): $FEDIMINT_WORKTREE" >&2
    return 1
  fi
  if [[ ! -d "$FEDIMINT_WORKTREE" ]]; then
    echo "refusing Fedimint worktree that is not a directory: $FEDIMINT_WORKTREE" >&2
    return 1
  fi
  if ! resolved_path="$(realpath -e -- "$FEDIMINT_WORKTREE")"; then
    echo "refusing unable to resolve Fedimint worktree: $FEDIMINT_WORKTREE" >&2
    return 1
  fi
  if ! worktree_list="$(mktemp)"; then
    echo "refusing unable to create temporary worktree-list file" >&2
    return 1
  fi
  if ! git -C "$FEDIMINT_REPO" worktree list --porcelain -z >"$worktree_list"; then
    rm -f -- "$worktree_list"
    echo "refusing unable to list Fedimint worktrees" >&2
    return 1
  fi

  while IFS= read -r -d '' field || [[ -n "$field" ]]; do
    if [[ -z "$field" ]]; then
      if [[ -n "$record_path" ]] &&
         record_resolved="$(realpath -e -- "$record_path" 2>/dev/null)" &&
         [[ "$record_resolved" == "$resolved_path" ]]; then
        ((matched += 1))
        matched_head="$record_head"
        matched_detached="$record_detached"
      fi
      record_path=""
      record_head=""
      record_detached=0
    elif [[ "$field" == "worktree "* ]]; then
      record_path="${field#worktree }"
    elif [[ "$field" == "HEAD "* ]]; then
      record_head="${field#HEAD }"
    elif [[ "$field" == "detached" ]]; then
      record_detached=1
    fi
  done <"$worktree_list"
  rm -f -- "$worktree_list"

  if [[ "$matched" -ne 1 ]]; then
    echo "refusing Fedimint worktree registration: expected exactly one entry for $resolved_path, found $matched" >&2
    return 1
  fi
  if [[ "$matched_head" != "$FEDIMINT_REV" ]]; then
    echo "refusing Fedimint worktree registration: recorded HEAD $matched_head is not pin $FEDIMINT_REV" >&2
    return 1
  fi
  if [[ "$matched_detached" -ne 1 ]]; then
    echo "refusing Fedimint worktree registration: pinned worktree must be detached" >&2
    return 1
  fi

  FEDIMINT_WORKTREE="$resolved_path"
  export FEDIMINT_WORKTREE
}

# Fetch from the wallet client's fork only if this checkout does not already have the exact SDK
# object. Do not fetch from `origin`: its upstream remote need not contain this fork-only pin.
refuse_git_replacements "$FEDIMINT_REPO"
if ! git -C "$FEDIMINT_REPO" cat-file -e "$FEDIMINT_REV^{commit}"; then
  git -C "$FEDIMINT_REPO" fetch https://github.com/douglaz/fedimint.git "$FEDIMINT_REV"
fi
git -C "$FEDIMINT_REPO" cat-file -e "$FEDIMINT_REV^{commit}"
worktree_symlink_path="${FEDIMINT_WORKTREE%/}"
worktree_symlink_path="${worktree_symlink_path:-/}"
if [[ -L "$worktree_symlink_path" ]]; then
  echo "refusing Fedimint worktree symlink (including dangling symlink): $FEDIMINT_WORKTREE" >&2
  exit 1
elif [[ -e "$FEDIMINT_WORKTREE" ]]; then
  :
else
  git -C "$FEDIMINT_REPO" worktree add --detach "$FEDIMINT_WORKTREE" "$FEDIMINT_REV"
fi
verify_pinned_worktree
refuse_git_replacements "$FEDIMINT_WORKTREE"
refuse_hidden_index_flags "$FEDIMINT_WORKTREE"
cd "$FEDIMINT_WORKTREE"
test "$(git rev-parse HEAD)" = "$FEDIMINT_REV"
# The pinned flake sources this ignored local hook during `nix develop`; do not execute
# unreviewed worktree-local configuration.
if [[ -e "$FEDIMINT_WORKTREE/.shrc.local" || -L "$FEDIMINT_WORKTREE/.shrc.local" ]]; then
  echo "refusing exact-pinned Fedimint worktree with ignored .shrc.local: $FEDIMINT_WORKTREE/.shrc.local" >&2
  exit 1
fi
```
- The nix devshell provides the external daemons: **bitcoind 31.0, lnd 0.19.3, esplora,
  lncli**, and the toolchain (cargo 1.93). esplora is NOT on the system PATH — you MUST be
  in `nix develop`.
- The **pinned Fedimint devshell** derives `REPO_ROOT` from the git top-level of the directory
  that invokes it. Invoked from the pin worktree, its release builds land in
  `$FEDIMINT_WORKTREE/target-nix/release`. Exact live recipes enter Nix through
  `run_exact_nix_develop`, whose fixed child-environment allowlist excludes ambient target,
  native-toolchain, Rust, and shell-hook overrides. Certifying Cargo builds use
  `run_exact_cargo`, which creates a fresh temporary Cargo source home for each invocation,
  so mutable Cargo Git checkouts cannot be reused. They reset the approved `target-nix`
  directory before each build, then pass Cargo's explicit `--target-dir` so debug or release
  wallet binaries land where the smokes execute them. Neither target variable may leak into
  the exact Fedimint build/run recipes.
- The **cachix cache is unavailable** ("not a trusted user"), so deps compile from source
  (cold build is long; a warm rebuild of just the workspace was ~4m17s).
- Built binaries: `devimint, fedimintd, gatewayd, gateway-cli, fedimint-cli` (0.12-alpha).
  Don't mix with the prebuilt `~/bin` binaries (those are 0.11.1).
- Mandatory for every §2 run, including a single-federation smoke: verify/apply
  `docs/devimint-two-fed-harness.patch` to this exact-pinned worktree **before its release
  build.** Section 2 intentionally requires the verifier and the exactly patched worktree;
  `--num-feds 1` simply leaves the added federation-B path inactive. The commands fail closed:
  a clean worktree receives a checked patch application;
  a dirty worktree is accepted only when its sole unstaged change is exactly the patch's
  `devimint/src/cli.rs` result. Staged, untracked, partial, or unrelated changes are refused
  without reset or clean.
  ```bash
  set -euo pipefail
  export TWO_FED_PATCH="$WALLETS_REPO/docs/devimint-two-fed-harness.patch"
  test "$(git -C "$FEDIMINT_WORKTREE" rev-parse HEAD)" = "$FEDIMINT_REV"
  test -f "$TWO_FED_PATCH"
  refuse_git_replacements "$FEDIMINT_WORKTREE"
  refuse_hidden_index_flags "$FEDIMINT_WORKTREE"
  refuse_cargo_config_for_dir "$FEDIMINT_WORKTREE"
  refuse_ambient_rust_build_overrides

  untracked=$(git -C "$FEDIMINT_WORKTREE" ls-files --others --exclude-standard)
  if ! git -C "$FEDIMINT_WORKTREE" diff --cached --quiet || [[ -n "$untracked" ]]; then
    echo "refusing non-clean two-fed worktree: staged or untracked changes found" >&2
    exit 1
  elif git -C "$FEDIMINT_WORKTREE" diff --quiet; then
    git -C "$FEDIMINT_WORKTREE" apply --check "$TWO_FED_PATCH"
    git -C "$FEDIMINT_WORKTREE" apply "$TWO_FED_PATCH"
  elif [[ "$(git -C "$FEDIMINT_WORKTREE" diff --name-only)" != "devimint/src/cli.rs" ]] ||
       [[ -n "$(git -C "$FEDIMINT_WORKTREE" diff --summary)" ]]; then
    echo "refusing non-clean two-fed worktree: unrelated unstaged changes found" >&2
    exit 1
  else
    expected_dir=$(mktemp -d)
    trap 'rm -rf "$expected_dir"' EXIT
    mkdir -p "$expected_dir/devimint/src"
    git -C "$FEDIMINT_WORKTREE" show HEAD:devimint/src/cli.rs > "$expected_dir/devimint/src/cli.rs"
    git -C "$expected_dir" apply --check "$TWO_FED_PATCH"
    git -C "$expected_dir" apply "$TWO_FED_PATCH"
    cmp "$expected_dir/devimint/src/cli.rs" "$FEDIMINT_WORKTREE/devimint/src/cli.rs"
    rm -rf "$expected_dir"
    trap - EXIT
    echo "two-fed harness patch already applied exactly"
  fi
  verify_exact_two_fed_worktree() {
    (
      set -euo pipefail
      refuse_git_replacements "$FEDIMINT_WORKTREE"
      refuse_hidden_index_flags "$FEDIMINT_WORKTREE"
      test "$(git -C "$FEDIMINT_WORKTREE" rev-parse HEAD)" = "$FEDIMINT_REV"
      test -z "$(git -C "$FEDIMINT_WORKTREE" ls-files --others --exclude-standard)"
      git -C "$FEDIMINT_WORKTREE" diff --cached --quiet
      test "$(git -C "$FEDIMINT_WORKTREE" diff --name-only)" = "devimint/src/cli.rs"
      test -z "$(git -C "$FEDIMINT_WORKTREE" diff --summary)"
      expected_dir=$(mktemp -d)
      trap 'rm -rf "$expected_dir"' EXIT
      mkdir -p "$expected_dir/devimint/src"
      git -C "$FEDIMINT_WORKTREE" show HEAD:devimint/src/cli.rs > "$expected_dir/devimint/src/cli.rs"
      git -C "$expected_dir" apply --check "$TWO_FED_PATCH"
      git -C "$expected_dir" apply "$TWO_FED_PATCH"
      cmp "$expected_dir/devimint/src/cli.rs" "$FEDIMINT_WORKTREE/devimint/src/cli.rs"
    )
  }
  verify_exact_two_fed_worktree
  refuse_cargo_config_for_dir "$FEDIMINT_WORKTREE"
  refuse_ambient_rust_build_overrides
  cd "$FEDIMINT_WORKTREE"
  # The pinned flake sources this ignored local hook during `nix develop`; reject it again
  # immediately before the build in case the worktree changed after the §1 provisioning check.
  if [[ -e "$FEDIMINT_WORKTREE/.shrc.local" || -L "$FEDIMINT_WORKTREE/.shrc.local" ]]; then
    echo "refusing release build: ignored .shrc.local exists in $FEDIMINT_WORKTREE" >&2
    exit 1
  fi
  if [[ -n "${CARGO_BUILD_TARGET_DIR:-}" || -n "${CARGO_TARGET_DIR:-}" ]]; then
    echo "refusing release build: CARGO_BUILD_TARGET_DIR and CARGO_TARGET_DIR must be unset in the clean shell" >&2
    exit 1
  fi
  RELEASE_DEVIMINT="$FEDIMINT_WORKTREE/target-nix/release/devimint"
  reset_exact_target_dir "$FEDIMINT_WORKTREE/target-nix"
  run_exact_cargo build --release --locked --workspace --bins \
    --target-dir "$FEDIMINT_WORKTREE/target-nix"
  test -x "$RELEASE_DEVIMINT"
  ```
  The release build follows patch verification/application in the same command block. Run the
  two-fed harness only with the resulting
  `"$FEDIMINT_WORKTREE/target-nix/release/devimint"` binary, not a devimint built from a
  different revision.

## 2. Run the patched two-federation harness + drive it
```bash
set -euo pipefail
if [[ -z "${FEDIMINT_WORKTREE:-}" ]]; then
  echo "refusing run: FEDIMINT_WORKTREE is unset; run §1 first" >&2
  exit 1
fi
if [[ -z "${FEDIMINT_REV:-}" ]]; then
  echo "refusing run: FEDIMINT_REV is unset; run §1 first" >&2
  exit 1
fi
if ! declare -F derive_fedimint_rev >/dev/null; then
  echo "refusing run: the pin derivation from §1 is unavailable in this shell; run §1 again" >&2
  exit 1
fi
if ! declare -F verify_exact_two_fed_worktree >/dev/null; then
  echo "refusing run: the exact patch verifier from §1 is unavailable; run §1 again" >&2
  exit 1
fi
if ! declare -F verify_pinned_worktree >/dev/null; then
  echo "refusing run: the registered-worktree verifier from §1 is unavailable; run §1 again" >&2
  exit 1
fi
if ! declare -F refuse_cargo_config_for_dir >/dev/null; then
  echo "refusing run: the Cargo-config guard from §1 is unavailable; run §1 again" >&2
  exit 1
fi
if ! declare -F refuse_ambient_rust_build_overrides >/dev/null; then
  echo "refusing run: the ambient-build guard from §1 is unavailable; run §1 again" >&2
  exit 1
fi
if ! declare -F run_exact_nix_develop >/dev/null; then
  echo "refusing run: the exact Nix-environment helper from §1 is unavailable; run §1 again" >&2
  exit 1
fi
if ! declare -F run_exact_cargo >/dev/null; then
  echo "refusing run: the exact Cargo-environment helper from §1 is unavailable; run §1 again" >&2
  exit 1
fi
if ! declare -F reset_exact_target_dir >/dev/null; then
  echo "refusing run: the exact target-reset helper from §1 is unavailable; run §1 again" >&2
  exit 1
fi
if ! declare -F refuse_git_replacements >/dev/null ||
   ! declare -F refuse_hidden_index_flags >/dev/null; then
  echo "refusing run: the Git-integrity guards from §1 are unavailable; run §1 again" >&2
  exit 1
fi
verify_exact_two_fed_launch_state() (
  set -euo pipefail
  local current_fedimint_rev
  current_fedimint_rev="$(derive_fedimint_rev)"
  if [[ "$current_fedimint_rev" != "$FEDIMINT_REV" ]]; then
    echo "refusing run: wallet manifests now pin $current_fedimint_rev, not prepared pin $FEDIMINT_REV" >&2
    exit 1
  fi
  if [[ -v CARGO_BUILD_TARGET_DIR || -v CARGO_TARGET_DIR ]]; then
    echo "refusing run: CARGO_BUILD_TARGET_DIR and CARGO_TARGET_DIR must be unset before entering the devshell" >&2
    exit 1
  fi
  verify_pinned_worktree
  cd "$FEDIMINT_WORKTREE"
  verify_exact_two_fed_worktree
  refuse_cargo_config_for_dir "$FEDIMINT_WORKTREE"
  refuse_ambient_rust_build_overrides
  if [[ -e "$FEDIMINT_WORKTREE/.shrc.local" || -L "$FEDIMINT_WORKTREE/.shrc.local" ]]; then
    echo "refusing run: ignored .shrc.local exists in $FEDIMINT_WORKTREE; the pinned flake sources it" >&2
    exit 1
  fi
)
verify_exact_two_fed_launch_state
echo "OUTER PREFLIGHT COMPLETE"

# This debug helper is defined but not invoked by the pasted block. It accepts only the
# CLI-only smokes whose headers build a debug wallet-cli; daemon and profile-specific smokes
# (especially the release soak) must use their complete header launches.
run_two_fed_cli_smoke() (
  set -euo pipefail
  if [[ "$#" -ne 1 || "$1" != /* || ! -f "$1" || ! -r "$1" ]]; then
    echo "usage: run_two_fed_cli_smoke /absolute/path/to/readable-cli-only-smoke.sh" >&2
    exit 1
  fi
  local smoke_script="$1"
  local smoke_name
  smoke_name="$(basename "$smoke_script")"
  if [[ "$smoke_script" != "$WALLETS_REPO/wallet-cli/tests/$smoke_name" ]]; then
    echo "refusing smoke outside $WALLETS_REPO/wallet-cli/tests: $smoke_script" >&2
    exit 1
  fi
  case "$smoke_name" in
    smoke_crash_move_devimint.sh|smoke_devimint.sh|smoke_directinflow_devimint.sh|\
    smoke_discover_devimint.sh|smoke_evacuate_devimint.sh|smoke_history_devimint.sh|\
    smoke_money_devimint.sh|smoke_move_devimint.sh|smoke_probe_devimint.sh|\
    smoke_tick_devimint.sh) ;;
    *)
      echo "refusing non-CLI-only or profile-specific smoke; use its complete header: $smoke_name" >&2
      exit 1
      ;;
  esac
  verify_exact_two_fed_launch_state
  cd "$FEDIMINT_WORKTREE"
  refuse_cargo_config_for_dir "$WALLETS_REPO"
  (
    cd "$WALLETS_REPO"
    if [[ -e .shrc.local || -L .shrc.local ]]; then
      echo "refusing wallets repository-local .shrc.local before wallet build" >&2
      exit 1
    fi
    reset_exact_target_dir "$WALLETS_REPO/target-nix"
    run_exact_cargo build \
      --locked --target-dir "$WALLETS_REPO/target-nix" -p wallet-cli
    test -x "$WALLETS_REPO/target-nix/debug/wallet-cli"
  )
  verify_exact_two_fed_launch_state
  cd "$FEDIMINT_WORKTREE"
  SMOKE_SCRIPT="$smoke_script" run_exact_nix_develop -c bash -c '
  set -euo pipefail
  export CARGO_PROFILE=release
  source scripts/_common.sh
  add_target_dir_to_path
  # Remove every inherited devimint/Fedimint fixture input before setting the exact harness
  # choices below. Devimint exports fresh FM_* runtime values for the new fixture.
  for variable in $(compgen -v FM_ || true); do
    unset "$variable"
  done
  # Do not let caller overrides or test hooks select stale wallet binaries or alter the fixture.
  for variable in $(compgen -v WALLET_CLI_ || true) $(compgen -v WALLETD_ || true); do
    unset "$variable"
  done
  export WALLET_CLI_BIN="$WALLETS_REPO/target-nix/debug/wallet-cli"
  test -x "$WALLET_CLI_BIN"
  export FM_DISCOVER_API_VERSION_TIMEOUT=10
  PINNED_FEDIMINT_BIN_DIR="$FEDIMINT_WORKTREE/target-nix/release"
  export FM_FEDIMINTD_BASE_EXECUTABLE="$PINNED_FEDIMINT_BIN_DIR/fedimintd"
  export FM_FEDIMINT_CLI_BASE_EXECUTABLE="$PINNED_FEDIMINT_BIN_DIR/fedimint-cli"
  export FM_GATEWAYD_BASE_EXECUTABLE="$PINNED_FEDIMINT_BIN_DIR/gatewayd"
  export FM_GATEWAY_CLI_BASE_EXECUTABLE="$PINNED_FEDIMINT_BIN_DIR/gateway-cli"
  export FM_RECURRINGD_BASE_EXECUTABLE="$PINNED_FEDIMINT_BIN_DIR/fedimint-recurringd"
  for binary in "$FM_FEDIMINTD_BASE_EXECUTABLE" "$FM_FEDIMINT_CLI_BASE_EXECUTABLE" \
                "$FM_GATEWAYD_BASE_EXECUTABLE" "$FM_GATEWAY_CLI_BASE_EXECUTABLE" \
                "$FM_RECURRINGD_BASE_EXECUTABLE"; do
    test -x "$binary"
  done
  export FM_DEVIMINT_STATIC_DATA_DIR="$PWD/devimint/share"   # the alias wrappers
  export RUST_LOG=warn
  export FM_ENABLE_MODULE_LNV1=1                             # §3 includes working lnv1 forms
  export FM_ENABLE_MODULE_MINT=1                             # wallet-cli primary module: mint v1
  export FM_ENABLE_MODULE_WALLET=1                           # wallet module required by dev-fed
  export FM_ENABLE_MODULE_LNV2=1                             # ensure lnv2 + LDK gateway
  export FM_NUM_FEDS=2
  DEVIMINT_BIN="$PINNED_FEDIMINT_BIN_DIR/devimint"
  test -x "$DEVIMINT_BIN"
  "$DEVIMINT_BIN" --link-test-dir "$FEDIMINT_WORKTREE/target-nix/devimint" \
    --num-feds 2 dev-fed --exec bash "$SMOKE_SCRIPT"
  '
)
```
- The block performs the outer preflight and defines `run_two_fed_cli_smoke`; it does not
  launch a fixture by itself. For a listed CLI-only smoke, invoke it with its absolute path,
  for example
  `run_two_fed_cli_smoke "$WALLETS_REPO/wallet-cli/tests/smoke_evacuate_devimint.sh"`.
  Use a smoke file's complete header launch for daemon or profile-specific inputs.
- `dev-fed` spins up bitcoind + esplora + LND node + LDK node + LND/LDK gateways + a
  4-guardian federation (DKG), opens a channel, pegs in a client (~1M sats), then runs
  `--exec <cmd>` with the env set and **tears down after** (one-shot). For a long-running
  fed, drop `--exec` (it holds until shutdown) and use `devimint rpc env` / `rpc wait` from
  another shell.
- `--num-feds N` (CommonArgs, before the subcommand) = number of federations. `-n`/`--fed-size`
  = guardians per fed (default 4).
- Bring-up takes ~1-3 min.
- `wallet-cli`'s primary module is mint v1, which it uses to read its balance. This pinned
  devimint does not enable mint v1 unless `FM_ENABLE_MODULE_MINT=1` is explicitly requested
  (and the wallet module is also required), so an invocation with only lnv2 enabled fails
  immediately with `Primary module not available`.

### Env available inside `--exec`
- `FM_INVITE_CODE` — fed-0's invite. `FM_DATA_DIR`, `FM_CLIENT_DIR` (=`$FM_DATA_DIR/clients/default-0`).
- `FM_PORT_GW_LDK`, `FM_PORT_GW_LND` — gateway API ports. `FM_BTC_CLIENT` (bitcoin-cli wrapper).
- Alias wrappers on PATH: `fm-cli`, `gateway-ldk`, `gateway-lnd`, `bitcoin-cli`, `lncli`.
- The funded internal client is `clients/default-0` (joined to fed-0).

## 3. fedimint-cli cheatsheet (the WORKING forms)
Capture **stdout only** (`2>/dev/null`) — deprecated commands print a `WARN` to stderr that
corrupts JSON parsing. Amounts are **msat** (e.g. `200000` = 200 sat).
```bash
fedimint-cli info | jq .total_amount_msat                       # balance (one number/fed)
# lnv1
fedimint-cli ln-invoice --amount 200000 2>/dev/null             # -> {invoice, operation_id}
fedimint-cli module ln pay <invoice> --force-internal 2>/dev/null  # -> {"Success":{"preimage":..}}
fedimint-cli module ln list-gateways                            # lnv1 gateways
# lnv2 (MUST pass --gateway explicitly; see gotcha below)
GW="http://127.0.0.1:${FM_PORT_GW_LDK}/"
fedimint-cli module lnv2 receive 200000 --gateway "$GW"         # -> [invoice, op_id]
fedimint-cli module lnv2 send <invoice> --gateway "$GW"         # -> "<op_id>"
fedimint-cli module lnv2 await-send <op_id>                     # -> {"Success":"<preimage>"}
fedimint-cli module lnv2 await-receive <op_id>                  # -> "Claimed"
```

## 4. Gotchas (each cost a bring-up to learn)
- **lnv2 gateway list is empty by default.** The LDK gateway connects to the fed
  (`fed_count: 1`) but devimint does NOT auto-register it into the federation's vetted lnv2
  `gateways list`. So `module lnv2 receive/send` with auto-select fail with "No gateways are
  available". **Fix: pass `--gateway "http://127.0.0.1:$FM_PORT_GW_LDK/"` explicitly** — the
  client uses it directly (this is what devimint's own tests do: `lnv2_send(&c, &gw.address(), inv)`).
- **Deprecated top-level `ln-invoice`/`ln-pay` warn on stderr** ("Use `module ln ...`"); the
  JSON is on stdout. Use `2>/dev/null`. Note `module ln invoice` has DIFFERENT (positional)
  syntax than `ln-invoice --amount`.
- **`supports_lnv2()` is true by DEFAULT** (unset env → enabled); set `FM_ENABLE_MODULE_LNV2=1`
  to be explicit. (`devimint/src/util.rs::supports_lnv2` at the pinned revision.)
- **Vanilla `dev-fed` NEVER stands up a second federation — `--num-feds 2` only reserves
  ports.** (An earlier version of this note claimed fed-1 comes up unjoined; wrong — it does
  not come up at all.) For a real two-fed test, use the exact-pinned patch-and-release-build
  procedure in §1, then the absolute release-binary invocation in §2; do not apply the patch
  to an arbitrary checkout or use a `devimint` resolved from PATH. The patched harness stands
  up federation B, connects the LDK gateway to it, pegs in B-side liquidity, and exposes
  `FED_B_INVITE` to the `--exec` script. A single client calling receive and then send on that
  same invoice through the same gateway exercises `is_direct_swap`.
- Don't `2>&1` into a var you `jq`; route logs elsewhere.

## 5. What was validated (see fedimint-mechanics.md "Live validation")
receive non-idempotency; lnv1 internal pay + dedup; **lnv2 `is_direct_swap`** (await-send
`Success`+preimage, await-receive `Claimed`, fee-only balance change) and **lnv2 dedup**
(re-send → `"This invoice has already been paid"`, no second debit). Validation scripts are
in the session scratchpad (`tv3.sh`, `lnv2swap.sh`).

## 6. For the original Phase 1 harness (formerly TODO T4)
- Bootstrap the fed **once per test session/CI job** (the bring-up is the cost), then run
  many tests against it; per test use fresh client DBs + amounts. devimint dev-fed is the
  one-fsync-domain fixture.
- Drive via `fedimint-cli` (above) or the client lib directly. For two-fed cross moves, use
  the two-fed harness patch (see the `--num-feds` gotcha in §4); the wallet-cli smokes in
  `wallet-cli/tests/smoke_*_devimint.sh` are the working references.
- Crash-resume test: kill the client/process mid-operation, reopen the client, assert the
  operation completes (the executor self-resumes) and balances are exactly-once.
