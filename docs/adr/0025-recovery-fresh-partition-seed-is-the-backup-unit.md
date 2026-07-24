---
status: accepted
---
# Recovery: the seed is the backup unit, and recovery never wipes

Two decisions define how a funded wallet is rebuilt. They are recorded together because
the second follows from the first plus one fact about the SDK.

**1. The backup unit is the seed plus the joined federation IDs — never the local stores.**
[ADR-0003](./0003-recovery-silent-backup.md) already established *what* is backed up; this
adds what is *not*. The wallet's two local stores (the client store holding ecash and the
seed, and the bookkeeping store holding the ledger, policy, and federation registry) carry
**no cross-store point-in-time guarantee**. They are restored together from a single
snapshot, or not at all. A mismatched pair — one store from one moment, the other from
another — is **out of contract**, and the wallet does not defend against it. When a store
is lost, the supported path is Recovery from the seed and federation IDs, not a store copy.

**2. Recovery always targets a fresh client partition; it never wipes or reuses one.**
If the federation is already open, recovery **refuses** rather than running a second client
on the same seed. Otherwise it allocates a new partition, recovers into it, and registers
the federation only after recovery completes. Any pre-existing partition is left untouched
and inert.

The forcing fact for (2): `ClientPreview::recover` rejects an already-initialized database.
So reusing a federation's existing partition necessarily means **wiping it before
rebuilding** — and a crash inside that window leaves the federation with neither a registry
row nor a partition, and nothing recording that a recovery owned the prefix. That is
silent, unrecoverable fund loss. Recovering into a fresh partition closes the window *by
construction*: nothing is destroyed, so nothing can be half-destroyed. The alternative —
keeping in-place recovery and making the wipe survivable — requires a durable
recovery marker, prefix reservation, interrupted-recovery resume, and a startup rule for
partitions caught mid-recovery. An earlier attempt built exactly that machinery and did not
converge; this decision deletes the problem instead of the symptom.

## Consequences

- **Orphaned partitions accumulate.** A recovered federation leaves its old partition on
  disk. This is safe, not merely tolerable: the registry drives what is opened, and the
  prefix allocator already refuses to reuse a partition for a different federation, so an
  orphan is inert. Reclaiming them is a **deliberate** GC command, never automatic. The
  cost is disk, and it is bounded by how often recovery is run — which is rarely, by design.
- **Recovering a live federation is refused**, not silently coerced. Consistent with the
  operating rule that there is one seed, one live client store, one daemon.
- **No recovery-lifecycle machinery is needed** — no in-progress marker, no resume, no
  mid-recovery startup rule. These exist only to survive a wipe that no longer happens.
- **A mismatched two-store restore is undefined behavior** and is documented as such in the
  runbook and glossary rather than defended in code. Accepting this is what keeps recovery
  small enough to verify.
- Recovery is a long-running operation and therefore runs as a detached driver task under
  [ADR-0024](./0024-fully-async-intent-model.md); it never blocks the actor. Since the
  federation is unregistered until recovery finishes, a running recovery is invisible to the
  Allocator.
- Recovery must be able to **fail**, not hang. Upstream's client parks a failed module
  recovery forever, making "failed" indistinguishable from "slow"; we carry a patch making it
  complete-or-fail so this decision's error path is reachable at all.
