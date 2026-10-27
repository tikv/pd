# Fixed-role Full / LogOnly Store pools

This opt-in deployment places data peers and LogOnly witness peers on different
hardware. It requires matching TiKV support; the companion CSE implementation is
[TiKV draft #6381](https://github.com/tidbcloud/cloud-storage-engine/pull/6381).
A LogOnly Store never maintains user data or becomes a Full Store, including
while it temporarily leads Raft to repair a Full voter.

TiKV declares its local, restart-fixed `raftstore.store-capability` as a reserved
Store metadata label `cse-store-capability`. PD recognizes `full-v1` and
`log-only-v1`; an absent label preserves legacy witness behavior. Unknown versions
and duplicate declarations are rejected. This uses existing Store metadata and
does not require a kvproto change. Explicit declarations cannot be changed or
removed by Store label updates, including force replacement and reboot
registration. A legacy Store may declare Full on upgrade; a LogOnly Store must
register with a new identity. LogOnly Stores cannot bootstrap a data Region.

Enable placement rules and existing witness support on matching software. For
2F1W, replace the applicable default rule with two rules; do not leave an
unconstrained default replica rule covering the same key range:

Submit this override group with `POST /pd/api/v1/config/placement-rule?partial`.
It supersedes the lower-priority default group without changing the legacy
`replication.max-replicas` setting:

```json
[{
  "group_id":"cse-log-store", "group_index":100, "group_override":true,
  "rules":[
    {"group_id":"cse-log-store","id":"full","role":"voter","count":2,"is_witness":false,
     "label_constraints":[
       {"key":"store-class","op":"in","values":["full"]},
       {"key":"cse-store-capability","op":"in","values":["full-v1"]}]},
    {"group_id":"cse-log-store","id":"log","role":"voter","count":1,"is_witness":true,
     "label_constraints":[
       {"key":"store-class","op":"in","values":["log"]},
       {"key":"cse-store-capability","op":"in","values":["log-only-v1"]}]}
  ]
}]
```

`store-class` is an example mutable placement label. Both it and the independent
capability must match. A label edit cannot authorize Full data on a Log Store.
Initialization learners retain their Full/LogOnly capability when promoted.

## Recovery protocol and rollout

Protocol v2 uses the independent `metapb.Store.log_only_protocol_version` field.
The immutable Full/LogOnly role label does not advertise software compatibility.
PD advertises its supported version in `GetClusterInfo`; when Scheduling runs
independently, this is capped by the active primary's election declaration.
Missing or stale Scheduling capability returns zero. Upgrade all PD/Scheduling
instances and participating TiKV/reader components before enabling dedicated W.

Activating or extending a dedicated LogOnly group requires compatible retained
members and destinations, including data learners. Old TiFlash/reader processes
without a v2 declaration are not automatically certified by being non-voters.
Legacy groups keep their existing behavior. Cleanup, leaving joint state and
removing W to return to a Full-only group remain available. A Store identity that declared protocol v2 keeps that minimum (including data
readers) even after returning to 3F: topology
rollback does not prove that old readers can consume persisted DFS object copies.
Software/reader rollback requires a separate controlled migration or replacement
procedure, not clearing the protocol declaration.

A leader reports `RegionRecoveryStatus` with explicit state, data-service
readiness, current/historical size provenance, selected Full and session ID.
PD preserves this report through Scheduling forwarding and sparse Region sync.
A v2 leader omitting its status is treated as not ready. Legacy absence remains
legacy behavior; labels and zero size are not used to infer recovery state.

During recovery or unready/historical-size reports, PD pauses discretionary
balance, split, merge and ordinary leader transfer. Full initialization,
promotion, safe retirement of failed peers and completion of a joint transition
remain possible. A recovering W retains control leadership while Full repair is
built; it never becomes a new ordinary leader-transfer destination. TiKV owns the
typed recovery handoff and validates its target/session and data readiness.

## Repair and migration

- A failed Full is replaced on compatible Full hardware, after the normal Store
  down-time threshold. Do not convert the existing W, and do not add a temporary
  W as a substitute for Full redundancy. Short down events leave the existing
  configuration intact instead of converting W.
- A failed W is replaced by a new LogOnly learner on a compatible Store. Let
  snapshot installation, catch-up and safe ConfChange complete before retiring
  the source. LogOnly initialization does not require Full data materialization.
- Placement fitting, replica selection, operator construction and ordinary/joint
  operator-step validation enforce capability. Raw/manual operators cannot bypass
  the checks. Normal leader targets remain data-capable Full voters.
- Witness balancing on explicit Log Stores migrates W by adding a new LogOnly
  peer and removing the source. The legacy role-swap optimization is not used.
- Missing compatible capacity leaves repair pending. Turning off witness
  scheduling must not convert an existing explicit Log Store into Full, but
  normal witness replacement requires witness scheduling to remain enabled.
- Exit the mode by adding enough Full peers on Full hardware to restore 3F, then
  remove W and retire the Log Stores. Changing a placement label or the witness
  switch is not a rollback procedure.

Unmarked Stores preserve previous behavior. TiKV must still enforce its durable
local capability before recovery, creation and snapshot installation; a reserved
PD declaration is not a substitute for that local check. This draft does not
provide an upstream TiKV witness implementation, minimal Log Store server
runtime, production resource sizing, or all CSE RecoveringLeader protocol and
peripheral rollout work.

The legacy PD unsafe recovery protocol is rejected while a non-retired LogOnly
Store is registered. It assumes every TiKV report can become a data leader or
host a newly created Full Region. Selecting an older Full instead could discard
a newer log suffix held by W; supporting this requires a separate recovery
protocol. This restriction does not affect normal election, W-to-F repair or
recovery transfer, which continue without PD.

## Tests

Core, placement/filter, operator, checker, scheduler, bootstrap and Store label
persistence tests cover the declaration and restrictions. The cross-repository
end-to-end test lives in the companion TiKV PR. Build this branch with
`make pd-server-basic`, then run in the TiKV worktree:

```sh
CSE_LOG_STORE_PD_BINARY=/absolute/path/to/pd-server cargo test --package tests --test cloud_engine --features testexport test_log_store_real_pd_repairs_full_and_witness_failures -- --ignored
```

It starts an isolated real PD process and five TiKV nodes backed by shared mock
S3, installs 2F1W rules, fails a Full and then W, verifies automatic replacements
on compatible pools, restarts the replacement W and checks acknowledged data.
