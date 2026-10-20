# etcd-replicate

`etcd-replicate` continuously copies an etcd v3 keyspace into another etcd v3
endpoint so that both are at **identical revisions**: after source revision R
is applied, the target is at revision R with the same keys, values, create and
mod revisions, versions and leases.

Kubernetes uses revisions as `resourceVersion`s, for watches and for optimistic
concurrency. Because the revisions match, kube-apiserver can be switched from
the source to the target, for example to move to a new etcd cluster or between
etcd and an etcd-compatible store such as [T4](https://github.com/t4db/t4),
without clients relisting, losing watches or hitting a wave of conflicts.

## How it works

Each source revision is applied as one target transaction guarded by a cursor
key stored on the target:

```
If:   ModRevision(cursor) == R-1
Then: the revision's events, Put(cursor, R)
```

The guard makes retries idempotent and keeps two replicators from
interleaving. After every transaction the replicator checks that the target is
at exactly revision R; any divergence stops replication instead of being
papered over. A revision is applied only once a later revision's events arrive
or a progress notification covers it, never on a watch response boundary.

## Install

```bash
go install github.com/t4db/etcd-replicate/cmd/etcd-replicate@latest
```

It can also be embedded as a Go library:

```go
r, err := replicate.New(replicate.Config{Source: src, Target: dst})
if err != nil { ... }
err = r.Run(ctx) // returns a *replicate.HaltError when an operator must act
```

## Requirements

- **The source must spend revisions like etcd.** Every revision must produce
  watch events. etcd does; so does a T4 database created with the meta
  keyspace. A source that spends revisions on bookkeeping its watch stream
  never shows stops replication with an error.
- **The target must be empty** (a fresh etcd cluster) or have been replicated
  into before. The replicator keeps its cursor on the target under
  `/__etcd_replicate/`.
- **The source must still have the history the target needs.** The replicator
  starts from the target's cursor, or from the source's first revision for an
  empty target. If the source compacted that history, replication stops with an
  error. kube-apiserver compacts every 5 minutes by default, so start the
  replicator together with the cluster. Bootstrapping a target from a snapshot
  of an existing database is not available yet.
- **Only the replicator may write to the target.** Protect it with etcd
  authentication so the kube-apiserver credentials cannot write until cutover.

## Running

```bash
etcd-replicate \
  --source-endpoints https://source:2379 \
  --source-cacert ca.pem --source-cert client.pem --source-key client-key.pem \
  --target-endpoints https://etcd-0:2379,https://etcd-1:2379,https://etcd-2:2379 \
  --target-cacert ca.pem --target-cert replicator.pem --target-key replicator-key.pem
```

Run one replicator per target, for example as a single-replica Deployment. A
second replicator cannot corrupt the target: every write is guarded by the
cursor, and the replicator that loses a race stops.

The replicator resumes from the cursor after a restart. It retries transient
errors, such as lost connections, and exits with an error only when an operator
must act.

### Flags

Every flag can also be set with an environment variable: `ETCD_REPLICATE_` and
the flag name in upper case with `-` replaced by `_`, for example
`ETCD_REPLICATE_SOURCE_ENDPOINTS`. Command-line flags take precedence.

| Flag | Default | Description |
|------|---------|-------------|
| `--source-endpoints` | — | Comma-separated etcd v3 endpoints of the source (required) |
| `--source-cacert` | — | CA bundle to verify the source's TLS certificate |
| `--source-cert` | — | Client certificate for the source |
| `--source-key` | — | Client key for the source |
| `--source-user` | — | Username for the source |
| `--source-password` | — | Password for the source |
| `--target-endpoints` | — | Comma-separated etcd v3 endpoints of the target (required) |
| `--target-cacert` | — | CA bundle to verify the target's TLS certificate |
| `--target-cert` | — | Client certificate for the target |
| `--target-key` | — | Client key for the target |
| `--target-user` | — | Username for the target |
| `--target-password` | — | Password for the target |
| `--state-prefix` | `/__etcd_replicate/` | Target key prefix for replication state; source keys under it are not replicated |
| `--lease-ttl-margin` | `10m` | Added to lease TTLs on the target so it never expires a lease before the source; must exceed the longest replicator downtime |
| `--lease-reconcile-interval` | `5s` | How often target leases are kept alive and leases gone from the source are revoked |
| `--metrics-addr` | `0.0.0.0:9091` | HTTP address for `/metrics` and `/healthz`; empty disables |
| `--log-level` | `info` | `debug`, `info`, `warn` or `error` |

### Leases

Leases are created on the target with the same IDs. Their TTL there is the
source's TTL plus `--lease-ttl-margin`. The replicator keeps them alive while
they exist on the source, and revokes them once the source no longer has them
and their keys are gone. Keys attached to a lease are deleted on the target
when the source deletes them, at the same revision. None of this consumes
target revisions.

The target must never expire a lease before the source does: that would
create a revision the source does not have. If the replicator is down longer
than the margin, target leases can expire and replication stops. Set the
margin above the longest downtime you want to tolerate.

### Transaction limits

Each source revision is applied as one target transaction. A source revision
with more operations than etcd's `--max-txn-ops` (default 128) or larger than
`--max-request-bytes` stops replication with an error that names the limit.
Raise the limits on the target etcd if your workload needs it.

## Monitoring

Metrics are served on `--metrics-addr` at `/metrics`, with a liveness endpoint
at `/healthz`.

| Metric                                    | Meaning                                                        |
|-------------------------------------------|----------------------------------------------------------------|
| `etcd_replicate_applied_revision`         | Source revision the target is at.                              |
| `etcd_replicate_source_revision`          | Latest source revision observed.                               |
| `etcd_replicate_lag_revisions`            | Revisions observed but not yet applied.                        |
| `etcd_replicate_applied_revisions_total`  | Source revisions applied.                                      |
| `etcd_replicate_apply_duration_seconds`   | Time to apply one source revision.                             |
| `etcd_replicate_target_leases`            | Leases mirrored on the target.                                 |
| `etcd_replicate_errors_total{op}`         | Retried errors.                                                |
| `etcd_replicate_halted`                   | 1 when replication stopped on an error that needs an operator. |

Alert on `etcd_replicate_halted`, on a growing lag, and on the process not
running.

## When replication stops

The replicator stops instead of letting the target silently diverge. Each
error message names the cause:

| Cause                                                     | What to do                                                   |
|-----------------------------------------------------------|--------------------------------------------------------------|
| The target changed behind the replicator's back           | Rebuild the target from an empty cluster.                    |
| The source compacted history the target still needs       | Rebuild the target from an empty cluster.                    |
| The source skipped revisions                              | The source does not spend revisions like etcd; it cannot be replicated revision-exactly. |
| The target has data but no cursor                         | Replicate into an empty target.                              |
| A lease expired on the target                             | Raise `--lease-ttl-margin`; rebuild the target.              |
| A source revision exceeds the target's transaction limits | Raise `--max-txn-ops` / `--max-request-bytes` on the target and restart. |

A write that bypasses the replicator is detected when the next source revision
is applied, so the target is already inconsistent at that point. Authentication
on the target prevents this.

## Switching kube-apiserver to the target

1. Check that replication is healthy: `etcd_replicate_halted` is 0 and the lag
   is near 0.
2. Stop writes to the source, for example by scaling kube-apiserver down.
3. Wait until `etcd_replicate_applied_revision` equals the source's revision,
   as reported by `etcdctl endpoint status` against the source.
4. Stop the replicator.
5. Give the kube-apiserver credentials access to the target.
6. Point kube-apiserver's `--etcd-servers` at the target and start it.

Watches resume from the resourceVersions clients already have. The
`/__etcd_replicate/` keys stay on the target; kube-apiserver does not read
them.

Moving back replicates in the other direction, into a new, empty database.
That needs the new source's history from its first revision, which a cluster
that has been running for a while no longer has, so in practice it needs
bootstrapping from a snapshot, which is not available yet.

## License

Apache 2.0
