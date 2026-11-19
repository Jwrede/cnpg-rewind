# cnpg-rewind

Restore a [CloudNativePG](https://cloudnative-pg.io) `Cluster` in place from one of its own backups.
The cluster keeps its name, its `-rw`/`-ro`/`-r` services and its application secret, so
applications reconnect with the credentials they already have.

CloudNativePG can only restore into a *new* cluster (`bootstrap.recovery`). Going back to an
earlier state of an existing cluster means: write a recovery manifest by hand, make sure the
backups survive the deletion of the cluster, delete it, recreate it under the same name, and
pick a new `serverName` so the WAL archive check does not fail. `cnpg-rewind` does these steps
in order and stops before anything destructive if the restore would not work.

## Why

- [cloudnative-pg/cloudnative-pg#5203](https://github.com/cloudnative-pg/cloudnative-pg/issues/5203)
  "In-place recovery from backup": 123 reactions, 37 comments, open since 07/2024 and still in
  triage; kept open against the stale bot at least four times, last request on 2026-11-09 from a
  team running a PostgreSQL DBaaS on CloudNativePG ("we can only do this by orchestrating delete
  & recreate with bootstrap.recovery and a new serverName generation [...] it is destructive
  before the restore succeeds").
- The workaround posted in that thread (change `destinationPath`, recreate the cluster) is the
  manual procedure this tool automates. Other operators (Crunchy PGO, Percona) have an in-place
  restore; CloudNativePG does not.
- Related: [#5778](https://github.com/cloudnative-pg/cloudnative-pg/issues/5778) "managing cnpg
  in GitOps workflows" (69 reactions), where recovering an existing cluster is one of the
  steps that needs manual manifest edits.

## What a run does

```
cnpg-rewind run -n app --backup <backup> --check-sql "select count(*) from orders" pg
```

1. **Plan** (also available alone as `cnpg-rewind plan`, which changes nothing): reads the
   Cluster and its Backups, picks the backup (`--backup`, or the newest completed one, or the
   newest that ended before `--to`), and builds the new Cluster manifest:
   `spec.bootstrap` is replaced by `recovery` (volume snapshots of the backup, or the object
   store as an `externalClusters` source), all other spec fields, labels and annotations are
   kept, `database`/`owner`/`secret` point at the existing application secret.
2. **Rehearsal**: restores the same backup into a throwaway one-instance cluster
   `<name>-rewind-check`, waits until it is ready, runs `--check-sql` there and prints the
   result, then deletes it. If this fails, the real cluster is not touched.
3. **Safety backup** of the current state (`<name>-before-rewind-<time>`), with the cluster's
   configured method. Rewinding to it undoes the rewind.
4. **Detach**: removes the owner reference to the Cluster from the application secret, from
   Backups (for example those created by a `ScheduledBackup` with
   `backupOwnerReference: cluster`) and from VolumeSnapshots (`snapshotOwnerReference: cluster`).
   Without this, deleting the cluster would delete the very backups you want to restore.
5. **Delete** the Cluster (with a UID precondition) and wait until its pods and PVCs are gone.
6. **Create** the Cluster from the new manifest, wait for `Ready`, run `--check-sql` again.

With a WAL archive (`spec.backup.barmanObjectStore` or the Barman Cloud plugin), the new
cluster archives under a new `serverName` (`pg` becomes `pg-rewind1`, then `pg-rewind2`), and
the old archive is the recovery source. That also enables point-in-time recovery with `--to`.

## Install

```
go install github.com/Jwrede/cnpg-rewind/cmd/cnpg-rewind@latest
```

It uses your kubeconfig (`--kubeconfig`, `--context`, `-n`) and needs permission to get, list,
create, patch and delete `clusters` and `backups` (postgresql.cnpg.io), get, list and patch
`volumesnapshots`, get and patch `secrets`, list `pods` and `persistentvolumeclaims`, and
`pods/exec` for `--check-sql`.

## Example

This is the output of the end-to-end test (`e2e/run.sh`): cluster `pg` with two instances,
row 1 written, backup `nightly-...` taken by a `ScheduledBackup`, rows 2 and 3 written later.
The cluster is rewound to the `nightly` backup; the check query counts the rows in table `t`.

```
$ cnpg-rewind run -n app --backup nightly-20261119052715 --check-sql "select count(*) from t" --yes -o /tmp/pg-rewound.yaml pg
Cluster app/pg
  restore from:   backup nightly-20261119052715 (method volumeSnapshot, ended 2026-11-19T05:27:23Z)
  keeps:          name, services, secret pg-app (database app, owner app)
  safety backup:  pg-before-rewind-20261119052728 (method volumeSnapshot)
  rehearsal:      cluster pg-rewind-check, deleted afterwards
  detach:         Secret pg-app (owned by the cluster, would be deleted with it)
  detach:         VolumeSnapshot manual (owned by the cluster, would be deleted with it)
  detach:         VolumeSnapshot manual-wal (owned by the cluster, would be deleted with it)
  detach:         Backup nightly-20261119052715 (owned by the cluster, would be deleted with it)
  detach:         VolumeSnapshot nightly-20261119052715 (owned by the cluster, would be deleted with it)
  detach:         VolumeSnapshot nightly-20261119052715-wal (owned by the cluster, would be deleted with it)
  note:           the Cluster object is deleted and recreated: pods, PVCs, services and operator-generated certificates are recreated, the application secret pg-app is kept
  note:           GitOps tools (Argo CD, Flux) may recreate the old Cluster while it is gone; suspend their sync for this Cluster first and commit the printed manifest afterwards
==> rehearsal: restoring pg-rewind-check from backup nightly-20261119052715
    cluster pg-rewind-check: Setting up primary
    cluster pg-rewind-check: Waiting for the instances to become active
check query on pg-rewind-check: 1
==> delete rehearsal cluster pg-rewind-check
    deletion of pg-rewind-check: 1 pods, 2 PVCs left
==> safety backup pg-before-rewind-20261119052728 of the current state
==> detach Secret pg-app from the cluster
==> detach VolumeSnapshot manual from the cluster
==> detach VolumeSnapshot manual-wal from the cluster
==> detach Backup nightly-20261119052715 from the cluster
==> detach VolumeSnapshot nightly-20261119052715 from the cluster
==> detach VolumeSnapshot nightly-20261119052715-wal from the cluster
==> detach VolumeSnapshot pg-before-rewind-20261119052728 from the cluster
==> detach VolumeSnapshot pg-before-rewind-20261119052728-wal from the cluster
==> delete cluster pg
    deletion of pg: 2 pods, 4 PVCs left
    deletion of pg: 1 pods, 2 PVCs left
==> create cluster pg from backup nightly-20261119052715
    cluster pg: Setting up primary
    cluster pg: Waiting for the instances to become active
    cluster pg: Creating a new replica
    cluster pg: Waiting for the instances to become active
check query on pg: 1
==> done: pg is ready, restored from backup nightly-20261119052715
```

Afterwards the test checks: one row in `t`, both instances ready, label `team: data` kept,
`pg-app` with the same UID and password, `psql -h pg-rw -U app` works with that password, all
four backups and their snapshots still there. The whole run (rehearsal, safety backup,
deletion, recovery of two instances) took about two minutes on a GitHub runner.

## Flags

```
-backup string      name of the Backup to restore (default: newest completed one, before -to if given)
-to string          point-in-time target, RFC 3339 (needs a WAL archive)
-check-sql string   SQL run with psql on the rehearsal and on the restored cluster; output is printed
-rehearse           restore into a throwaway cluster first and stop if that fails (default true)
-safety-backup      back up the current state before deleting the cluster (default true)
-o string           also write the new Cluster manifest to this file
-yes                do not ask for confirmation (otherwise you type the cluster name)
-timeout duration   timeout for each wait (default 20m0s)
-n, -kubeconfig, -context
```

## Tests

- `go test ./...`: plan construction for volume snapshot, Barman Cloud plugin and in-tree
  `barmanObjectStore` clusters (backup choice, recovery source, `serverName` bump, owner
  references, rehearsal and safety backup manifests).
- `e2e/run.sh` (CI, kind + CSI hostpath driver with snapshots + CloudNativePG 1.30.1): two-instance
  cluster with volume snapshot backups owned by the cluster (one from a `ScheduledBackup` with
  `backupOwnerReference: cluster`). Checks that `plan` changes nothing; that `run` rewinds to the
  first backup with rehearsal, safety backup and check query; that the cluster comes back with
  both instances, the old labels, the same application secret (same UID and password) and a
  working login through `pg-rw`; that all backups and snapshots survive; that a new backup of
  the rewound cluster succeeds; and that rewinding to the safety backup brings the later rows
  back. Runs weekly against the current runner image.

## Limitations

- End-to-end tested with volume snapshot backups only. The object store paths (Barman Cloud
  plugin, in-tree `barmanObjectStore`) and `--to` are covered by unit tests of the generated
  manifests, not by a run against a real object store.
- Downtime: the cluster is gone between deletion and the end of the recovery. This is an
  in-place restore, not a switchover.
- Operator-generated certificates (`<name>-ca`, `<name>-server`, `<name>-replication`) are
  recreated. Clients that pin the CA (`sslmode=verify-ca`) need the new one.
- The superuser secret is recreated if it was generated by the operator.
- GitOps tools may recreate the old Cluster while it is deleted. Suspend their sync for this
  Cluster first and commit the manifest written with `-o` afterwards.
- Replica clusters are refused; rewind the primary cluster.
- A run that fails after the deletion leaves no cluster behind; the manifest from `-o` (or
  `plan`) can then be applied by hand. Steps before the deletion leave the cluster untouched.

## License

MIT
