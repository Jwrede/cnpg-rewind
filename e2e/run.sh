#!/usr/bin/env bash
# End-to-end test against a kind cluster with the CSI hostpath driver,
# the external snapshotter and the CloudNativePG operator.
# Needs: docker, kind, kubectl. Runs in CI (.github/workflows/ci.yml).
set -euo pipefail
cd "$(dirname "$0")/.."

CNPG_VERSION=${CNPG_VERSION:-1.30.1}
CSI_DRIVER_VERSION=${CSI_DRIVER_VERSION:-v1.18.0}
SNAPSHOTTER_VERSION=${SNAPSHOTTER_VERSION:-v8.6.0}
PROVISIONER_VERSION=${PROVISIONER_VERSION:-v6.3.0}
ATTACHER_VERSION=${ATTACHER_VERSION:-v4.13.0}
RESIZER_VERSION=${RESIZER_VERSION:-v2.2.1}
CLUSTER=${KIND_CLUSTER:-cnpg-rewind}
CSI=https://raw.githubusercontent.com/kubernetes-csi
NS=app

log() { printf '\n=== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
k() { kubectl "$@"; }

if [[ "${KEEP_CLUSTER:-}" == "" ]]; then
  trap 'kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true' EXIT
fi

log "kind cluster"
kind create cluster --name "$CLUSTER" --wait 120s

log "CSI hostpath driver and snapshot controller (same steps as CloudNativePG's own e2e setup)"
for crd in volumesnapshotclasses volumesnapshotcontents volumesnapshots; do
  k apply -f "$CSI/external-snapshotter/$SNAPSHOTTER_VERSION/client/config/crd/snapshot.storage.k8s.io_$crd.yaml"
done
k apply -f "$CSI/external-snapshotter/$SNAPSHOTTER_VERSION/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml"
k apply -f "$CSI/external-snapshotter/$SNAPSHOTTER_VERSION/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml"
k apply -f "$CSI/external-snapshotter/$SNAPSHOTTER_VERSION/deploy/kubernetes/csi-snapshotter/rbac-csi-snapshotter.yaml"
k apply -f "$CSI/external-provisioner/$PROVISIONER_VERSION/deploy/kubernetes/rbac.yaml"
k apply -f "$CSI/external-attacher/$ATTACHER_VERSION/deploy/kubernetes/rbac.yaml"
k apply -f "$CSI/external-resizer/$RESIZER_VERSION/deploy/kubernetes/rbac.yaml"
k apply -f "$CSI/csi-driver-host-path/$CSI_DRIVER_VERSION/deploy/kubernetes-1.34/hostpath/csi-hostpath-driverinfo.yaml"
curl -fsSL --retry 5 "$CSI/csi-driver-host-path/$CSI_DRIVER_VERSION/deploy/kubernetes-1.34/hostpath/csi-hostpath-plugin.yaml" |
  sed "s|registry.k8s.io/sig-storage/hostpathplugin:.*|registry.k8s.io/sig-storage/hostpathplugin:$CSI_DRIVER_VERSION|" | k apply -f -
k apply -f "$CSI/csi-driver-host-path/$CSI_DRIVER_VERSION/deploy/kubernetes-1.34/hostpath/csi-hostpath-snapshotclass.yaml"
k patch volumesnapshotclass csi-hostpath-snapclass -p '{"parameters":{"ignoreFailedRead":"true"}}' --type merge
k apply -f "$CSI/csi-driver-host-path/$CSI_DRIVER_VERSION/examples/csi-storageclass.yaml"
k rollout status statefulset/csi-hostpathplugin --timeout=300s
k -n kube-system rollout status deployment/snapshot-controller --timeout=300s

log "CloudNativePG $CNPG_VERSION"
k apply --server-side -f "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-${CNPG_VERSION%.*}/releases/cnpg-$CNPG_VERSION.yaml"
k -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=300s


log "cnpg-rewind binary"
go build -o bin/cnpg-rewind ./cmd/cnpg-rewind
R=bin/cnpg-rewind

log "Postgres cluster pg (2 instances, volume snapshot backups owned by the cluster)"
k create namespace "$NS"
cat <<YAML | k -n "$NS" apply -f -
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: pg
  labels:
    team: data
spec:
  instances: 2
  storage:
    storageClass: csi-hostpath-sc
    size: 1Gi
  walStorage:
    storageClass: csi-hostpath-sc
    size: 1Gi
  backup:
    volumeSnapshot:
      className: csi-hostpath-snapclass
      snapshotOwnerReference: cluster
YAML
k -n "$NS" wait --for=condition=Ready cluster/pg --timeout=600s

primary() { k -n "$NS" get pods -l "cnpg.io/cluster=$1,cnpg.io/instanceRole=primary" -o jsonpath='{.items[0].metadata.name}'; }
psql() { k -n "$NS" exec "$(primary "$1")" -c postgres -- psql -v ON_ERROR_STOP=1 -U postgres -d app -At -c "$2"; }
psql pg "create table t (x int); grant select on t to app; insert into t values (1);"

log "backup 1: from a ScheduledBackup with backupOwnerReference: cluster (deleted with the cluster unless detached)"
cat <<YAML | k -n "$NS" apply -f -
apiVersion: postgresql.cnpg.io/v1
kind: ScheduledBackup
metadata:
  name: nightly
spec:
  schedule: "0 0 3 * * *"
  immediate: true
  method: volumeSnapshot
  backupOwnerReference: cluster
  cluster:
    name: pg
YAML
for _ in $(seq 1 60); do
  B1=$(k -n "$NS" get backups -l cnpg.io/scheduled-backup=nightly -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  [[ -n "$B1" ]] && break
  sleep 2
done
[[ -n "$B1" ]] || fail "ScheduledBackup created no backup"
k -n "$NS" wait --for=jsonpath='{.status.phase}'=completed "backup/$B1" --timeout=300s
k -n "$NS" patch scheduledbackup nightly --type merge -p '{"spec":{"suspend":true}}'
k -n "$NS" get backup "$B1" -o jsonpath='{.metadata.ownerReferences[*].kind}' | grep -q Cluster || fail "$B1 is not owned by the cluster"

psql pg "insert into t values (2);"
log "backup 2: manual"
cat <<YAML | k -n "$NS" apply -f -
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata:
  name: manual
spec:
  method: volumeSnapshot
  cluster:
    name: pg
YAML
k -n "$NS" wait --for=jsonpath='{.status.phase}'=completed backup/manual --timeout=300s
psql pg "insert into t values (3);"
[[ "$(psql pg 'select count(*) from t')" == 3 ]] || fail "setup: want 3 rows"
k -n "$NS" get backups,volumesnapshots

APP_PW=$(k -n "$NS" get secret pg-app -o jsonpath='{.data.password}')
APP_UID=$(k -n "$NS" get secret pg-app -o jsonpath='{.metadata.uid}')
OLD_UID=$(k -n "$NS" get cluster pg -o jsonpath='{.metadata.uid}')
B1_SNAPS=$(k -n "$NS" get backup "$B1" -o jsonpath='{.status.snapshotBackupStatus.elements[*].name}')

log "plan: changes nothing"
PLAN=$($R plan -n "$NS" --backup "$B1" pg)
echo "$PLAN"
echo "$PLAN" | grep -q "restore from:   backup $B1 (method volumeSnapshot" || fail "plan: source"
echo "$PLAN" | grep -q "keeps:          name, services, secret pg-app (database app, owner app)" || fail "plan: identity"
echo "$PLAN" | grep -q "detach:         Backup $B1 " || fail "plan: owned backup not detached"
echo "$PLAN" | grep -q "detach:         Secret pg-app " || fail "plan: app secret not detached"
for s in $B1_SNAPS; do echo "$PLAN" | grep -q "detach:         VolumeSnapshot $s " || fail "plan: snapshot $s not detached"; done
echo "$PLAN" | grep -q "^      volumeSnapshots:" || fail "plan: manifest"
[[ "$(k -n "$NS" get cluster pg -o jsonpath='{.metadata.uid}')" == "$OLD_UID" ]] || fail "plan touched the cluster"

log "run: rewind pg to $B1 (rehearsal, safety backup, check query)"
OUT=$($R run -n "$NS" --backup "$B1" --check-sql "select count(*) from t" --yes -o /tmp/pg-rewound.yaml pg)
echo "$OUT"
echo "$OUT" | grep -q "check query on pg-rewind-check: 1" || fail "rehearsal check"
echo "$OUT" | grep -q "check query on pg: 1" || fail "final check"
echo "$OUT" | grep -q "==> done: pg is ready, restored from backup $B1" || fail "run summary"

log "verify the rewound cluster"
[[ "$(k -n "$NS" get cluster pg -o jsonpath='{.metadata.uid}')" != "$OLD_UID" ]] || fail "cluster was not recreated"
[[ "$(psql pg 'select count(*) from t')" == 1 ]] || fail "rewound cluster does not have exactly row 1"
k -n "$NS" wait --for=jsonpath='{.status.readyInstances}'=2 cluster/pg --timeout=600s
[[ "$(k -n "$NS" get cluster pg -o jsonpath='{.metadata.labels.team}')" == data ]] || fail "label lost"
[[ "$(k -n "$NS" get cluster pg -o 'jsonpath={.metadata.annotations.cnpg-rewind\.jwrede\.github\.io/backup}')" == "$B1" ]] || fail "annotation"
[[ "$(k -n "$NS" get secret pg-app -o jsonpath='{.metadata.uid}')" == "$APP_UID" ]] || fail "app secret was replaced"
[[ "$(k -n "$NS" get secret pg-app -o jsonpath='{.data.password}')" == "$APP_PW" ]] || fail "app password changed"
PW=$(echo "$APP_PW" | base64 -d)
ROWS=$(k -n "$NS" exec "$(primary pg)" -c postgres -- env PGPASSWORD="$PW" psql -h pg-rw -U app -d app -At -c "select count(*) from t")
[[ "$ROWS" == 1 ]] || fail "app login through pg-rw with the old password: $ROWS"
k -n "$NS" get cluster pg-rewind-check >/dev/null 2>&1 && fail "rehearsal cluster left behind"
SAFETY=$(k -n "$NS" get backups -l app.kubernetes.io/managed-by=cnpg-rewind -o jsonpath='{.items[0].metadata.name}')
[[ -n "$SAFETY" ]] || fail "no safety backup"
for b in "$B1" manual "$SAFETY"; do
  [[ "$(k -n "$NS" get backup "$b" -o jsonpath='{.status.phase}')" == completed ]] || fail "backup $b lost"
  for s in $(k -n "$NS" get backup "$b" -o jsonpath='{.status.snapshotBackupStatus.elements[*].name}'); do
    [[ "$(k -n "$NS" get volumesnapshot "$s" -o jsonpath='{.status.readyToUse}')" == true ]] || fail "snapshot $s of $b lost"
  done
done
grep -q "name: $B1" /tmp/pg-rewound.yaml || fail "manifest file"

log "a new backup of the rewound cluster works"
cat <<YAML | k -n "$NS" apply -f -
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata:
  name: after
spec:
  method: volumeSnapshot
  cluster:
    name: pg
YAML
k -n "$NS" wait --for=jsonpath='{.status.phase}'=completed backup/after --timeout=300s

log "undo: rewind to the safety backup $SAFETY (no rehearsal, no second safety backup)"
OUT=$($R run -n "$NS" --backup "$SAFETY" --rehearse=false --safety-backup=false --yes pg)
echo "$OUT" | grep -q "safety backup:  skipped" || fail "undo: safety backup not shown as skipped"
echo "$OUT"
[[ "$(psql pg 'select count(*) from t')" == 3 ]] || fail "undo did not bring back 3 rows"
k -n "$NS" get clusters,backups,volumesnapshots

log "PASS"
