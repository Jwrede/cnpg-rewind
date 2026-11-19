#!/usr/bin/env bash
# End-to-end test of the object store path: kind, cert-manager, CloudNativePG,
# the Barman Cloud plugin and MinIO. Point-in-time rewind with -to, then undo
# through the safety backup. Needs: docker, kind, kubectl, go. Runs in CI.
set -euo pipefail
cd "$(dirname "$0")/.."

CNPG_VERSION=${CNPG_VERSION:-1.30.1}
PLUGIN_VERSION=${PLUGIN_VERSION:-v0.16.0}
CERT_MANAGER_VERSION=${CERT_MANAGER_VERSION:-v1.21.3}
MINIO_IMAGE=${MINIO_IMAGE:-minio/minio:latest}
MC_IMAGE=${MC_IMAGE:-minio/mc:latest}
CLUSTER=${KIND_CLUSTER:-cnpg-rewind-os}
NS=app

log() { printf '\n=== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
k() { kubectl "$@"; }

if [[ "${KEEP_CLUSTER:-}" == "" ]]; then
  trap 'kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true' EXIT
fi

log "kind cluster"
kind create cluster --name "$CLUSTER" --wait 120s

log "cert-manager $CERT_MANAGER_VERSION"
k apply -f "https://github.com/cert-manager/cert-manager/releases/download/$CERT_MANAGER_VERSION/cert-manager.yaml"
for d in cert-manager cert-manager-cainjector cert-manager-webhook; do
  k -n cert-manager rollout status "deployment/$d" --timeout=300s
done

log "CloudNativePG $CNPG_VERSION and Barman Cloud plugin $PLUGIN_VERSION"
k apply --server-side -f "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-${CNPG_VERSION%.*}/releases/cnpg-$CNPG_VERSION.yaml"
k -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=300s
# The plugin's certificates need a ready cert-manager webhook; retry the apply.
for i in 1 2 3 4 5; do
  k apply -f "https://github.com/cloudnative-pg/plugin-barman-cloud/releases/download/$PLUGIN_VERSION/manifest.yaml" && break
  sleep $((i * 5))
done
k -n cnpg-system rollout status deployment/barman-cloud --timeout=300s

log "cnpg-rewind binary"
go build -o bin/cnpg-rewind ./cmd/cnpg-rewind
R=bin/cnpg-rewind

log "MinIO and ObjectStore"
k create namespace "$NS"
k -n "$NS" create secret generic minio --from-literal=ACCESS_KEY_ID=rewind --from-literal=ACCESS_SECRET_KEY=rewind-secret
cat <<YAML | k -n "$NS" apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
spec:
  selector: {matchLabels: {app: minio}}
  template:
    metadata: {labels: {app: minio}}
    spec:
      containers:
        - name: minio
          image: $MINIO_IMAGE
          args: ["server", "/data"]
          env:
            - {name: MINIO_ROOT_USER, valueFrom: {secretKeyRef: {name: minio, key: ACCESS_KEY_ID}}}
            - {name: MINIO_ROOT_PASSWORD, valueFrom: {secretKeyRef: {name: minio, key: ACCESS_SECRET_KEY}}}
          ports: [{containerPort: 9000}]
          readinessProbe: {httpGet: {path: /minio/health/ready, port: 9000}}
          volumeMounts: [{name: data, mountPath: /data}]
      volumes: [{name: data, emptyDir: {}}]
---
apiVersion: v1
kind: Service
metadata:
  name: minio
spec:
  selector: {app: minio}
  ports: [{port: 9000, targetPort: 9000}]
---
apiVersion: batch/v1
kind: Job
metadata:
  name: bucket
spec:
  backoffLimit: 10
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: mc
          image: $MC_IMAGE
          command: ["sh", "-c", "mc alias set s3 http://minio:9000 \$U \$P && mc mb --ignore-existing s3/backups"]
          env:
            - {name: U, valueFrom: {secretKeyRef: {name: minio, key: ACCESS_KEY_ID}}}
            - {name: P, valueFrom: {secretKeyRef: {name: minio, key: ACCESS_SECRET_KEY}}}
---
apiVersion: barmancloud.cnpg.io/v1
kind: ObjectStore
metadata:
  name: store
spec:
  configuration:
    destinationPath: s3://backups/
    endpointURL: http://minio.$NS.svc:9000
    s3Credentials:
      accessKeyId: {name: minio, key: ACCESS_KEY_ID}
      secretAccessKey: {name: minio, key: ACCESS_SECRET_KEY}
    wal:
      compression: gzip
YAML
k -n "$NS" rollout status deployment/minio --timeout=300s
k -n "$NS" wait --for=condition=complete job/bucket --timeout=300s

log "Postgres cluster pg with the plugin as WAL archiver"
cat <<YAML | k -n "$NS" apply -f -
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: pg
spec:
  instances: 1
  storage:
    size: 1Gi
  plugins:
    - name: barman-cloud.cloudnative-pg.io
      isWALArchiver: true
      parameters:
        barmanObjectName: store
YAML
k -n "$NS" wait --for=condition=Ready cluster/pg --timeout=600s
k -n "$NS" wait --for=condition=ContinuousArchiving cluster/pg --timeout=300s

primary() { k -n "$NS" get pods -l "cnpg.io/cluster=$1,cnpg.io/instanceRole=primary" -o jsonpath='{.items[0].metadata.name}'; }
psql() { k -n "$NS" exec "$(primary "$1")" -c postgres -- psql -v ON_ERROR_STOP=1 -U postgres -d app -At -c "$2"; }
archive_now() {
  local seg
  seg=$(psql pg "select pg_walfile_name(pg_switch_wal())")
  for _ in $(seq 1 60); do
    [[ "$(psql pg 'select last_archived_wal from pg_stat_archiver')" == "$seg" ]] && return 0
    sleep 2
  done
  fail "WAL segment $seg was not archived"
}

backup() {
  cat <<YAML | k -n "$NS" apply -f -
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata:
  name: $1
spec:
  method: plugin
  pluginConfiguration:
    name: barman-cloud.cloudnative-pg.io
  cluster:
    name: pg
YAML
  k -n "$NS" wait --for=jsonpath='{.status.phase}'=completed "backup/$1" --timeout=600s
}

psql pg "create table t (x int); insert into t values (1);"
backup base
k -n "$NS" get backup base -o jsonpath='{.status}'; echo
psql pg "insert into t values (2);"
sleep 2
TARGET=$(psql pg "select to_char(now() at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')")
sleep 2
psql pg "insert into t values (3);"
archive_now
[[ "$(psql pg 'select count(*) from t')" == 3 ]] || fail "setup: want 3 rows"
echo "target time: $TARGET"

log "plan for -to $TARGET"
PLAN=$($R plan -n "$NS" --to "$TARGET" pg)
echo "$PLAN"
echo "$PLAN" | grep -q "restore from:   backup base (method plugin" || fail "plan: base backup not chosen"
echo "$PLAN" | grep -q "replay WAL to:  $TARGET (from serverName \"pg\")" || fail "plan: PITR line"
echo "$PLAN" | grep -q 'WAL archive:    plugin, serverName "pg" -> "pg-rewind1"' || fail "plan: serverName bump"

log "run: point-in-time rewind to $TARGET"
OUT=$($R run -n "$NS" --to "$TARGET" --check-sql "select count(*) from t" --yes pg)
echo "$OUT"
echo "$OUT" | grep -q "check query on pg-rewind-check: 2" || fail "rehearsal check"
echo "$OUT" | grep -q "check query on pg: 2" || fail "final check"

log "verify"
[[ "$(psql pg 'select count(*) from t')" == 2 ]] || fail "rewound cluster does not have rows 1 and 2"
[[ "$(k -n "$NS" get cluster pg -o jsonpath='{.spec.plugins[0].parameters.serverName}')" == pg-rewind1 ]] || fail "serverName not bumped"
k -n "$NS" wait --for=condition=ContinuousArchiving cluster/pg --timeout=300s
psql pg "insert into t values (20);"
archive_now
backup after
SAFETY=$(k -n "$NS" get backups -l app.kubernetes.io/managed-by=cnpg-rewind -o jsonpath='{.items[0].metadata.name}')
[[ -n "$SAFETY" ]] || fail "no safety backup"
[[ "$(k -n "$NS" get backup "$SAFETY" -o 'jsonpath={.metadata.annotations.cnpg-rewind\.jwrede\.github\.io/server-name}')" == pg ]] || fail "safety backup annotation"

log "undo: rewind to the safety backup $SAFETY (taken under serverName pg)"
OUT=$($R run -n "$NS" --backup "$SAFETY" --rehearse=false --safety-backup=false --check-sql "select count(*) from t" --yes pg)
echo "$OUT"
echo "$OUT" | grep -q 'WAL archive:    plugin, serverName "pg-rewind1" -> "pg-rewind2"' || fail "undo: serverName bump"
echo "$OUT" | grep -q 'the archive "pg" stays untouched and is the source of this rewind' || fail "undo: source archive"
[[ "$(psql pg 'select count(*) from t')" == 3 ]] || fail "undo did not bring back 3 rows"
k -n "$NS" wait --for=condition=ContinuousArchiving cluster/pg --timeout=300s
k -n "$NS" get clusters,backups

log "PASS"
