package rewind

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

func obj(t *testing.T, s string) *unstructured.Unstructured {
	t.Helper()
	u, err := decode(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// decode parses YAML the way the API client does (integers stay int64).
func decode(s string) (*unstructured.Unstructured, error) {
	j, err := yaml.YAMLToJSON([]byte(s))
	if err != nil {
		return nil, err
	}
	u := &unstructured.Unstructured{}
	return u, u.UnmarshalJSON(j)
}

const snapCluster = `
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: pg
  namespace: app
  uid: c-1
  annotations:
    kubectl.kubernetes.io/last-applied-configuration: "{}"
    team: data
spec:
  instances: 3
  storage: {size: 1Gi}
  walStorage: {size: 1Gi}
  bootstrap:
    initdb: {database: shop, owner: shopper}
  backup:
    volumeSnapshot: {className: csi}
status:
  phase: Cluster in healthy state
`

func snapBackup(name, stopped string, owned bool) unstructured.Unstructured {
	owner := ""
	if owned {
		owner = `
  ownerReferences:
  - {apiVersion: postgresql.cnpg.io/v1, kind: Cluster, name: pg, uid: c-1}`
	}
	u, _ := decode(`
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata:
  name: ` + name + owner + `
spec:
  method: volumeSnapshot
  cluster: {name: pg}
status:
  phase: completed
  method: volumeSnapshot
  stoppedAt: "` + stopped + `"
  snapshotBackupStatus:
    elements:
    - {name: ` + name + `, type: PG_DATA}
    - {name: ` + name + `-wal, type: PG_WAL}
`)
	return *u
}

var now = time.Date(2026, 11, 19, 6, 0, 0, 0, time.UTC)

func TestVolumeSnapshotRewindKeepsIdentity(t *testing.T) {
	backups := []unstructured.Unstructured{
		snapBackup("b1", "2026-11-18T01:00:00Z", false),
		snapBackup("b2", "2026-11-19T01:00:00Z", true),
	}
	p, err := Build(obj(t, snapCluster), backups, true, map[string]bool{"b2": true}, Options{Backup: "b1", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if p.BackupName != "b1" || p.Archive != "" || p.Database != "shop" || p.Owner != "shopper" || p.AppSecret != "pg-app" {
		t.Fatalf("plan: %+v", p)
	}
	rec, _, _ := unstructured.NestedMap(p.NewCluster.Object, "spec", "bootstrap", "recovery")
	if n, _, _ := unstructured.NestedString(rec, "volumeSnapshots", "storage", "name"); n != "b1" {
		t.Errorf("data snapshot %q", n)
	}
	if n, _, _ := unstructured.NestedString(rec, "volumeSnapshots", "walStorage", "name"); n != "b1-wal" {
		t.Errorf("wal snapshot %q", n)
	}
	if s, _, _ := unstructured.NestedString(rec, "secret", "name"); s != "pg-app" {
		t.Errorf("recovery secret %q", s)
	}
	if _, ok := rec["source"]; ok {
		t.Error("no source expected without PITR")
	}
	if _, ok, _ := unstructured.NestedMap(p.NewCluster.Object, "spec", "bootstrap", "initdb"); ok {
		t.Error("initdb must be replaced")
	}
	if i, _, _ := unstructured.NestedInt64(p.NewCluster.Object, "spec", "instances"); i != 3 {
		t.Errorf("instances %d", i)
	}
	if _, ok := p.NewCluster.Object["status"]; ok {
		t.Error("status copied")
	}
	a := p.NewCluster.GetAnnotations()
	if a["team"] != "data" || a[AnnoBackup] != "b1" || a["kubectl.kubernetes.io/last-applied-configuration"] != "" {
		t.Errorf("annotations %v", a)
	}
	if p.NewCluster.GetUID() != "" {
		t.Error("uid copied")
	}

	want := []ObjRef{{"Secret", "pg-app"}, {"Backup", "b2"}, {"VolumeSnapshot", "b2"}}
	if len(p.Detach) != len(want) {
		t.Fatalf("detach %v", p.Detach)
	}
	for i := range want {
		if p.Detach[i] != want[i] {
			t.Errorf("detach[%d] = %v, want %v", i, p.Detach[i], want[i])
		}
	}

	// The rehearsal is a single instance with its own secret and no backups.
	r := p.Rehearsal
	if r.GetName() != "pg-rewind-check" {
		t.Errorf("rehearsal name %s", r.GetName())
	}
	if i, _, _ := unstructured.NestedInt64(r.Object, "spec", "instances"); i != 1 {
		t.Errorf("rehearsal instances %d", i)
	}
	if _, ok, _ := unstructured.NestedMap(r.Object, "spec", "backup"); ok {
		t.Error("rehearsal must not back up")
	}
	if _, ok, _ := unstructured.NestedMap(r.Object, "spec", "bootstrap", "recovery", "secret"); ok {
		t.Error("rehearsal must not take over the app secret")
	}

	if m, _, _ := unstructured.NestedString(p.SafetyBackup.Object, "spec", "method"); m != "volumeSnapshot" {
		t.Errorf("safety backup method %q", m)
	}
	if p.SafetyBackup.GetName() != "pg-before-rewind-20261119060000" {
		t.Errorf("safety backup name %s", p.SafetyBackup.GetName())
	}
}

func TestNewestBackupIsDefault(t *testing.T) {
	backups := []unstructured.Unstructured{
		snapBackup("b1", "2026-11-18T01:00:00Z", false),
		snapBackup("b3", "2026-11-19T03:00:00Z", false),
		snapBackup("b2", "2026-11-19T01:00:00Z", false),
	}
	failed := snapBackup("b4", "2026-11-19T05:00:00Z", false)
	_ = unstructured.SetNestedField(failed.Object, "failed", "status", "phase")
	other := snapBackup("x", "2026-11-19T05:30:00Z", false)
	_ = unstructured.SetNestedField(other.Object, "other", "spec", "cluster", "name")
	backups = append(backups, failed, other)
	p, err := Build(obj(t, snapCluster), backups, false, nil, Options{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if p.BackupName != "b3" {
		t.Fatalf("chose %s, want b3", p.BackupName)
	}
	if _, err := Build(obj(t, snapCluster), backups, false, nil, Options{Backup: "b4", Now: now}); err == nil || !strings.Contains(err.Error(), "not completed") {
		t.Errorf("failed backup accepted: %v", err)
	}
	if _, err := Build(obj(t, snapCluster), backups, false, nil, Options{Backup: "x", Now: now}); err == nil {
		t.Error("backup of another cluster accepted")
	}
}

func TestPITRNeedsArchive(t *testing.T) {
	backups := []unstructured.Unstructured{snapBackup("b1", "2026-11-18T01:00:00Z", false)}
	_, err := Build(obj(t, snapCluster), backups, false, nil, Options{TargetTime: now, Now: now})
	if err == nil || !strings.Contains(err.Error(), "WAL archive") {
		t.Fatalf("got %v", err)
	}
}

const pluginCluster = `
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: pg, namespace: app, uid: c-1}
spec:
  instances: 2
  storage: {size: 1Gi}
  plugins:
  - name: barman-cloud.cloudnative-pg.io
    isWALArchiver: true
    parameters: {barmanObjectName: store}
  externalClusters:
  - name: other
    connectionParameters: {host: x}
`

func objectBackup(name, stopped, id string) unstructured.Unstructured {
	u, _ := decode(`
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata: {name: ` + name + `}
spec: {method: plugin, cluster: {name: pg}}
status: {phase: completed, method: plugin, stoppedAt: "` + stopped + `", backupId: "` + id + `"}
`)
	return *u
}

func TestPluginPITR(t *testing.T) {
	backups := []unstructured.Unstructured{
		objectBackup("b1", "2026-11-18T01:00:00Z", "20261118T010000"),
		objectBackup("b2", "2026-11-19T05:00:00Z", "20261119T050000"),
	}
	target := time.Date(2026, 11, 19, 4, 0, 0, 0, time.UTC)
	p, err := Build(obj(t, pluginCluster), backups, false, nil, Options{TargetTime: target, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if p.BackupName != "b1" {
		t.Errorf("chose %s, want b1 (b2 ended after the target)", p.BackupName)
	}
	if p.OldServerName != "pg" || p.NewServerName != "pg-rewind1" {
		t.Errorf("server names %q -> %q", p.OldServerName, p.NewServerName)
	}
	rec, _, _ := unstructured.NestedMap(p.NewCluster.Object, "spec", "bootstrap", "recovery")
	if rec["source"] != OriginName {
		t.Errorf("source %v", rec["source"])
	}
	if id, _, _ := unstructured.NestedString(rec, "recoveryTarget", "backupID"); id != "20261118T010000" {
		t.Errorf("backupID %q", id)
	}
	if tt, _, _ := unstructured.NestedString(rec, "recoveryTarget", "targetTime"); tt != "2026-11-19T04:00:00Z" {
		t.Errorf("targetTime %q", tt)
	}
	ext, _, _ := unstructured.NestedSlice(p.NewCluster.Object, "spec", "externalClusters")
	if len(ext) != 2 {
		t.Fatalf("externalClusters %v", ext)
	}
	origin := ext[1].(map[string]any)
	if sn, _, _ := unstructured.NestedString(origin, "plugin", "parameters", "serverName"); sn != "pg" {
		t.Errorf("origin serverName %q", sn)
	}
	if bn, _, _ := unstructured.NestedString(origin, "plugin", "parameters", "barmanObjectName"); bn != "store" {
		t.Errorf("origin barmanObjectName %q", bn)
	}
	plugins, _, _ := unstructured.NestedSlice(p.NewCluster.Object, "spec", "plugins")
	if sn, _, _ := unstructured.NestedString(plugins[0].(map[string]any), "parameters", "serverName"); sn != "pg-rewind1" {
		t.Errorf("new archive serverName %q", sn)
	}
	// The live object must not be modified.
	live := obj(t, pluginCluster)
	if _, err := Build(live, backups, false, nil, Options{Now: now}); err != nil {
		t.Fatal(err)
	}
	lp, _, _ := unstructured.NestedSlice(live.Object, "spec", "plugins")
	if _, ok, _ := unstructured.NestedString(lp[0].(map[string]any), "parameters", "serverName"); ok {
		t.Error("Build modified the live cluster")
	}
	if m, _, _ := unstructured.NestedString(p.SafetyBackup.Object, "spec", "method"); m != "plugin" {
		t.Errorf("safety backup method %q", m)
	}
	if _, err := Build(obj(t, pluginCluster), backups, false, nil, Options{Backup: "b2", TargetTime: target, Now: now}); err == nil {
		t.Error("backup after the target time accepted")
	}
}

func TestBarmanObjectStore(t *testing.T) {
	c := obj(t, `
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: pg, namespace: app, uid: c-1}
spec:
  instances: 1
  storage: {size: 1Gi}
  bootstrap:
    recovery: {source: old, database: db1, owner: o1, secret: {name: creds}}
  backup:
    barmanObjectStore:
      destinationPath: s3://bucket/pg
      serverName: pg-rewind1
      wal: {compression: gzip}
`)
	b := objectBackup("b1", "2026-11-18T01:00:00Z", "ID1")
	_ = unstructured.SetNestedField(b.Object, "barmanObjectStore", "status", "method")
	p, err := Build(c, []unstructured.Unstructured{b}, false, nil, Options{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if p.Database != "db1" || p.Owner != "o1" || p.AppSecret != "creds" {
		t.Errorf("identity %s %s %s", p.Database, p.Owner, p.AppSecret)
	}
	if p.NewServerName != "pg-rewind2" {
		t.Errorf("new server name %q", p.NewServerName)
	}
	if sn, _, _ := unstructured.NestedString(p.NewCluster.Object, "spec", "backup", "barmanObjectStore", "serverName"); sn != "pg-rewind2" {
		t.Errorf("archive serverName %q", sn)
	}
	ext, _, _ := unstructured.NestedSlice(p.NewCluster.Object, "spec", "externalClusters")
	origin := ext[0].(map[string]any)
	if sn, _, _ := unstructured.NestedString(origin, "barmanObjectStore", "serverName"); sn != "pg-rewind1" {
		t.Errorf("origin serverName %q", sn)
	}
	if _, ok, _ := unstructured.NestedMap(origin, "barmanObjectStore", "wal"); ok {
		t.Error("archive settings copied into the recovery source")
	}
}

func TestSourceServerNameAfterEarlierRewind(t *testing.T) {
	c := obj(t, pluginCluster)
	plugins, _, _ := unstructured.NestedSlice(c.Object, "spec", "plugins")
	_ = unstructured.SetNestedField(plugins[0].(map[string]any), "pg-rewind1", "parameters", "serverName")
	_ = unstructured.SetNestedSlice(c.Object, plugins, "spec", "plugins")
	safety := objectBackup("pg-before-rewind-1", "2026-11-18T01:00:00Z", "ID0")
	safety.SetAnnotations(map[string]string{AnnoServerName: "pg"})
	fromStatus := objectBackup("b-status", "2026-11-18T02:00:00Z", "ID2")
	_ = unstructured.SetNestedField(fromStatus.Object, "pg-old", "status", "serverName")
	recent := objectBackup("b-recent", "2026-11-19T02:00:00Z", "ID3")
	backups := []unstructured.Unstructured{safety, fromStatus, recent}
	for backup, want := range map[string]string{"pg-before-rewind-1": "pg", "b-status": "pg-old", "b-recent": "pg-rewind1"} {
		p, err := Build(c, backups, false, nil, Options{Backup: backup, Now: now})
		if err != nil {
			t.Fatal(err)
		}
		if p.SourceServerName != want || p.NewServerName != "pg-rewind2" {
			t.Errorf("%s: source %q new %q", backup, p.SourceServerName, p.NewServerName)
		}
		ext, _, _ := unstructured.NestedSlice(p.NewCluster.Object, "spec", "externalClusters")
		if sn, _, _ := unstructured.NestedString(ext[len(ext)-1].(map[string]any), "plugin", "parameters", "serverName"); sn != want {
			t.Errorf("%s: origin serverName %q", backup, sn)
		}
		if a := p.SafetyBackup.GetAnnotations()[AnnoServerName]; a != "pg-rewind1" {
			t.Errorf("safety backup annotation %q", a)
		}
	}
}

func TestNextServerName(t *testing.T) {
	for in, want := range map[string]string{"pg": "pg-rewind1", "pg-rewind1": "pg-rewind2", "pg-rewind9": "pg-rewind10", "a-rewind": "a-rewind-rewind1"} {
		if got := NextServerName(in); got != want {
			t.Errorf("NextServerName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestReplicaRefused(t *testing.T) {
	c := obj(t, snapCluster)
	_ = unstructured.SetNestedField(c.Object, true, "spec", "replica", "enabled")
	if _, err := Build(c, nil, false, nil, Options{Now: now}); err == nil {
		t.Error("replica cluster accepted")
	}
}
