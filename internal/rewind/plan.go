// Package rewind builds and executes an in-place recovery of a
// CloudNativePG Cluster: the Cluster keeps its name, services and
// application secret, and its data is replaced by a backup.
package rewind

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	// BarmanPlugin is the name of the Barman Cloud CNPG-I plugin.
	BarmanPlugin = "barman-cloud.cloudnative-pg.io"
	// OriginName is the externalClusters entry that points at the
	// cluster's own backups.
	OriginName = "cnpg-rewind-origin"

	annoPrefix = "cnpg-rewind.jwrede.github.io/"
	// AnnoBackup records the backup the cluster was rewound to.
	AnnoBackup = annoPrefix + "backup"
	// AnnoTarget records the recovery target time, if any.
	AnnoTarget = annoPrefix + "target-time"
	// AnnoAt records when the rewind started.
	AnnoAt = annoPrefix + "rewound-at"
	// AnnoServerName records, on a safety backup, the serverName of the
	// WAL archive the backup belongs to.
	AnnoServerName = annoPrefix + "server-name"
)

// Options select what the cluster is rewound to.
type Options struct {
	// Backup is the name of a completed Backup of the cluster. When empty,
	// the newest completed backup that ended before TargetTime (or the
	// newest completed backup at all) is used.
	Backup string
	// TargetTime requests point-in-time recovery. It needs a WAL archive.
	TargetTime time.Time
	// Now is used for annotations and generated names.
	Now time.Time
}

// ObjRef names an object whose owner reference to the cluster is removed
// before the cluster is deleted, so that it survives the deletion.
type ObjRef struct {
	Kind, Name string
}

// Plan is everything the run needs; it is computed without side effects.
type Plan struct {
	Namespace, Name string
	ClusterUID      string

	BackupName   string
	BackupMethod string
	BackupStop   string
	TargetTime   string

	// Archive is "barmanObjectStore", "plugin" or "" (no WAL archive).
	Archive       string
	OldServerName string
	NewServerName string
	// SourceServerName is the serverName the chosen backup was taken under;
	// it differs from OldServerName when the backup predates an earlier
	// rewind.
	SourceServerName string

	Database, Owner, AppSecret string

	// SafetyBackup is the Backup taken of the current state before the
	// cluster is deleted.
	SafetyBackup *unstructured.Unstructured
	// Rehearsal is a throwaway cluster restored from the same source.
	Rehearsal *unstructured.Unstructured
	// NewCluster replaces the existing cluster.
	NewCluster *unstructured.Unstructured
	// Detach lists objects whose owner reference to the cluster is removed.
	Detach []ObjRef
	Notes  []string
}

// Build computes the plan from the live Cluster and the namespace's Backups.
// snapshotsOwned maps VolumeSnapshot names to true when they carry an owner
// reference to the cluster.
func Build(cluster *unstructured.Unstructured, backups []unstructured.Unstructured, appSecretOwned bool, snapshotsOwned map[string]bool, opts Options) (*Plan, error) {
	name := cluster.GetName()
	p := &Plan{Namespace: cluster.GetNamespace(), Name: name, ClusterUID: string(cluster.GetUID())}
	spec, _, _ := unstructured.NestedMap(cluster.Object, "spec")
	if spec == nil {
		return nil, fmt.Errorf("cluster %s has no spec", name)
	}
	if _, ok := spec["replica"]; ok {
		if enabled, _, _ := unstructured.NestedBool(spec, "replica", "enabled"); enabled {
			return nil, fmt.Errorf("cluster %s is a replica cluster; rewind its primary cluster instead", name)
		}
	}

	if err := p.detectArchive(spec, name); err != nil {
		return nil, err
	}
	if !opts.TargetTime.IsZero() {
		if p.Archive == "" {
			return nil, fmt.Errorf("point-in-time recovery needs a WAL archive, but cluster %s has neither spec.backup.barmanObjectStore nor the %s plugin as WAL archiver", name, BarmanPlugin)
		}
		p.TargetTime = opts.TargetTime.UTC().Format(time.RFC3339)
	}

	b, err := chooseBackup(name, backups, opts)
	if err != nil {
		return nil, err
	}
	p.BackupName = b.GetName()
	p.BackupMethod, _, _ = unstructured.NestedString(b.Object, "status", "method")
	if p.BackupMethod == "" {
		p.BackupMethod, _, _ = unstructured.NestedString(b.Object, "spec", "method")
	}
	p.BackupStop, _, _ = unstructured.NestedString(b.Object, "status", "stoppedAt")
	if p.Archive != "" {
		p.SourceServerName, _, _ = unstructured.NestedString(b.Object, "status", "serverName")
		if p.SourceServerName == "" {
			p.SourceServerName = b.GetAnnotations()[AnnoServerName]
		}
		if p.SourceServerName == "" {
			p.SourceServerName = p.OldServerName
		}
	}
	if p.BackupMethod != "volumeSnapshot" && p.Archive == "" {
		return nil, fmt.Errorf("backup %s uses method %q, but the cluster has no object store configured to restore it from", p.BackupName, p.BackupMethod)
	}

	p.Database, p.Owner, p.AppSecret = appIdentity(spec, name)

	recovery, err := p.recovery(b, true)
	if err != nil {
		return nil, err
	}
	p.NewCluster = p.newCluster(cluster, spec, recovery, opts)

	rehearsalRecovery, _ := p.recovery(b, false)
	p.Rehearsal = p.rehearsal(cluster, spec, rehearsalRecovery)
	p.SafetyBackup = p.safetyBackup(spec, opts.Now)

	if appSecretOwned {
		p.Detach = append(p.Detach, ObjRef{"Secret", p.AppSecret})
	}
	for i := range backups {
		bk := &backups[i]
		if c, _, _ := unstructured.NestedString(bk.Object, "spec", "cluster", "name"); c != name {
			continue
		}
		if ownedBy(bk, p.ClusterUID) {
			p.Detach = append(p.Detach, ObjRef{"Backup", bk.GetName()})
		}
		for _, s := range snapshotNames(bk) {
			if snapshotsOwned[s] {
				p.Detach = append(p.Detach, ObjRef{"VolumeSnapshot", s})
			}
		}
	}

	if p.Archive != "" {
		p.Notes = append(p.Notes, fmt.Sprintf("WAL archive and new base backups go to serverName %q; the archive %q stays untouched and is the source of this rewind", p.NewServerName, p.SourceServerName))
		if p.SourceServerName != p.OldServerName && p.TargetTime != "" {
			p.Notes = append(p.Notes, fmt.Sprintf("backup %s belongs to serverName %q from before an earlier rewind; WAL replay can only reach times before that rewind", p.BackupName, p.SourceServerName))
		}
	}
	p.Notes = append(p.Notes,
		"the Cluster object is deleted and recreated: pods, PVCs, services and operator-generated certificates are recreated, the application secret "+p.AppSecret+" is kept",
		"GitOps tools (Argo CD, Flux) may recreate the old Cluster while it is gone; suspend their sync for this Cluster first and commit the printed manifest afterwards")
	return p, nil
}

func (p *Plan) detectArchive(spec map[string]any, name string) error {
	if bos, ok, _ := unstructured.NestedMap(spec, "backup", "barmanObjectStore"); ok && bos != nil {
		p.Archive = "barmanObjectStore"
		p.OldServerName, _, _ = unstructured.NestedString(spec, "backup", "barmanObjectStore", "serverName")
	} else if i := barmanPluginIndex(spec); i >= 0 {
		p.Archive = "plugin"
		plugins, _, _ := unstructured.NestedSlice(spec, "plugins")
		params, _, _ := unstructured.NestedStringMap(plugins[i].(map[string]any), "parameters")
		if params["barmanObjectName"] == "" {
			return fmt.Errorf("plugin %s on cluster %s has no barmanObjectName parameter", BarmanPlugin, name)
		}
		p.OldServerName = params["serverName"]
	}
	if p.Archive != "" {
		if p.OldServerName == "" {
			p.OldServerName = name
		}
		p.NewServerName = NextServerName(p.OldServerName)
	}
	return nil
}

func barmanPluginIndex(spec map[string]any) int {
	plugins, _, _ := unstructured.NestedSlice(spec, "plugins")
	for i, raw := range plugins {
		m, ok := raw.(map[string]any)
		if !ok || m["name"] != BarmanPlugin {
			continue
		}
		if enabled, found, _ := unstructured.NestedBool(m, "enabled"); found && !enabled {
			continue
		}
		return i
	}
	return -1
}

var rewindSuffix = regexp.MustCompile(`^(.*)-rewind(\d+)$`)

// NextServerName returns the serverName for the WAL archive after a rewind:
// "pg" becomes "pg-rewind1", "pg-rewind1" becomes "pg-rewind2".
func NextServerName(old string) string {
	if m := rewindSuffix.FindStringSubmatch(old); m != nil {
		n, _ := strconv.Atoi(m[2])
		return fmt.Sprintf("%s-rewind%d", m[1], n+1)
	}
	return old + "-rewind1"
}

func chooseBackup(cluster string, backups []unstructured.Unstructured, opts Options) (*unstructured.Unstructured, error) {
	var candidates []*unstructured.Unstructured
	for i := range backups {
		b := &backups[i]
		if c, _, _ := unstructured.NestedString(b.Object, "spec", "cluster", "name"); c != cluster {
			continue
		}
		if opts.Backup != "" && b.GetName() == opts.Backup {
			if phase, _, _ := unstructured.NestedString(b.Object, "status", "phase"); phase != "completed" {
				return nil, fmt.Errorf("backup %s is %q, not completed", opts.Backup, phase)
			}
			if !opts.TargetTime.IsZero() {
				stop, err := stoppedAt(b)
				if err != nil {
					return nil, err
				}
				if stop.After(opts.TargetTime) {
					return nil, fmt.Errorf("backup %s ended at %s, after the target time %s", opts.Backup, stop.Format(time.RFC3339), opts.TargetTime.UTC().Format(time.RFC3339))
				}
			}
			return b, nil
		}
		if phase, _, _ := unstructured.NestedString(b.Object, "status", "phase"); phase == "completed" {
			candidates = append(candidates, b)
		}
	}
	if opts.Backup != "" {
		return nil, fmt.Errorf("backup %s of cluster %s not found", opts.Backup, cluster)
	}
	type cand struct {
		b    *unstructured.Unstructured
		stop time.Time
	}
	var cs []cand
	for _, b := range candidates {
		stop, err := stoppedAt(b)
		if err != nil {
			continue
		}
		if !opts.TargetTime.IsZero() && stop.After(opts.TargetTime) {
			continue
		}
		cs = append(cs, cand{b, stop})
	}
	if len(cs) == 0 {
		if opts.TargetTime.IsZero() {
			return nil, fmt.Errorf("cluster %s has no completed backup", cluster)
		}
		return nil, fmt.Errorf("cluster %s has no completed backup that ended before %s", cluster, opts.TargetTime.UTC().Format(time.RFC3339))
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].stop.After(cs[j].stop) })
	return cs[0].b, nil
}

func stoppedAt(b *unstructured.Unstructured) (time.Time, error) {
	s, _, _ := unstructured.NestedString(b.Object, "status", "stoppedAt")
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("backup %s has no valid status.stoppedAt (%q)", b.GetName(), s)
	}
	return t, nil
}

// appIdentity returns the application database, its owner and the secret
// holding the owner's credentials, following the cluster's bootstrap.
func appIdentity(spec map[string]any, name string) (db, owner, secret string) {
	db, owner = "app", ""
	for _, method := range []string{"initdb", "recovery", "pg_basebackup"} {
		m, ok, _ := unstructured.NestedMap(spec, "bootstrap", method)
		if !ok {
			continue
		}
		if v, _ := m["database"].(string); v != "" {
			db = v
		}
		if v, _ := m["owner"].(string); v != "" {
			owner = v
		}
		if v, _, _ := unstructured.NestedString(m, "secret", "name"); v != "" {
			secret = v
		}
		break
	}
	if owner == "" {
		owner = db
	}
	if secret == "" {
		secret = name + "-app"
	}
	return db, owner, secret
}

func snapshotNames(b *unstructured.Unstructured) []string {
	elems, _, _ := unstructured.NestedSlice(b.Object, "status", "snapshotBackupStatus", "elements")
	var out []string
	for _, e := range elems {
		if m, ok := e.(map[string]any); ok {
			if n, _ := m["name"].(string); n != "" {
				out = append(out, n)
			}
		}
	}
	return out
}

func ownedBy(o *unstructured.Unstructured, uid string) bool {
	for _, r := range o.GetOwnerReferences() {
		if string(r.UID) == uid {
			return true
		}
	}
	return false
}

// recovery returns spec.bootstrap.recovery. withSecret keeps the
// application database, owner and secret (used for the real cluster; the
// rehearsal gets its own generated secret).
func (p *Plan) recovery(b *unstructured.Unstructured, withSecret bool) (map[string]any, error) {
	r := map[string]any{}
	if p.BackupMethod == "volumeSnapshot" {
		vs := map[string]any{}
		elems, _, _ := unstructured.NestedSlice(b.Object, "status", "snapshotBackupStatus", "elements")
		tablespaces := map[string]any{}
		for _, e := range elems {
			m, _ := e.(map[string]any)
			n, _ := m["name"].(string)
			ref := map[string]any{"name": n, "kind": "VolumeSnapshot", "apiGroup": "snapshot.storage.k8s.io"}
			switch m["type"] {
			case "PG_DATA":
				vs["storage"] = ref
			case "PG_WAL":
				vs["walStorage"] = ref
			case "PG_TABLESPACE":
				if ts, _ := m["tablespaceName"].(string); ts != "" {
					tablespaces[ts] = ref
				}
			}
		}
		if vs["storage"] == nil {
			return nil, fmt.Errorf("backup %s has no PG_DATA snapshot in status.snapshotBackupStatus", b.GetName())
		}
		if len(tablespaces) > 0 {
			vs["tablespaceStorage"] = tablespaces
		}
		r["volumeSnapshots"] = vs
		if p.TargetTime != "" {
			r["source"] = OriginName
			r["recoveryTarget"] = map[string]any{"targetTime": p.TargetTime}
		}
	} else {
		r["source"] = OriginName
		target := map[string]any{}
		if id, _, _ := unstructured.NestedString(b.Object, "status", "backupId"); id != "" {
			target["backupID"] = id
		}
		if p.TargetTime != "" {
			target["targetTime"] = p.TargetTime
		}
		if len(target) > 0 {
			r["recoveryTarget"] = target
		}
	}
	if withSecret {
		r["database"] = p.Database
		r["owner"] = p.Owner
		r["secret"] = map[string]any{"name": p.AppSecret}
	}
	return r, nil
}

func (p *Plan) originCluster(spec map[string]any) map[string]any {
	switch p.Archive {
	case "barmanObjectStore":
		bos, _, _ := unstructured.NestedMap(spec, "backup", "barmanObjectStore")
		bos = deepCopyMap(bos)
		bos["serverName"] = p.SourceServerName
		// Archive-side settings do not apply to a recovery source.
		delete(bos, "data")
		delete(bos, "wal")
		delete(bos, "tags")
		delete(bos, "historyTags")
		return map[string]any{"name": OriginName, "barmanObjectStore": bos}
	case "plugin":
		plugins, _, _ := unstructured.NestedSlice(spec, "plugins")
		params, _, _ := unstructured.NestedStringMap(plugins[barmanPluginIndex(spec)].(map[string]any), "parameters")
		return map[string]any{"name": OriginName, "plugin": map[string]any{
			"name": BarmanPlugin,
			"parameters": map[string]any{
				"barmanObjectName": params["barmanObjectName"],
				"serverName":       p.SourceServerName,
			},
		}}
	}
	return nil
}

func withOrigin(spec map[string]any, origin map[string]any) {
	if origin == nil {
		return
	}
	ext, _, _ := unstructured.NestedSlice(spec, "externalClusters")
	var out []any
	for _, e := range ext {
		if m, ok := e.(map[string]any); ok && m["name"] == OriginName {
			continue
		}
		out = append(out, e)
	}
	spec["externalClusters"] = append(out, origin)
}

func (p *Plan) newCluster(cluster *unstructured.Unstructured, liveSpec, recovery map[string]any, opts Options) *unstructured.Unstructured {
	spec := deepCopyMap(liveSpec)
	spec["bootstrap"] = map[string]any{"recovery": recovery}
	withOrigin(spec, p.originCluster(liveSpec))
	switch p.Archive {
	case "barmanObjectStore":
		_ = unstructured.SetNestedField(spec, p.NewServerName, "backup", "barmanObjectStore", "serverName")
	case "plugin":
		plugins, _, _ := unstructured.NestedSlice(spec, "plugins")
		i := barmanPluginIndex(spec)
		m := plugins[i].(map[string]any)
		params, _, _ := unstructured.NestedMap(m, "parameters")
		if params == nil {
			params = map[string]any{}
		}
		params["serverName"] = p.NewServerName
		m["parameters"] = params
		plugins[i] = m
		spec["plugins"] = plugins
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": cluster.GetAPIVersion(),
		"kind":       "Cluster",
		"metadata":   map[string]any{"name": p.Name, "namespace": p.Namespace},
		"spec":       spec,
	}}
	if l := cluster.GetLabels(); len(l) > 0 {
		u.SetLabels(l)
	}
	annos := map[string]string{}
	for k, v := range cluster.GetAnnotations() {
		if k == "kubectl.kubernetes.io/last-applied-configuration" || strings.HasPrefix(k, annoPrefix) {
			continue
		}
		annos[k] = v
	}
	annos[AnnoBackup] = p.BackupName
	annos[AnnoAt] = opts.Now.UTC().Format(time.RFC3339)
	if p.TargetTime != "" {
		annos[AnnoTarget] = p.TargetTime
	}
	u.SetAnnotations(annos)
	return u
}

// RehearsalName is the name of the throwaway cluster.
func RehearsalName(cluster string) string { return cluster + "-rewind-check" }

func (p *Plan) rehearsal(cluster *unstructured.Unstructured, liveSpec, recovery map[string]any) *unstructured.Unstructured {
	spec := deepCopyMap(liveSpec)
	spec["instances"] = int64(1)
	spec["bootstrap"] = map[string]any{"recovery": recovery}
	for _, k := range []string{"backup", "plugins", "managed", "replica", "externalClusters", "monitoring", "superuserSecret"} {
		delete(spec, k)
	}
	withOrigin(spec, p.originCluster(liveSpec))
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": cluster.GetAPIVersion(),
		"kind":       "Cluster",
		"metadata": map[string]any{
			"name": RehearsalName(p.Name), "namespace": p.Namespace,
			"labels": map[string]any{"app.kubernetes.io/managed-by": "cnpg-rewind"},
		},
		"spec": spec,
	}}
}

func (p *Plan) safetyBackup(spec map[string]any, now time.Time) *unstructured.Unstructured {
	bs := map[string]any{"cluster": map[string]any{"name": p.Name}}
	switch {
	case hasKey(spec, "backup", "volumeSnapshot"):
		bs["method"] = "volumeSnapshot"
	case p.Archive == "barmanObjectStore":
		bs["method"] = "barmanObjectStore"
	case p.Archive == "plugin":
		bs["method"] = "plugin"
		bs["pluginConfiguration"] = map[string]any{"name": BarmanPlugin}
	default:
		return nil
	}
	meta := map[string]any{
		"name":      fmt.Sprintf("%s-before-rewind-%s", p.Name, now.UTC().Format("20060102150405")),
		"namespace": p.Namespace,
		"labels":    map[string]any{"app.kubernetes.io/managed-by": "cnpg-rewind"},
	}
	if p.Archive != "" {
		meta["annotations"] = map[string]any{AnnoServerName: p.OldServerName}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "postgresql.cnpg.io/v1",
		"kind":       "Backup",
		"metadata":   meta,
		"spec":       bs,
	}}
}

func hasKey(m map[string]any, path ...string) bool {
	v, ok, _ := unstructured.NestedFieldNoCopy(m, path...)
	return ok && v != nil
}

func deepCopyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	return (&unstructured.Unstructured{Object: m}).DeepCopy().Object
}
