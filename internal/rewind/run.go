package rewind

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/yaml"
)

var (
	ClusterGVR  = schema.GroupVersionResource{Group: "postgresql.cnpg.io", Version: "v1", Resource: "clusters"}
	BackupGVR   = schema.GroupVersionResource{Group: "postgresql.cnpg.io", Version: "v1", Resource: "backups"}
	SnapshotGVR = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}
)

// Client bundles the API clients the rewind needs.
type Client struct {
	Dyn  dynamic.Interface
	Kube kubernetes.Interface
	REST *rest.Config
	Out  io.Writer
	// Poll is the interval for all waits.
	Poll time.Duration
}

func (c *Client) logf(format string, a ...any) { fmt.Fprintf(c.Out, format+"\n", a...) }

// Load reads the Cluster, its Backups and ownership data and builds the plan.
func (c *Client) Load(ctx context.Context, ns, name string, opts Options) (*Plan, error) {
	cluster, err := c.Dyn.Resource(ClusterGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get cluster: %w", err)
	}
	bl, err := c.Dyn.Resource(BackupGVR).Namespace(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list backups: %w", err)
	}
	uid := string(cluster.GetUID())
	owned := map[string]bool{}
	if sl, err := c.Dyn.Resource(SnapshotGVR).Namespace(ns).List(ctx, metav1.ListOptions{}); err == nil {
		for i := range sl.Items {
			if ownedBy(&sl.Items[i], uid) {
				owned[sl.Items[i].GetName()] = true
			}
		}
	}
	_, _, secret := appIdentity(cluster.Object["spec"].(map[string]any), name)
	secretOwned := false
	if s, err := c.Kube.CoreV1().Secrets(ns).Get(ctx, secret, metav1.GetOptions{}); err == nil {
		for _, r := range s.OwnerReferences {
			if string(r.UID) == uid {
				secretOwned = true
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get secret %s: %w", secret, err)
	}
	return Build(cluster, bl.Items, secretOwned, owned, opts)
}

// Print writes a human-readable summary of the plan for the given run options.
func (p *Plan) Print(w io.Writer, ro RunOptions) {
	fmt.Fprintf(w, "Cluster %s/%s\n", p.Namespace, p.Name)
	fmt.Fprintf(w, "  restore from:   backup %s (method %s, ended %s)\n", p.BackupName, p.BackupMethod, p.BackupStop)
	if p.TargetTime != "" {
		fmt.Fprintf(w, "  replay WAL to:  %s (from serverName %q)\n", p.TargetTime, p.OldServerName)
	}
	fmt.Fprintf(w, "  keeps:          name, services, secret %s (database %s, owner %s)\n", p.AppSecret, p.Database, p.Owner)
	if p.Archive != "" {
		fmt.Fprintf(w, "  WAL archive:    %s, serverName %q -> %q\n", p.Archive, p.OldServerName, p.NewServerName)
	}
	switch {
	case !ro.SafetyBackup:
		fmt.Fprintf(w, "  safety backup:  skipped\n")
	case p.SafetyBackup != nil:
		fmt.Fprintf(w, "  safety backup:  %s (method %v)\n", p.SafetyBackup.GetName(), p.SafetyBackup.Object["spec"].(map[string]any)["method"])
	default:
		fmt.Fprintf(w, "  safety backup:  none possible (no backup method configured)\n")
	}
	if ro.Rehearse {
		fmt.Fprintf(w, "  rehearsal:      cluster %s, deleted afterwards\n", RehearsalName(p.Name))
	} else {
		fmt.Fprintf(w, "  rehearsal:      skipped\n")
	}
	for _, d := range p.Detach {
		fmt.Fprintf(w, "  detach:         %s %s (owned by the cluster, would be deleted with it)\n", d.Kind, d.Name)
	}
	for _, n := range p.Notes {
		fmt.Fprintf(w, "  note:           %s\n", n)
	}
}

// Manifest returns the replacement Cluster as YAML.
func (p *Plan) Manifest() ([]byte, error) { return yaml.Marshal(p.NewCluster.Object) }

// RunOptions control the destructive run.
type RunOptions struct {
	Rehearse     bool
	SafetyBackup bool
	CheckSQL     string
	Timeout      time.Duration
}

// Run executes the plan.
func (c *Client) Run(ctx context.Context, p *Plan, ro RunOptions) error {
	ns := p.Namespace
	if ro.Rehearse {
		c.logf("==> rehearsal: restoring %s from backup %s", RehearsalName(p.Name), p.BackupName)
		if err := c.rehearse(ctx, p, ro); err != nil {
			return fmt.Errorf("rehearsal failed, cluster %s was not touched: %w", p.Name, err)
		}
	}

	if ro.SafetyBackup {
		if p.SafetyBackup == nil {
			return fmt.Errorf("no backup method configured for a safety backup; rerun with -safety-backup=false to skip it")
		}
		c.logf("==> safety backup %s of the current state", p.SafetyBackup.GetName())
		if _, err := c.Dyn.Resource(BackupGVR).Namespace(ns).Create(ctx, p.SafetyBackup, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create safety backup: %w", err)
		}
		if err := c.waitBackup(ctx, ns, p.SafetyBackup.GetName(), ro.Timeout); err != nil {
			return fmt.Errorf("safety backup: %w (cluster %s was not touched)", err, p.Name)
		}
		// The safety backup and its snapshots must survive the deletion too.
		b, err := c.Dyn.Resource(BackupGVR).Namespace(ns).Get(ctx, p.SafetyBackup.GetName(), metav1.GetOptions{})
		if err != nil {
			return err
		}
		if ownedBy(b, p.ClusterUID) {
			p.Detach = append(p.Detach, ObjRef{"Backup", b.GetName()})
		}
		for _, s := range snapshotNames(b) {
			if sn, err := c.Dyn.Resource(SnapshotGVR).Namespace(ns).Get(ctx, s, metav1.GetOptions{}); err == nil && ownedBy(sn, p.ClusterUID) {
				p.Detach = append(p.Detach, ObjRef{"VolumeSnapshot", s})
			}
		}
	}

	for _, d := range p.Detach {
		c.logf("==> detach %s %s from the cluster", d.Kind, d.Name)
		if err := c.detach(ctx, ns, d, p.ClusterUID); err != nil {
			return fmt.Errorf("detach %s %s: %w (cluster %s was not touched)", d.Kind, d.Name, err, p.Name)
		}
	}

	c.logf("==> delete cluster %s", p.Name)
	bg := metav1.DeletePropagationBackground
	uid := types.UID(p.ClusterUID)
	if err := c.Dyn.Resource(ClusterGVR).Namespace(ns).Delete(ctx, p.Name, metav1.DeleteOptions{
		PropagationPolicy: &bg,
		Preconditions:     &metav1.Preconditions{UID: &uid},
	}); err != nil {
		return fmt.Errorf("delete cluster: %w", err)
	}
	if err := c.waitGone(ctx, ns, p.Name, ro.Timeout); err != nil {
		return err
	}

	c.logf("==> create cluster %s from backup %s", p.Name, p.BackupName)
	if _, err := c.Dyn.Resource(ClusterGVR).Namespace(ns).Create(ctx, p.NewCluster, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create cluster (the old one is deleted; apply the printed manifest by hand): %w", err)
	}
	if err := c.waitReady(ctx, ns, p.Name, ro.Timeout); err != nil {
		return err
	}
	if ro.CheckSQL != "" {
		out, err := c.query(ctx, ns, p.Name, p.Database, ro.CheckSQL)
		if err != nil {
			return fmt.Errorf("check query on %s: %w", p.Name, err)
		}
		c.logf("check query on %s: %s", p.Name, out)
	}
	c.logf("==> done: %s is ready, restored from backup %s", p.Name, p.BackupName)
	return nil
}

func (c *Client) rehearse(ctx context.Context, p *Plan, ro RunOptions) error {
	ns, name := p.Namespace, RehearsalName(p.Name)
	if _, err := c.Dyn.Resource(ClusterGVR).Namespace(ns).Create(ctx, p.Rehearsal, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	defer func() {
		c.logf("==> delete rehearsal cluster %s", name)
		bg := metav1.DeletePropagationBackground
		dctx, cancel := context.WithTimeout(context.Background(), ro.Timeout)
		defer cancel()
		if err := c.Dyn.Resource(ClusterGVR).Namespace(ns).Delete(dctx, name, metav1.DeleteOptions{PropagationPolicy: &bg}); err != nil && !apierrors.IsNotFound(err) {
			c.logf("warning: delete %s: %v", name, err)
			return
		}
		if err := c.waitGone(dctx, ns, name, ro.Timeout); err != nil {
			c.logf("warning: %v", err)
		}
	}()
	if err := c.waitReady(ctx, ns, name, ro.Timeout); err != nil {
		return err
	}
	if ro.CheckSQL != "" {
		out, err := c.query(ctx, ns, name, p.Database, ro.CheckSQL)
		if err != nil {
			return fmt.Errorf("check query: %w", err)
		}
		c.logf("check query on %s: %s", name, out)
	}
	return nil
}

func (c *Client) detach(ctx context.Context, ns string, d ObjRef, uid string) error {
	var refs []metav1.OwnerReference
	var get func() (metav1.Object, error)
	var patch func([]byte) error
	switch d.Kind {
	case "Secret":
		get = func() (metav1.Object, error) {
			return c.Kube.CoreV1().Secrets(ns).Get(ctx, d.Name, metav1.GetOptions{})
		}
		patch = func(b []byte) error {
			_, err := c.Kube.CoreV1().Secrets(ns).Patch(ctx, d.Name, types.MergePatchType, b, metav1.PatchOptions{})
			return err
		}
	default:
		gvr := BackupGVR
		if d.Kind == "VolumeSnapshot" {
			gvr = SnapshotGVR
		}
		get = func() (metav1.Object, error) {
			return c.Dyn.Resource(gvr).Namespace(ns).Get(ctx, d.Name, metav1.GetOptions{})
		}
		patch = func(b []byte) error {
			_, err := c.Dyn.Resource(gvr).Namespace(ns).Patch(ctx, d.Name, types.MergePatchType, b, metav1.PatchOptions{})
			return err
		}
	}
	o, err := get()
	if err != nil {
		return err
	}
	for _, r := range o.GetOwnerReferences() {
		if string(r.UID) != uid {
			refs = append(refs, r)
		}
	}
	// A JSON merge patch replaces the whole list; nil removes the field.
	b, _ := json.Marshal(map[string]any{"metadata": map[string]any{"ownerReferences": refs}})
	return patch(b)
}

func (c *Client) poll(ctx context.Context, timeout time.Duration, what string, done func() (bool, string, error)) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	last := ""
	for {
		ok, state, err := done()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if state != last && state != "" {
			c.logf("    %s: %s", what, state)
			last = state
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for %s (last state: %s)", what, last)
		case <-time.After(c.Poll):
		}
	}
}

func (c *Client) waitBackup(ctx context.Context, ns, name string, timeout time.Duration) error {
	return c.poll(ctx, timeout, "backup "+name, func() (bool, string, error) {
		b, err := c.Dyn.Resource(BackupGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, "", err
		}
		phase, _, _ := unstructured.NestedString(b.Object, "status", "phase")
		if phase == "failed" {
			msg, _, _ := unstructured.NestedString(b.Object, "status", "error")
			return false, phase, fmt.Errorf("backup %s failed: %s", name, msg)
		}
		return phase == "completed", phase, nil
	})
}

func (c *Client) waitReady(ctx context.Context, ns, name string, timeout time.Duration) error {
	return c.poll(ctx, timeout, "cluster "+name, func() (bool, string, error) {
		cl, err := c.Dyn.Resource(ClusterGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, "", err
		}
		phase, _, _ := unstructured.NestedString(cl.Object, "status", "phase")
		conds, _, _ := unstructured.NestedSlice(cl.Object, "status", "conditions")
		for _, raw := range conds {
			m, _ := raw.(map[string]any)
			if m["type"] == "Ready" && m["status"] == "True" {
				return true, phase, nil
			}
		}
		return false, phase, nil
	})
}

func (c *Client) waitGone(ctx context.Context, ns, name string, timeout time.Duration) error {
	sel := metav1.ListOptions{LabelSelector: "cnpg.io/cluster=" + name}
	return c.poll(ctx, timeout, "deletion of "+name, func() (bool, string, error) {
		_, err := c.Dyn.Resource(ClusterGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			return false, "cluster terminating", nil
		}
		if !apierrors.IsNotFound(err) {
			return false, "", err
		}
		pods, err := c.Kube.CoreV1().Pods(ns).List(ctx, sel)
		if err != nil {
			return false, "", err
		}
		pvcs, err := c.Kube.CoreV1().PersistentVolumeClaims(ns).List(ctx, sel)
		if err != nil {
			return false, "", err
		}
		if n := len(pods.Items) + len(pvcs.Items); n > 0 {
			return false, fmt.Sprintf("%d pods, %d PVCs left", len(pods.Items), len(pvcs.Items)), nil
		}
		return true, "", nil
	})
}

// query runs SQL with psql in the primary pod of the cluster.
func (c *Client) query(ctx context.Context, ns, cluster, db, sql string) (string, error) {
	pods, err := c.Kube.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "cnpg.io/cluster=" + cluster + ",cnpg.io/instanceRole=primary"})
	if err != nil {
		return "", err
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no primary pod for cluster %s", cluster)
	}
	req := c.Kube.CoreV1().RESTClient().Post().Resource("pods").Namespace(ns).Name(pods.Items[0].Name).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "postgres",
			Command:   []string{"psql", "-v", "ON_ERROR_STOP=1", "-At", "-d", db, "-c", sql},
			Stdout:    true, Stderr: true,
		}, scheme.ParameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(c.REST, "POST", req.URL())
	if err != nil {
		return "", err
	}
	var stdout, stderr bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}
