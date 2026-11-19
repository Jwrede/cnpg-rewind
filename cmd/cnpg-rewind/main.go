// Command cnpg-rewind restores a CloudNativePG Cluster in place from one of
// its own backups: same name, same services, same application secret.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/Jwrede/cnpg-rewind/internal/rewind"
)

const usage = `cnpg-rewind restores a CloudNativePG Cluster in place from one of its backups.

Usage:
  cnpg-rewind plan [flags] CLUSTER   show what would happen and print the new Cluster manifest
  cnpg-rewind run  [flags] CLUSTER   rehearse, take a safety backup, then delete and recreate CLUSTER

Flags:
`

func main() {
	if len(os.Args) < 2 || (os.Args[1] != "plan" && os.Args[1] != "run") {
		fmt.Fprint(os.Stderr, usage)
		newFlags("plan").fs.PrintDefaults()
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type cfg struct {
	fs                        *flag.FlagSet
	ns, kubeconfig, kcontext  string
	backup, to, checkSQL, out string
	rehearse, safety, yes     bool
	timeout                   time.Duration
}

func newFlags(cmd string) *cfg {
	c := &cfg{fs: flag.NewFlagSet(cmd, flag.ExitOnError)}
	f := c.fs
	f.StringVar(&c.ns, "n", "", "namespace (default: the kubeconfig context's namespace)")
	f.StringVar(&c.kubeconfig, "kubeconfig", "", "path to the kubeconfig (default: $KUBECONFIG or ~/.kube/config)")
	f.StringVar(&c.kcontext, "context", "", "kubeconfig context")
	f.StringVar(&c.backup, "backup", "", "name of the Backup to restore (default: newest completed one, before -to if given)")
	f.StringVar(&c.to, "to", "", "point-in-time target, RFC 3339 (needs a WAL archive)")
	f.StringVar(&c.checkSQL, "check-sql", "", "SQL run with psql on the rehearsal and on the restored cluster; output is printed")
	f.StringVar(&c.out, "o", "", "also write the new Cluster manifest to this file")
	f.BoolVar(&c.rehearse, "rehearse", true, "restore into a throwaway cluster first and stop if that fails")
	f.BoolVar(&c.safety, "safety-backup", true, "back up the current state before deleting the cluster")
	f.BoolVar(&c.yes, "yes", false, "do not ask for confirmation")
	f.DurationVar(&c.timeout, "timeout", 20*time.Minute, "timeout for each wait (rehearsal, backup, deletion, recovery)")
	return c
}

func run(cmd string, args []string) error {
	c := newFlags(cmd)
	// Accept the cluster name before or after the flags.
	var name string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		name, args = args[0], args[1:]
	}
	if err := c.fs.Parse(args); err != nil {
		return err
	}
	if name == "" && c.fs.NArg() == 1 {
		name = c.fs.Arg(0)
	} else if c.fs.NArg() != 0 || name == "" {
		return fmt.Errorf("exactly one CLUSTER argument is required")
	}

	opts := rewind.Options{Backup: c.backup, Now: time.Now()}
	if c.to != "" {
		t, err := time.Parse(time.RFC3339, c.to)
		if err != nil {
			return fmt.Errorf("-to: %w", err)
		}
		opts.TargetTime = t
	}

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if c.kubeconfig != "" {
		rules.ExplicitPath = c.kubeconfig
	}
	kc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: c.kcontext})
	rc, err := kc.ClientConfig()
	if err != nil {
		return err
	}
	ns := c.ns
	if ns == "" {
		if ns, _, err = kc.Namespace(); err != nil {
			return err
		}
	}
	dyn, err := dynamic.NewForConfig(rc)
	if err != nil {
		return err
	}
	kube, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return err
	}
	cl := &rewind.Client{Dyn: dyn, Kube: kube, REST: rc, Out: os.Stdout, Poll: 3 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	p, err := cl.Load(ctx, ns, name, opts)
	if err != nil {
		return err
	}
	ro := rewind.RunOptions{Rehearse: c.rehearse, SafetyBackup: c.safety, CheckSQL: c.checkSQL, Timeout: c.timeout}
	p.Print(os.Stdout, ro)
	manifest, err := p.Manifest()
	if err != nil {
		return err
	}
	if c.out != "" {
		if err := os.WriteFile(c.out, manifest, 0o600); err != nil {
			return err
		}
		fmt.Printf("\nNew Cluster manifest written to %s\n", c.out)
	}
	if cmd == "plan" {
		fmt.Printf("\n--- new Cluster manifest\n%s", manifest)
		return nil
	}
	if !c.yes {
		fmt.Printf("\nThis deletes cluster %s/%s and recreates it from backup %s.\nType the cluster name to continue: ", ns, name, p.BackupName)
		line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if strings.TrimSpace(line) != name {
			return fmt.Errorf("aborted")
		}
	}
	fmt.Println()
	return cl.Run(ctx, p, ro)
}
