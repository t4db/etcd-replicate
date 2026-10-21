// Package t4 tests replication between T4 and etcd in both directions. It is
// a module of its own so that the replicator's module does not depend on T4.
package t4

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/t4db/t4"
	t4etcd "github.com/t4db/t4/etcd"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"

	replicate "github.com/t4db/etcd-replicate"
)

// TestReplicateT4ToEtcd replicates a T4 database into etcd while a random
// workload runs on it, half of it written before the replicator starts.
func TestReplicateT4ToEtcd(t *testing.T) {
	testReplicateWorkload(t, startT4(t), startEtcd(t))
}

// TestReplicateEtcdToT4 replicates etcd into a T4 database.
func TestReplicateEtcdToT4(t *testing.T) {
	testReplicateWorkload(t, startEtcd(t), startT4(t))
}

func testReplicateWorkload(t *testing.T, source, target *clientv3.Client) {
	s := seed(t)
	w := &workload{t: t, cli: source, rnd: rand.New(rand.NewSource(s))}
	for range 150 {
		w.step()
	}
	r := startReplicator(t, source, target)
	for range 250 {
		w.step()
	}
	waitApplied(t, r, source)
	assertReplicated(t, source, target)
	waitLeasesMirrored(t, source, target)
}

// TestReplicateT4LeaseExpiry: when a lease expires on a T4 source, the
// deletions of its keys reach the target at the same revisions, and the
// target's copy of the lease, which has a longer TTL, is revoked once its
// keys are gone.
func TestReplicateT4LeaseExpiry(t *testing.T) {
	ctx := testCtx(t)
	source, target := startT4(t), startEtcd(t)
	r := startReplicator(t, source, target)

	lease, err := source.Grant(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"/leased/a", "/leased/b"} {
		if _, err := source.Put(ctx, k, "v", clientv3.WithLease(lease.ID)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.Put(ctx, "/plain", "v"); err != nil {
		t.Fatal(err)
	}
	waitApplied(t, r, source)
	ttl, err := target.TimeToLive(ctx, lease.ID)
	if err != nil || ttl.GrantedTTL <= 2 {
		t.Fatalf("target lease: %+v, %v; want the source's lease with a longer TTL", ttl, err)
	}

	eventually(t, 20*time.Second, "source lease to expire", func() bool {
		resp, err := source.Get(ctx, "/leased/", clientv3.WithPrefix(), clientv3.WithCountOnly())
		return err == nil && resp.Count == 0
	})
	waitApplied(t, r, source)
	assertReplicated(t, source, target)
	eventually(t, 5*time.Second, "target lease to be revoked", func() bool {
		return leaseIDs(t, target) == ""
	})
}

// workload drives a source with random puts, deletes, transactions and lease
// operations on a small keyspace, so that keys are often overwritten,
// deleted, recreated and attached to leases.
type workload struct {
	t      *testing.T
	cli    *clientv3.Client
	rnd    *rand.Rand
	leases []clientv3.LeaseID
}

func (w *workload) key() string { return "/k/" + strconv.Itoa(w.rnd.Intn(16)) }
func (w *workload) val() string { return "v" + strconv.Itoa(w.rnd.Intn(1000)) }

// leaseOpt attaches a live lease to a put half of the time.
func (w *workload) leaseOpt() []clientv3.OpOption {
	if len(w.leases) == 0 || w.rnd.Intn(2) == 0 {
		return nil
	}
	return []clientv3.OpOption{clientv3.WithLease(w.leases[w.rnd.Intn(len(w.leases))])}
}

func (w *workload) step() {
	w.t.Helper()
	ctx := testCtx(w.t)
	var err error
	switch op := w.rnd.Intn(10); {
	case op < 4:
		_, err = w.cli.Put(ctx, w.key(), w.val(), w.leaseOpt()...)
	case op < 5:
		_, err = w.cli.Delete(ctx, w.key())
	case op < 6:
		// A range delete of a few adjacent keys: one revision, several events.
		from := w.rnd.Intn(16)
		_, err = w.cli.Delete(ctx, "/k/"+strconv.Itoa(from), clientv3.WithRange("/k/"+strconv.Itoa(from+3)))
	case op < 8:
		// A transaction may not touch a key twice.
		key, other := w.key(), w.key()
		for other == key {
			other = w.key()
		}
		_, err = w.cli.Txn(ctx).
			If(clientv3.Compare(clientv3.Version(key), ">", 1)).
			Then(clientv3.OpPut(key, w.val()), clientv3.OpDelete(other)).
			Else(clientv3.OpPut(w.key(), w.val(), w.leaseOpt()...)).
			Commit()
	case op < 9:
		var resp *clientv3.LeaseGrantResponse
		resp, err = w.cli.Grant(ctx, 600)
		if err == nil {
			w.leases = append(w.leases, resp.ID)
		}
	default:
		if len(w.leases) == 0 {
			return
		}
		i := w.rnd.Intn(len(w.leases))
		_, err = w.cli.Revoke(ctx, w.leases[i])
		w.leases = append(w.leases[:i], w.leases[i+1:]...)
	}
	if err != nil {
		w.t.Fatalf("workload: %v", err)
	}
}

// seed returns the workload seed: ETCD_REPLICATE_SEED, or a random one that
// the test logs so a failure can be rerun.
func seed(t *testing.T) int64 {
	t.Helper()
	if v := os.Getenv("ETCD_REPLICATE_SEED"); v != "" {
		s, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("ETCD_REPLICATE_SEED: %v", err)
		}
		return s
	}
	s := time.Now().UnixNano()
	t.Logf("seed %d (rerun with ETCD_REPLICATE_SEED=%d)", s, s)
	return s
}

func startT4(t *testing.T) *clientv3.Client {
	t.Helper()
	node, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("t4.Open: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(t4etcd.NewServerOptions(nil, nil)...)
	t4etcd.New(node, nil, nil).Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return dial(t, lis.Addr().String())
}

func startEtcd(t *testing.T) *clientv3.Client {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	client := url.URL{Scheme: "http", Host: freePort(t)}
	peer := url.URL{Scheme: "http", Host: freePort(t)}
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = []url.URL{client}, []url.URL{client}
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = []url.URL{peer}, []url.URL{peer}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("start etcd: %v", err)
	}
	t.Cleanup(e.Close)
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		t.Fatal("etcd not ready")
	}
	return dial(t, client.Host)
}

func freePort(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lis.Close() }()
	return lis.Addr().String()
}

func dial(t *testing.T, endpoint string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// startReplicator runs the replicator until the test ends. A replicator that
// stops early fails the test.
func startReplicator(t *testing.T, source, target *clientv3.Client) *replicate.Replicator {
	t.Helper()
	r, err := replicate.New(replicate.Config{
		Source:            source,
		Target:            target,
		LeaseTTLMargin:    time.Minute,
		ReconcileInterval: 100 * time.Millisecond,
		RetryInterval:     50 * time.Millisecond,
		Log:               slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("replicator stopped: %v", err)
		}
	})
	return r
}

func waitApplied(t *testing.T, r *replicate.Replicator, source *clientv3.Client) {
	t.Helper()
	resp, err := source.Get(testCtx(t), "/")
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, fmt.Sprintf("target to reach revision %d", resp.Header.Revision), func() bool {
		return r.Applied() >= resp.Header.Revision
	})
}

// assertReplicated checks that the target holds exactly the source's keys
// with the same metadata, at the same revision.
func assertReplicated(t *testing.T, source, target *clientv3.Client) {
	t.Helper()
	srcRev, src := state(t, source)
	dstRev, dst := state(t, target)
	if srcRev != dstRev {
		t.Fatalf("revision: source %d, target %d", srcRev, dstRev)
	}
	if src != dst {
		t.Fatalf("target differs from source:\nsource:\n%s\ntarget:\n%s", src, dst)
	}
}

func state(t *testing.T, cli *clientv3.Client) (int64, string) {
	t.Helper()
	resp, err := cli.Get(testCtx(t), "", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	var kvs []string
	for _, kv := range resp.Kvs {
		if !strings.HasPrefix(string(kv.Key), replicate.DefaultStatePrefix) {
			kvs = append(kvs, kvString(kv))
		}
	}
	return resp.Header.Revision, strings.Join(kvs, "\n")
}

func kvString(kv *mvccpb.KeyValue) string {
	return fmt.Sprintf("%s=%q c=%d m=%d v=%d l=%x", kv.Key, kv.Value, kv.CreateRevision, kv.ModRevision, kv.Version, kv.Lease)
}

// waitLeasesMirrored waits until the target has exactly the source's leases.
// Revoked leases leave the target on the next reconcile.
func waitLeasesMirrored(t *testing.T, source, target *clientv3.Client) {
	t.Helper()
	eventually(t, 5*time.Second, "target leases to match the source's", func() bool {
		return leaseIDs(t, source) == leaseIDs(t, target)
	})
}

func leaseIDs(t *testing.T, cli *clientv3.Client) string {
	t.Helper()
	resp, err := cli.Leases(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(resp.Leases))
	for i, l := range resp.Leases {
		ids[i] = fmt.Sprintf("%x", l.ID)
	}
	slices.Sort(ids)
	return strings.Join(ids, ",")
}

func eventually(t *testing.T, timeout time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
