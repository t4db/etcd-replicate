package replicate

import (
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// eventually waits until ok holds, failing the test after timeout.
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

func hasLease(t *testing.T, cli *clientv3.Client, id clientv3.LeaseID) bool {
	t.Helper()
	resp, err := cli.Leases(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range resp.Leases {
		if l.ID == id {
			return true
		}
	}
	return false
}

// TestReplicateLeaseExpiry: a lease that expires on the source deletes its
// keys there; the deletions reach the target at the same revisions, and the
// target's copy of the lease, granted with the source's TTL plus the margin,
// is revoked once its keys are gone.
func TestReplicateLeaseExpiry(t *testing.T) {
	ctx := testCtx(t)
	source, target := startEtcd(t), startEtcd(t)
	r := newReplicator(t, source, target)
	run(t, r)

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
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(2 + time.Minute/time.Second); ttl.GrantedTTL != want {
		t.Fatalf("target lease TTL %d, want the source's 2 plus the 60s margin = %d", ttl.GrantedTTL, want)
	}

	eventually(t, 20*time.Second, "the source lease to expire", func() bool {
		resp, err := source.Get(ctx, "/leased/", clientv3.WithPrefix(), clientv3.WithCountOnly())
		return err == nil && resp.Count == 0
	})
	waitApplied(t, r, source)
	assertReplicated(t, source, target)
	eventually(t, 5*time.Second, "the target lease to be revoked", func() bool {
		return !hasLease(t, target, lease.ID)
	})
}

// TestReplicateKeepsTargetLeaseAlive: while the source keeps a lease alive,
// the replicator keeps the target's copy alive past its own TTL, so the
// target never deletes the lease's keys on its own.
func TestReplicateKeepsTargetLeaseAlive(t *testing.T) {
	ctx := testCtx(t)
	source, target := startEtcd(t), startEtcd(t)
	r := newReplicator(t, source, target, func(c *Config) { c.LeaseTTLMargin = time.Second })
	run(t, r)

	lease, err := source.Grant(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	ka, err := source.KeepAlive(ctx, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for range ka {
		}
	}()
	if _, err := source.Put(ctx, "/leased", "v", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatal(err)
	}
	waitApplied(t, r, source)

	// Three times the target lease's TTL of 2s + 1s; with a 1s margin it is
	// renewed every 100ms.
	time.Sleep(9 * time.Second)
	if !hasLease(t, target, lease.ID) {
		t.Fatal("target lease expired while the source kept it alive")
	}
	if _, err := source.Put(ctx, "/next", "v"); err != nil {
		t.Fatal(err)
	}
	waitApplied(t, r, source)
	assertReplicated(t, source, target)
}

// TestReplicateHaltsOnLeaseExpiredOnTarget: a lease the source still has but
// the target lost means the target can no longer follow the source, so the
// replicator halts.
func TestReplicateHaltsOnLeaseExpiredOnTarget(t *testing.T) {
	ctx := testCtx(t)
	source, target := startEtcd(t), startEtcd(t)
	// A small margin renews target leases, and so notices the lost one,
	// every 200ms.
	r := newReplicator(t, source, target, func(c *Config) { c.LeaseTTLMargin = 2 * time.Second })
	running := run(t, r)

	// A lease without keys is granted on the target by the reconcile loop,
	// and revoking it there does not change the target's revision.
	lease, err := source.Grant(ctx, 600)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "the target to get the lease", func() bool {
		return hasLease(t, target, lease.ID)
	})
	if _, err := target.Revoke(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	mustHalt(t, running, "expired on the target")
}
