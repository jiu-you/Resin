package googlecheck

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func TestManagerCheckNow_DoesNotAffectGenericHealth(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	hash := node.HashFromRawOptions([]byte(`{"type":"google-check"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"google-check"}`), "sub1")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("node missing")
	}
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	entry.FailureCount.Store(2)
	circuitBefore := entry.CircuitOpenSince.Load()

	mgr := NewManager(Config{
		Pool: pool,
		Fetcher: func(_ context.Context, _ node.Hash, _ string) (*Response, error) {
			return &Response{StatusCode: http.StatusFound, Header: http.Header{
				"Location": []string{"https://www.google.com.hk/"},
			}}, nil
		},
	})
	state, err := mgr.CheckNow(hash)
	if err != nil {
		t.Fatalf("CheckNow: %v", err)
	}
	if state.Status != node.GoogleAccessSentToChina {
		t.Fatalf("status: got %q want %q", state.Status, node.GoogleAccessSentToChina)
	}
	if got := entry.FailureCount.Load(); got != 2 {
		t.Fatalf("generic failure count changed: got %d want 2", got)
	}
	if got := entry.CircuitOpenSince.Load(); got != circuitBefore {
		t.Fatalf("generic circuit state changed: got %d want %d", got, circuitBefore)
	}
}

func TestManagerScan_UsesPlatformInterval(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	sub := subscription.NewSubscription("sub1", "Sub1", "url", true, false)
	subMgr.Register(sub)
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})
	raw := json.RawMessage(`{"type":"google-interval"}`)
	hash := node.HashFromRawOptions(raw)
	managed := subscription.NewManagedNodes()
	managed.StoreNode(hash, subscription.ManagedNode{Tags: []string{"node"}})
	sub.SwapManagedNodes(managed)
	pool.AddNodeFromSub(hash, raw, "sub1")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("node missing")
	}
	outbound := testutil.NewNoopOutbound()
	entry.Outbound.Store(&outbound)
	entry.CircuitOpenSince.Store(0)
	entry.SetEgressIP(netip.MustParseAddr("203.0.113.10"))
	entry.LatencyTable.LoadEntry("google.com", node.DomainLatencyStats{
		Ewma:        50 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	entry.SetGoogleAccessState(node.GoogleAccessState{
		Status:    node.GoogleAccessOK,
		CheckedAt: time.Now().Add(-2 * time.Hour),
	})

	plat := platform.NewPlatform("p1", "P1", nil, nil)
	plat.GoogleCheckEnabled = true
	plat.GoogleCheckIntervalNs = int64(3 * time.Hour)
	pool.RebuildPlatform(plat)
	pool.RegisterPlatform(plat)

	mgr := NewManager(Config{Pool: pool, Fetcher: func(context.Context, node.Hash, string) (*Response, error) {
		return &Response{StatusCode: http.StatusOK, Header: make(http.Header)}, nil
	}})
	mgr.scan()
	if got := len(mgr.queue); got != 0 {
		t.Fatalf("3h interval should not enqueue a 2h-old result, queue=%d", got)
	}

	next := platform.NewPlatform("p1", "P1", nil, nil)
	next.GoogleCheckEnabled = true
	next.GoogleCheckIntervalNs = int64(time.Hour)
	if err := pool.ReplacePlatform(next); err != nil {
		t.Fatalf("replace platform: %v", err)
	}
	mgr.scan()
	if got := len(mgr.queue); got != 1 {
		t.Fatalf("1h interval should enqueue a 2h-old result, queue=%d", got)
	}
}

func TestClassify(t *testing.T) {
	checkedAt := time.Unix(123, 0).UTC()
	tests := []struct {
		name         string
		resp         *Response
		err          error
		wantStatus   node.GoogleAccessStatus
		wantRedirect string
	}{
		{
			name:       "success",
			resp:       &Response{StatusCode: http.StatusOK, Header: make(http.Header)},
			wantStatus: node.GoogleAccessOK,
		},
		{
			name: "hong kong redirect is sent to china",
			resp: &Response{StatusCode: http.StatusFound, Header: http.Header{
				"Location": []string{"https://www.google.com.hk/"},
			}},
			wantStatus:   node.GoogleAccessSentToChina,
			wantRedirect: "www.google.com.hk",
		},
		{
			name: "google cn subdomain is sent to china",
			resp: &Response{StatusCode: http.StatusTemporaryRedirect, Header: http.Header{
				"Location": []string{"https://news.google.cn/"},
			}},
			wantStatus:   node.GoogleAccessSentToChina,
			wantRedirect: "news.google.cn",
		},
		{
			name: "unrelated redirect is unavailable not sent to china",
			resp: &Response{StatusCode: http.StatusFound, Header: http.Header{
				"Location": []string{"https://accounts.google.com/"},
			}},
			wantStatus:   node.GoogleAccessUnavailable,
			wantRedirect: "accounts.google.com",
		},
		{
			name:       "transport error",
			err:        errors.New("timeout"),
			wantStatus: node.GoogleAccessUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Classify(tt.resp, tt.err, checkedAt)
			if got.Status != tt.wantStatus {
				t.Fatalf("status: got %q want %q", got.Status, tt.wantStatus)
			}
			if got.RedirectHost != tt.wantRedirect {
				t.Fatalf("redirect host: got %q want %q", got.RedirectHost, tt.wantRedirect)
			}
			if !got.CheckedAt.Equal(checkedAt) {
				t.Fatalf("checked_at: got %v want %v", got.CheckedAt, checkedAt)
			}
		})
	}
}
