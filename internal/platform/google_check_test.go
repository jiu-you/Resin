package platform

import (
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
)

func TestPlatform_GoogleSentToChinaPolicyIsPlatformLocal(t *testing.T) {
	h := makeHash(`{"type":"ss","name":"google-policy"}`)
	entry := makeFullyRoutableEntry(h, "sub1")
	entry.SetGoogleAccessState(node.GoogleAccessState{
		Status:    node.GoogleAccessSentToChina,
		CheckedAt: time.Now(),
	})

	rejecting := NewPlatform("rejecting", "Rejecting", nil, nil)
	rejecting.GoogleCheckEnabled = true
	rejecting.GoogleRejectSentToChina = true
	rejecting.FullRebuild(func(fn func(node.Hash, *node.NodeEntry) bool) {
		fn(h, entry)
	}, alwaysLookup, usGeoLookup)
	if rejecting.View().Contains(h) {
		t.Fatal("sent-to-China node should be excluded from rejecting platform")
	}

	permissive := NewPlatform("permissive", "Permissive", nil, nil)
	permissive.GoogleCheckEnabled = true
	permissive.GoogleRejectSentToChina = false
	permissive.FullRebuild(func(fn func(node.Hash, *node.NodeEntry) bool) {
		fn(h, entry)
	}, alwaysLookup, usGeoLookup)
	if !permissive.View().Contains(h) {
		t.Fatal("same node should remain routable in a platform that does not reject it")
	}
}

func TestPlatform_GoogleUnavailableRemainsRoutable(t *testing.T) {
	h := makeHash(`{"type":"ss","name":"google-unavailable"}`)
	entry := makeFullyRoutableEntry(h, "sub1")
	entry.SetGoogleAccessState(node.GoogleAccessState{
		Status:    node.GoogleAccessUnavailable,
		CheckedAt: time.Now(),
		Reason:    "timeout",
	})

	p := NewPlatform("p1", "Test", nil, nil)
	p.GoogleCheckEnabled = true
	p.GoogleRejectSentToChina = true
	p.FullRebuild(func(fn func(node.Hash, *node.NodeEntry) bool) {
		fn(h, entry)
	}, alwaysLookup, usGeoLookup)
	if !p.View().Contains(h) {
		t.Fatal("unavailable Google check must not remove an otherwise healthy node")
	}
}
