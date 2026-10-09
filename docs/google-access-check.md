# Google regional redirect checks

This fork adds a node-level Google access classifier without coupling it to Resin's generic node-health circuit breaker.

## Detection lifecycle

A successful egress IP/region probe submits the same node to an independent Google-check queue. This applies to:

- the immediate egress probe for a newly added or re-enabled node;
- scheduled egress probes controlled by `max_egress_test_interval`;
- manual egress probes.

The Google workers are independent from ProbeManager workers. Google latency, rate limits, or failures therefore do not block egress probing, increment `FailureCount`, or open the generic circuit breaker. Duplicate queued/running checks for the same node are coalesced.

## Classification

The checker requests `https://www.google.com/` through the node with automatic redirects disabled.

- HTTP 2xx: `ok`
- Redirect to `google.cn`, a `google.cn` subdomain, `google.com.hk`, or a `google.com.hk` subdomain: `sent_to_china`
- Transport failure, unexpected status, or unrelated redirect: `unavailable`
- Not checked yet: `unknown`

Results are kept in memory. The node API and node list include the latest status, timestamp, redirect host, and reason. A manual Google-only check is also available:

```http
POST /api/v1/nodes/{hash}/actions/check-google
```

## Platform setting

Platforms expose only one persisted policy:

- `google_reject_sent_to_china`: exclude nodes classified as `sent_to_china` from this platform.

The classification is global per node, while rejection is platform-local. `unknown` and `unavailable` nodes remain routable, and another platform with rejection disabled can continue using the same node.

## Keeping the fork easy to rebase

Keep this feature as a small commit stack on a feature branch rather than developing directly on `master`:

```bash
git remote add upstream https://github.com/Resinat/Resin.git
git fetch upstream
git switch -c feature/google-cn-detection
```

To update from upstream:

```bash
git fetch upstream
git switch master
git rebase upstream/master
git switch feature/google-cn-detection
git rebase master
```

Most implementation is isolated in `internal/googlecheck/` and `internal/netutil/outbound_http_response.go`. Existing upstream files contain only lifecycle wiring, the egress-success callback, model propagation, API routing, and UI fields.
