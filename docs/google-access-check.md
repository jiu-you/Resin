# Platform-scoped Google regional redirect checks

This fork adds an optional Google access classifier without coupling it to Resin's generic node-health circuit breaker.

## Platform settings

The platform detail configuration page exposes two persisted settings:

- `google_check_enabled`: periodically classify nodes currently eligible for the platform.
- `google_check_interval`: per-platform automatic check interval in Go duration syntax; default `24h`, minimum `1m`.
- `google_reject_sent_to_china`: exclude nodes classified as `sent_to_china` from this platform only.

The rejection setting requires checking to be enabled. Existing and newly created platforms default both switches to `false` and the interval to `24h`.

## Classification

The checker requests `https://www.google.com/` through the node with automatic redirects disabled.

- HTTP 2xx: `ok`
- Redirect to `google.cn`, a `google.cn` subdomain, `google.com.hk`, or a `google.com.hk` subdomain: `sent_to_china`
- Transport failure, unexpected status, or unrelated redirect: `unavailable`
- Not checked yet: `unknown`

Only `sent_to_china` is eligible for platform-local exclusion. `unknown` and `unavailable` remain routable. Google failures never increment `FailureCount` and never open the generic node circuit breaker.

Results are kept in memory and refreshed according to each enabled platform's interval. If a node belongs to multiple enabled platforms, the shortest applicable interval wins. After restart, enabled platforms classify their eligible nodes again. The node API includes the latest status, timestamp, redirect host, and reason. A manual check is also available:

```http
POST /api/v1/nodes/{hash}/actions/check-google
```

## Keeping the fork easy to rebase

Keep this feature as a small commit stack on a feature branch rather than developing directly on `master`:

```bash
git remote add upstream https://github.com/Resinat/Resin.git
git fetch upstream
git switch -c feature/google-cn-detection
```

Recommended commit split:

1. Google classifier and outbound response metadata helper.
2. Platform persistence, filtering, API, and migrations.
3. Web UI configuration and status display.

To update from upstream:

```bash
git fetch upstream
git switch master
git rebase upstream/master
git switch feature/google-cn-detection
git rebase master
```

Most implementation is isolated in `internal/googlecheck/` and `internal/netutil/outbound_http_response.go`. Existing upstream files contain only model propagation, lifecycle wiring, API routing, and UI fields.
