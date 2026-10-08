package googlecheck

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/scanloop"
	"github.com/Resinat/Resin/internal/topology"
)

const (
	DefaultCheckURL  = "https://www.google.com/"
	defaultInterval  = 24 * time.Hour
	defaultQueueSize = 1024
)

// Response is the subset of HTTP response metadata needed for classification.
type Response struct {
	StatusCode int
	Header     http.Header
}

// Fetcher executes a redirect-preserving request through a specific node.
type Fetcher func(ctx context.Context, hash node.Hash, targetURL string) (*Response, error)

// Config configures Manager.
type Config struct {
	Pool        *topology.GlobalNodePool
	Fetcher     Fetcher
	Timeout     time.Duration
	Interval    time.Duration
	Concurrency int
	CheckURL    string
}

// Manager periodically checks nodes belonging to platforms that opted in.
type Manager struct {
	pool        *topology.GlobalNodePool
	fetcher     Fetcher
	timeout     time.Duration
	interval    time.Duration
	checkURL    string
	workerCount int

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
	queue    chan node.Hash
	pending  sync.Map // map[node.Hash]struct{}
	tracked  sync.Map // map[node.Hash]struct{}, keeps rejected nodes eligible for refresh
}

// NewManager creates a Google access manager.
func NewManager(cfg Config) *Manager {
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = 4
	}
	checkURL := strings.TrimSpace(cfg.CheckURL)
	if checkURL == "" {
		checkURL = DefaultCheckURL
	}
	return &Manager{
		pool: cfg.Pool, fetcher: cfg.Fetcher, timeout: timeout,
		interval: interval, checkURL: checkURL, workerCount: concurrency,
		stopCh: make(chan struct{}), queue: make(chan node.Hash, max(defaultQueueSize, concurrency*4)),
	}
}

// Start starts the scanner and workers.
func (m *Manager) Start() {
	if m == nil || m.pool == nil || m.fetcher == nil {
		return
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		scanloop.Run(m.stopCh, scanloop.DefaultMinInterval, scanloop.DefaultJitterRange, m.scan)
	}()
	for i := 0; i < m.workerCount; i++ {
		m.wg.Add(1)
		go m.worker()
	}
}

// Stop stops the scanner and waits for in-flight checks.
func (m *Manager) Stop() {
	if m == nil {
		return
	}
	m.stopOnce.Do(func() { close(m.stopCh) })
	m.wg.Wait()
}

func (m *Manager) worker() {
	defer m.wg.Done()
	for {
		select {
		case <-m.stopCh:
			return
		case hash := <-m.queue:
			_, _ = m.CheckNow(hash)
			m.pending.Delete(hash)
		}
	}
}

func (m *Manager) scan() {
	if m.pool == nil {
		return
	}
	candidates := make(map[node.Hash]time.Duration)
	enabled := false
	trackedInterval := time.Duration(0)
	m.pool.RangePlatforms(func(plat *platform.Platform) bool {
		if plat == nil || !plat.GoogleCheckEnabled {
			return true
		}
		enabled = true
		interval := time.Duration(plat.GoogleCheckIntervalNs)
		if interval <= 0 {
			interval = m.interval
		}
		if trackedInterval == 0 || interval < trackedInterval {
			trackedInterval = interval
		}
		plat.View().Range(func(hash node.Hash) bool {
			if current, ok := candidates[hash]; !ok || interval < current {
				candidates[hash] = interval
			}
			return true
		})
		return true
	})
	if !enabled {
		return
	}
	// A rejected sent-to-China node is no longer present in platform views.
	// Keep previously checked nodes eligible so a later clean result can restore it.
	m.tracked.Range(func(key, _ any) bool {
		if hash, ok := key.(node.Hash); ok {
			if _, exists := candidates[hash]; !exists {
				candidates[hash] = trackedInterval
			}
		}
		return true
	})

	now := time.Now()
	for hash, interval := range candidates {
		entry, ok := m.pool.GetEntry(hash)
		if !ok || entry == nil || !entry.HasOutbound() {
			continue
		}
		last := entry.GetGoogleAccessState().CheckedAt
		if !last.IsZero() && now.Sub(last) < interval {
			continue
		}
		if _, loaded := m.pending.LoadOrStore(hash, struct{}{}); loaded {
			continue
		}
		select {
		case m.queue <- hash:
		case <-m.stopCh:
			m.pending.Delete(hash)
			return
		default:
			m.pending.Delete(hash)
		}
	}
}

// TriggerScan asks the manager to enqueue currently eligible nodes immediately.
func (m *Manager) TriggerScan() {
	if m == nil {
		return
	}
	m.scan()
}

// CheckNow performs a synchronous check and updates the node state.
func (m *Manager) CheckNow(hash node.Hash) (node.GoogleAccessState, error) {
	if m == nil || m.pool == nil || m.fetcher == nil {
		return node.GoogleAccessState{}, fmt.Errorf("google checker is not configured")
	}
	entry, ok := m.pool.GetEntry(hash)
	if !ok || entry == nil {
		return node.GoogleAccessState{}, fmt.Errorf("node not found")
	}
	if !entry.HasOutbound() {
		return node.GoogleAccessState{}, fmt.Errorf("node outbound is not ready")
	}

	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	resp, err := m.fetcher(ctx, hash, m.checkURL)
	state := Classify(resp, err, time.Now().UTC())
	entry.SetGoogleAccessState(state)
	m.tracked.Store(hash, struct{}{})
	m.pool.NotifyNodeDirty(hash)
	return state, err
}

// Classify maps a redirect-preserving Google response to a node state.
func Classify(resp *Response, fetchErr error, checkedAt time.Time) node.GoogleAccessState {
	state := node.GoogleAccessState{Status: node.GoogleAccessUnavailable, CheckedAt: checkedAt}
	if fetchErr != nil {
		state.Reason = fetchErr.Error()
		return state
	}
	if resp == nil {
		state.Reason = "empty_response"
		return state
	}
	state.HTTPStatus = resp.StatusCode

	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		location := strings.TrimSpace(resp.Header.Get("Location"))
		if location == "" {
			state.Reason = "redirect_without_location"
			return state
		}
		u, err := url.Parse(location)
		if err != nil {
			state.Reason = "invalid_redirect_location"
			return state
		}
		host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
		state.RedirectHost = host
		if isSentToChinaHost(host) {
			state.Status = node.GoogleAccessSentToChina
			state.Reason = "redirected_to_china_google_host"
			return state
		}
		state.Reason = "unexpected_redirect"
		return state
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		state.Status = node.GoogleAccessOK
		state.Reason = ""
		return state
	}
	state.Reason = fmt.Sprintf("unexpected_http_status_%d", resp.StatusCode)
	return state
}

func isSentToChinaHost(host string) bool {
	return host == "google.cn" || strings.HasSuffix(host, ".google.cn") ||
		host == "google.com.hk" || strings.HasSuffix(host, ".google.com.hk")
}
