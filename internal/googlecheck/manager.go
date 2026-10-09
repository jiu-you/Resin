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
	"github.com/Resinat/Resin/internal/topology"
)

const DefaultCheckURL = "https://www.google.com/"

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
	Concurrency int
	CheckURL    string
}

// Manager executes Google checks submitted by successful egress probes.
// Its queue and workers are independent from generic probe workers so Google
// failures and latency cannot block or trip Resin's normal health checks.
type Manager struct {
	pool        *topology.GlobalNodePool
	fetcher     Fetcher
	timeout     time.Duration
	checkURL    string
	workerCount int

	queueMu   sync.Mutex
	queueCond *sync.Cond
	queue     []node.Hash
	queueHead int
	stopped   bool
	pending   sync.Map // map[node.Hash]struct{}

	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

// NewManager creates a Google access manager.
func NewManager(cfg Config) *Manager {
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
	m := &Manager{
		pool: cfg.Pool, fetcher: cfg.Fetcher, timeout: timeout,
		checkURL: checkURL, workerCount: concurrency,
	}
	m.queueCond = sync.NewCond(&m.queueMu)
	return m
}

// Start starts the independent Google check workers.
func (m *Manager) Start() {
	if m == nil || m.pool == nil || m.fetcher == nil {
		return
	}
	m.startOnce.Do(func() {
		for i := 0; i < m.workerCount; i++ {
			m.wg.Add(1)
			go m.worker()
		}
	})
}

// Stop drops queued checks and waits for in-flight checks.
func (m *Manager) Stop() {
	if m == nil {
		return
	}
	m.stopOnce.Do(func() {
		m.queueMu.Lock()
		m.stopped = true
		for i := m.queueHead; i < len(m.queue); i++ {
			m.pending.Delete(m.queue[i])
		}
		m.queue = nil
		m.queueHead = 0
		m.queueMu.Unlock()
		m.queueCond.Broadcast()
	})
	m.wg.Wait()
}

// Trigger enqueues a node check without blocking the egress probe worker.
// Duplicate queued/running checks for the same node are coalesced.
func (m *Manager) Trigger(hash node.Hash) bool {
	if m == nil || m.pool == nil || m.fetcher == nil {
		return false
	}
	if _, loaded := m.pending.LoadOrStore(hash, struct{}{}); loaded {
		return true
	}

	m.queueMu.Lock()
	if m.stopped {
		m.queueMu.Unlock()
		m.pending.Delete(hash)
		return false
	}
	m.queue = append(m.queue, hash)
	m.queueMu.Unlock()
	m.queueCond.Signal()
	return true
}

func (m *Manager) worker() {
	defer m.wg.Done()
	for {
		hash, ok := m.dequeue()
		if !ok {
			return
		}
		_, _ = m.CheckNow(hash)
		m.pending.Delete(hash)
	}
}

func (m *Manager) dequeue() (node.Hash, bool) {
	m.queueMu.Lock()
	defer m.queueMu.Unlock()
	for !m.stopped && m.queueHead >= len(m.queue) {
		m.queueCond.Wait()
	}
	if m.stopped {
		return node.Hash{}, false
	}
	hash := m.queue[m.queueHead]
	m.queueHead++
	if m.queueHead >= len(m.queue) {
		m.queue = nil
		m.queueHead = 0
	} else if m.queueHead > 1024 && m.queueHead*2 >= len(m.queue) {
		m.queue = append([]node.Hash(nil), m.queue[m.queueHead:]...)
		m.queueHead = 0
	}
	return hash, true
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
