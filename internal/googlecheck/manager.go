package googlecheck

import (
	"context"
	"fmt"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/scanloop"
	"github.com/Resinat/Resin/internal/topology"
)

const (
	DefaultCheckURL          = "https://www.google.com/"
	activeLeaseCheckInterval = 10 * time.Minute
	minimumIPCheckInterval   = time.Minute
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
	Concurrency int
	CheckURL    string
}

type queueTask struct {
	hash node.Hash
	ip   netip.Addr
}

// Manager executes Google checks submitted by successful egress probes and
// periodically refreshes IPs leased by Google-rejecting platforms.
type Manager struct {
	pool        *topology.GlobalNodePool
	fetcher     Fetcher
	timeout     time.Duration
	checkURL    string
	workerCount int

	callbackMu         sync.RWMutex
	activeNodeProvider func() []node.Hash
	onClassified       func(netip.Addr, node.GoogleAccessState)

	queueMu   sync.Mutex
	queueCond *sync.Cond
	queue     []queueTask
	queueHead int
	stopped   bool
	pending   sync.Map // map[netip.Addr]struct{}
	ipStates  sync.Map // map[netip.Addr]node.GoogleAccessState

	stopCh    chan struct{}
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
		checkURL: checkURL, workerCount: concurrency, stopCh: make(chan struct{}),
	}
	m.queueCond = sync.NewCond(&m.queueMu)
	return m
}

// SetActiveNodeProvider configures the source of nodes with non-expired leases
// in platforms that enabled Google rejection. Call before Start.
func (m *Manager) SetActiveNodeProvider(provider func() []node.Hash) {
	if m == nil {
		return
	}
	m.callbackMu.Lock()
	m.activeNodeProvider = provider
	m.callbackMu.Unlock()
}

// SetOnClassified configures a callback invoked after an IP classification has
// been applied to every node currently using that IP. Call before Start.
func (m *Manager) SetOnClassified(callback func(netip.Addr, node.GoogleAccessState)) {
	if m == nil {
		return
	}
	m.callbackMu.Lock()
	m.onClassified = callback
	m.callbackMu.Unlock()
}

// Start starts the independent Google check workers and active-lease scanner.
func (m *Manager) Start() {
	if m == nil || m.pool == nil || m.fetcher == nil {
		return
	}
	m.startOnce.Do(func() {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			scanloop.Run(m.stopCh, scanloop.DefaultMinInterval, scanloop.DefaultJitterRange, m.scanActiveLeases)
		}()
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
		close(m.stopCh)
		m.queueMu.Lock()
		m.stopped = true
		for i := m.queueHead; i < len(m.queue); i++ {
			m.pending.Delete(m.queue[i].ip)
		}
		m.queue = nil
		m.queueHead = 0
		m.queueMu.Unlock()
		m.queueCond.Broadcast()
	})
	m.wg.Wait()
}

// Trigger enqueues a node after a successful egress probe. Results are shared
// and rate-limited by egress IP, so multiple nodes using the same IP do not
// produce duplicate Google requests.
func (m *Manager) Trigger(hash node.Hash) bool {
	return m.triggerIfDue(hash, minimumIPCheckInterval)
}

func (m *Manager) scanActiveLeases() {
	m.callbackMu.RLock()
	provider := m.activeNodeProvider
	m.callbackMu.RUnlock()
	if provider == nil {
		return
	}
	for _, hash := range provider() {
		m.triggerIfDue(hash, activeLeaseCheckInterval)
	}
}

func (m *Manager) triggerIfDue(hash node.Hash, interval time.Duration) bool {
	if m == nil || m.pool == nil || m.fetcher == nil {
		return false
	}
	entry, ok := m.pool.GetEntry(hash)
	if !ok || entry == nil || !entry.HasOutbound() {
		return false
	}
	ip := entry.GetEgressIP()
	if !ip.IsValid() {
		return false
	}
	if cached, ok := m.ipStates.Load(ip); ok {
		state := cached.(node.GoogleAccessState)
		if !state.CheckedAt.IsZero() && time.Since(state.CheckedAt) < interval {
			entry.SetGoogleAccessState(state)
			m.pool.NotifyNodeDirty(hash)
			return true
		}
	}
	if _, loaded := m.pending.LoadOrStore(ip, struct{}{}); loaded {
		return true
	}

	m.queueMu.Lock()
	if m.stopped {
		m.queueMu.Unlock()
		m.pending.Delete(ip)
		return false
	}
	m.queue = append(m.queue, queueTask{hash: hash, ip: ip})
	m.queueMu.Unlock()
	m.queueCond.Signal()
	return true
}

func (m *Manager) worker() {
	defer m.wg.Done()
	for {
		task, ok := m.dequeue()
		if !ok {
			return
		}
		_, _ = m.checkIP(task.hash, task.ip)
		m.pending.Delete(task.ip)
	}
}

func (m *Manager) dequeue() (queueTask, bool) {
	m.queueMu.Lock()
	defer m.queueMu.Unlock()
	for !m.stopped && m.queueHead >= len(m.queue) {
		m.queueCond.Wait()
	}
	if m.stopped {
		return queueTask{}, false
	}
	task := m.queue[m.queueHead]
	m.queueHead++
	if m.queueHead >= len(m.queue) {
		m.queue = nil
		m.queueHead = 0
	} else if m.queueHead > 1024 && m.queueHead*2 >= len(m.queue) {
		m.queue = append([]queueTask(nil), m.queue[m.queueHead:]...)
		m.queueHead = 0
	}
	return task, true
}

// CheckNow performs a synchronous check and updates all nodes sharing the
// selected node's current egress IP.
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
	ip := entry.GetEgressIP()
	if !ip.IsValid() {
		return node.GoogleAccessState{}, fmt.Errorf("node egress IP is unknown")
	}
	return m.checkIP(hash, ip)
}

func (m *Manager) checkIP(hash node.Hash, expectedIP netip.Addr) (node.GoogleAccessState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	resp, err := m.fetcher(ctx, hash, m.checkURL)
	state := Classify(resp, err, time.Now().UTC())

	entry, ok := m.pool.GetEntry(hash)
	if !ok || entry == nil || entry.GetEgressIP() != expectedIP {
		return node.GoogleAccessState{}, fmt.Errorf("node egress IP changed during Google check")
	}

	m.ipStates.Store(expectedIP, state)
	var affected []node.Hash
	m.pool.RangeNodes(func(nodeHash node.Hash, candidate *node.NodeEntry) bool {
		if candidate != nil && candidate.GetEgressIP() == expectedIP {
			candidate.SetGoogleAccessState(state)
			affected = append(affected, nodeHash)
		}
		return true
	})
	for _, nodeHash := range affected {
		m.pool.NotifyNodeDirty(nodeHash)
	}

	m.callbackMu.RLock()
	callback := m.onClassified
	m.callbackMu.RUnlock()
	if callback != nil {
		callback(expectedIP, state)
	}
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
