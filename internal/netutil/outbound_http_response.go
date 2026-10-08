package netutil

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

// OutboundHTTPResponse contains response metadata needed by classifiers that
// must inspect redirects or status codes rather than only the response body.
type OutboundHTTPResponse struct {
	StatusCode int
	Header     http.Header
	Body       []byte
	Latency    time.Duration
}

// OutboundHTTPResponseOptions controls metadata-preserving outbound requests.
type OutboundHTTPResponseOptions struct {
	FollowRedirects bool
	MaxBodyBytes    int64
	UserAgent       string
	OnConnLifecycle func(op ConnLifecycleOp)
}

// HTTPGetResponseViaOutbound executes an HTTP GET through an outbound while
// retaining status and headers. Timeout and cancellation are controlled by ctx.
func HTTPGetResponseViaOutbound(
	ctx context.Context,
	outbound adapter.Outbound,
	targetURL string,
	opts OutboundHTTPResponseOptions,
) (*OutboundHTTPResponse, error) {
	if outbound == nil {
		return nil, fmt.Errorf("outbound fetch: outbound is nil")
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := outbound.DialContext(ctx, network, M.ParseSocksaddr(addr))
			if err != nil {
				return nil, err
			}
			if opts.OnConnLifecycle != nil {
				opts.OnConnLifecycle(ConnLifecycleOpen)
				return &connCloseHook{Conn: conn, onClose: func() { opts.OnConnLifecycle(ConnLifecycleClose) }}, nil
			}
			return conn, nil
		},
		DisableKeepAlives: true,
		ForceAttemptHTTP2: true,
	}

	client := &http.Client{Transport: transport}
	if !opts.FollowRedirects {
		client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, err
	}
	userAgent := opts.UserAgent
	if userAgent == "" {
		userAgent = defaultOutboundUserAgent
	}
	req.Header.Set("User-Agent", userAgent)

	var start time.Time
	var latency time.Duration
	trace := &httptrace.ClientTrace{
		TLSHandshakeStart: func() { start = time.Now() },
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			if err == nil {
				latency = time.Since(start)
			}
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(ctx, trace))

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	limit := opts.MaxBodyBytes
	if limit <= 0 {
		limit = 64 << 10
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return nil, err
	}

	return &OutboundHTTPResponse{
		StatusCode: resp.StatusCode,
		Header:     resp.Header.Clone(),
		Body:       body,
		Latency:    latency,
	}, nil
}
