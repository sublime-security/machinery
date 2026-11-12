package sqs

import (
	"crypto/tls"
	"net/http/httptrace"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/opentracing/opentracing-go"
)

// phaseTiming keeps constant-size state, even when dials overlap (for example,
// IPv4/IPv6 racing). Duration is the sum of completed, non-overlapping busy
// windows, not a sum of individual overlapping dials. Incomplete windows are
// counted as starts but contribute no duration.
type phaseTiming struct {
	active, starts, completed, errors int
	start                             time.Time
	elapsed                           time.Duration
}

func (p *phaseTiming) begin() {
	if p.active == 0 {
		p.start = time.Now()
	}
	p.active++
	p.starts++
}
func (p *phaseTiming) end() {
	if p.active == 0 {
		return
	}
	p.active--
	p.completed++
	if p.active == 0 {
		p.elapsed += time.Since(p.start)
	}
}

// Transport callbacks may arrive concurrently, including after cancellation.
// They only update this local accumulator, never a finished tracing span.
type publishTrace struct {
	mu                                       sync.Mutex
	phases                                   [4]phaseTiming
	sendStart                                time.Time
	sendElapsed                              time.Duration
	attempts, sendCompleted, retries, status int
	sdkCompleted                             bool
	gotConns, reused, idle, wroteRequests    int
	writeErrors                              int
	idleElapsed                              time.Duration
	firstByteStart                           time.Time
	firstBytes                               int
	firstByteElapsed                         time.Duration
}

const (
	phaseGetConn = iota
	phaseDNS
	phaseConnect
	phaseTLS
)

func (t *publishTrace) begin(phase int) { t.mu.Lock(); defer t.mu.Unlock(); t.phases[phase].begin() }
func (t *publishTrace) end(phase int, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.phases[phase].end()
	if err != nil {
		t.phases[phase].errors++
	}
}
func (t *publishTrace) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GetConn: func(string) { t.begin(phaseGetConn) },
		GotConn: func(info httptrace.GotConnInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.phases[phaseGetConn].end()
			t.gotConns++
			if info.Reused {
				t.reused++
			}
			if info.WasIdle {
				t.idle++
				t.idleElapsed += info.IdleTime
			}
		},
		DNSStart:          func(httptrace.DNSStartInfo) { t.begin(phaseDNS) },
		DNSDone:           func(info httptrace.DNSDoneInfo) { t.end(phaseDNS, info.Err) },
		ConnectStart:      func(string, string) { t.begin(phaseConnect) },
		ConnectDone:       func(_, _ string, err error) { t.end(phaseConnect, err) },
		TLSHandshakeStart: func() { t.begin(phaseTLS) },
		TLSHandshakeDone:  func(_ tls.ConnectionState, err error) { t.end(phaseTLS, err) },
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.wroteRequests++
			if info.Err != nil {
				t.writeErrors++
			}
		},
		GotFirstResponseByte: func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			if !t.firstByteStart.IsZero() {
				t.firstBytes++
				t.firstByteElapsed += time.Since(t.firstByteStart)
				t.firstByteStart = time.Time{}
			}
		},
	}
}
func (t *publishTrace) requestOption(r *request.Request) {
	r.Handlers.Send.PushFrontNamed(request.NamedHandler{Name: "machinery.sqs.send.start", Fn: func(*request.Request) {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.attempts++
		t.sendStart = time.Now()
		t.firstByteStart = t.sendStart
	}})
	r.Handlers.Send.PushBackNamed(request.NamedHandler{Name: "machinery.sqs.send.end", Fn: func(*request.Request) {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.sendCompleted++
		t.sendElapsed += time.Since(t.sendStart)
	}})
	r.Handlers.CompleteAttempt.PushBackNamed(request.NamedHandler{Name: "machinery.sqs.attempt.complete", Fn: func(r *request.Request) {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.status = 0
		if r.HTTPResponse != nil {
			t.status = r.HTTPResponse.StatusCode
		}
	}})
	r.Handlers.Complete.PushBackNamed(request.NamedHandler{Name: "machinery.sqs.complete", Fn: func(r *request.Request) {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.sdkCompleted = true
		t.retries = r.RetryCount
	}})
}
func milliseconds(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
func (t *publishTrace) finish(span opentracing.Span) {
	t.mu.Lock()
	defer t.mu.Unlock()
	span.SetTag("sqs.sdk_send_attempts", t.attempts)
	if t.sdkCompleted {
		span.SetTag("sqs.sdk_retries", t.retries)
	}
	span.SetTag("sqs.send_handlers_completed", t.sendCompleted)
	if t.sendCompleted > 0 && t.sendCompleted == t.attempts {
		span.SetTag("sqs.send_handlers_sum_ms", milliseconds(t.sendElapsed))
	}
	if t.status != 0 {
		span.SetTag("http.status_code", t.status)
	}
	for i, name := range []string{"get_conn", "dns", "connect", "tls"} {
		p := t.phases[i]
		span.SetTag("sqs.http."+name+".started", p.starts)
		span.SetTag("sqs.http."+name+".completed", p.completed)
		span.SetTag("sqs.http."+name+".errors", p.errors)
		if p.completed > 0 && p.active == 0 {
			span.SetTag("sqs.http."+name+".sum_ms", milliseconds(p.elapsed))
		}
	}
	span.SetTag("sqs.http.got_connections", t.gotConns)
	span.SetTag("sqs.http.reused_connections", t.reused)
	span.SetTag("sqs.http.idle_connections", t.idle)
	if t.idle > 0 {
		span.SetTag("sqs.http.idle_sum_ms", milliseconds(t.idleElapsed))
	}
	span.SetTag("sqs.http.wrote_requests", t.wroteRequests)
	span.SetTag("sqs.http.write_errors", t.writeErrors)
	span.SetTag("sqs.http.first_bytes", t.firstBytes)
	if t.firstBytes > 0 {
		span.SetTag("sqs.http.send_to_first_byte_sum_ms", milliseconds(t.firstByteElapsed))
	}
}

// Only retain a bounded code, never an SDK error message (which can include the
// endpoint or request body). Invalid/custom codes are deliberately redacted.
func boundedAWSErrorCode(code string) string {
	if len(code) == 0 || len(code) > 128 {
		return "other"
	}
	for _, c := range code {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return "other"
		}
	}
	return code
}
