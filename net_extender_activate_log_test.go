package connect

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// What the activator logs when an activation post fails (connect#229).
//
// A post whose every attempt failed ends in an exhausted request, whose own
// message is the fixed "http request attempts exhausted": logged alone, it
// hides which candidate failed and how. The log line names the cause of each
// attempt after it, and never the jwt the post carried.
//
// The post runs through a real direct strategy, as the role builds one, whose
// tcp dial and udp socket each fail at once with their own cause, so the
// normal dialer and the two alt dialers fail on every attempt. It runs in a
// synctest bubble: the request budget ends in bubble time once every attempt
// has failed, with no wait and no network.

// Records every line the activator logs.
type testActivateLogger struct {
	stateLock sync.Mutex
	lines     []string
}

func (self *testActivateLogger) add(line string) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.lines = append(self.lines, line)
}

func (self *testActivateLogger) Info(args ...any) {
	self.add(fmt.Sprint(args...))
}

func (self *testActivateLogger) Infof(format string, args ...any) {
	self.add(fmt.Sprintf(format, args...))
}

func (self *testActivateLogger) Warningf(format string, args ...any) {
	self.add(fmt.Sprintf(format, args...))
}

func (self *testActivateLogger) Errorf(format string, args ...any) {
	self.add(fmt.Sprintf(format, args...))
}

func (self *testActivateLogger) V(level int32) Verbose {
	return NewNoopLogger().V(level)
}

// The logged lines that start with `prefix`.
func (self *testActivateLogger) linesWithPrefix(prefix string) []string {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	lines := []string{}
	for _, line := range self.lines {
		if strings.HasPrefix(line, prefix) {
			lines = append(lines, line)
		}
	}
	return lines
}

func TestExtenderActivatorLogsTheCauseOfEveryAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tcpCause := errors.New("test tcp dial refused")
		udpCause := errors.New("test udp socket refused")
		const byJwt = "test-activation-jwt"

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		strategySettings := DefaultClientStrategySettings()
		strategySettings.Log = NewNoopLogger()
		strategySettings.ConnectSettings.Log = NewNoopLogger()
		// the normal dialer and the two alt dialers: one attempt each per round
		strategySettings.EnableResilient = false
		// an ip literal, so the alt attempts resolve nothing
		strategySettings.AltUrl = "https://192.0.2.10"
		strategySettings.ConnectSettings.DialContextSettings = &DialContextSettings{
			DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
				return nil, tcpCause
			},
			PacketConnFactory: func(ctx context.Context) (net.PacketConn, error) {
				return nil, udpCause
			},
		}
		clientStrategy := NewDirectClientStrategy(ctx, strategySettings, 0)
		defer clientStrategy.Close()

		log := &testActivateLogger{}
		settings := DefaultExtenderActivatorSettings()
		settings.Log = log
		settings.ApiUrlV4 = "https://192.0.2.4"
		settings.ByJwt = func() string {
			return byJwt
		}
		settings.ClientStrategy = clientStrategy
		settings.PublicKey = []byte("test extender public key")
		settings.Carriers = func() []string {
			return []string{ExtenderCarrierTcp}
		}
		settings.IpVersionSupported = func(ipVersion int) bool {
			return true
		}
		settings.AddNetworkChangeListener = func(listener func()) func() {
			return func() {}
		}
		// the bubble clock never sleeps, so there is no resume to watch
		settings.ResumeCheckTimeout = 0
		activator := NewExtenderActivator(ctx, settings)
		defer activator.Close()

		// the first recorded outcome is the failed post of the first pass
		for {
			version, change := activator.ChangeMonitor().Get()
			if 0 < version {
				break
			}
			<-change
		}
		status := activator.Status().Family(4)
		if status == nil || status.Activated || status.LastError == "" {
			t.Fatalf("v4 status = %+v, expected a failed activation", status)
		}

		lines := log.linesWithPrefix("[extender] activate v4 failed ")
		if len(lines) == 0 {
			t.Fatal("the failed activation logged no error")
		}
		line := lines[0]
		t.Logf("activation error log: %s", line)
		for _, expected := range []string{
			(&HttpRequestExhaustedError{}).Error(),
			tcpCause.Error(),
			udpCause.Error(),
			"carriers=tcp", "api=192.0.2.4", "elapsed=", "attempts=",
		} {
			if !strings.Contains(line, expected) {
				t.Errorf("the activation error log %q does not name %q", line, expected)
			}
		}
		if strings.Contains(line, byJwt) {
			t.Errorf("the activation error log %q carries the jwt", line)
		}
	})
}

func TestActivationLogTextRedactsCredentialsAndNewlines(t *testing.T) {
	token := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzZWNyZXQifQ.signatureSecret123"
	text := activationLogText("bad\nhttps://user:pass@example.org?token=hidden "+token+" test-jwt", "test-jwt", "https://user:pass@example.org?token=hidden")
	for _, secret := range []string{token, "test-jwt", "pass", "hidden", "\n"} {
		if strings.Contains(text, secret) {
			t.Fatalf("secret or newline in log: %q", text)
		}
	}
	if !strings.Contains(text, "example.org") {
		t.Fatal(text)
	}
}

// http.Client.Do masks the password in the url of its error and appends the
// request path, so the text no longer contains the configured api url (js/wasm
// returns that error as is).
func TestActivationLogTextSanitizesRedactedPathAppendedUrl(t *testing.T) {
	text := activationLogText(
		`http request failed: Post "https://user:xxxxx@example.org/v1/extender/activate?token=hidden#frag": EOF`,
		"test-jwt",
		"https://user:pass@example.org?token=hidden",
	)
	for _, secret := range []string{"user", "xxxxx", "hidden", "frag", "/v1/extender"} {
		if strings.Contains(text, secret) {
			t.Fatalf("%q leaked into log: %q", secret, text)
		}
	}
	if !strings.Contains(text, "example.org") || !strings.Contains(text, "EOF") {
		t.Fatalf("log lost the host or the cause: %q", text)
	}
}

func TestExtenderActivationRefusalLog(t *testing.T) {
	const jwt = "sensitive-test-jwt"
	operator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/hello" {
			fmt.Fprint(w, `{"client_address":"127.0.0.1"}`)
			return
		}
		fmt.Fprint(w, `{"activated":false,"ip_version":4,"error":"quic carrier did not answer: sensitive-test-jwt"}`)
	}))
	defer operator.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	strategySettings := DefaultClientStrategySettings()
	strategySettings.Log = NewNoopLogger()
	strategySettings.ConnectSettings.Log = NewNoopLogger()
	strategySettings.EnableResilient = false
	strategy := NewDirectClientStrategy(ctx, strategySettings, 0)
	defer strategy.Close()
	logger := &testActivateLogger{}
	settings := DefaultExtenderActivatorSettings()
	settings.Log = logger
	settings.ApiUrlV4 = operator.URL
	settings.ByJwt = func() string { return jwt }
	settings.ClientStrategy = strategy
	settings.Carriers = func() []string { return []string{ExtenderCarrierQuic} }
	settings.IpVersionSupported = func(int) bool { return true }
	settings.AddNetworkChangeListener = func(func()) func() { return func() {} }
	activator := NewExtenderActivator(ctx, settings)
	defer activator.Close()
	for {
		version, changed := activator.ChangeMonitor().Get()
		if version > 0 {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	lines := logger.linesWithPrefix("[extender] activate v4 refused ")
	if len(lines) != 1 {
		t.Fatalf("refusal lines: %v", lines)
	}
	line := lines[0]
	for _, want := range []string{"carriers=quic", "api=127.0.0.1:", "last_remote=127.0.0.1:", "elapsed=", "attempts=", "quic carrier did not answer", "[redacted]"} {
		if !strings.Contains(line, want) {
			t.Errorf("missing %q from %q", want, line)
		}
	}
	if strings.Contains(line, jwt) {
		t.Fatal("jwt leaked")
	}
	// the recorded failure carries the same operator text as the log, so it
	// must be sanitized too (Status().Family(4).LastError is what a diagnostic
	// reader sees later)
	if family := activator.Status().Family(4); family != nil && strings.Contains(family.LastError, jwt) {
		t.Fatalf("jwt leaked into the recorded failure: %q", family.LastError)
	}
	t.Logf("refusal log: %s", line)
}

// The literal apiURL replacement matches a bare substring. If it runs before
// the url matcher, "<apiURL>?code=secret" becomes "<host>?code=secret": the
// scheme is gone, the matcher no longer sees a url, and the query survives.
// This is the ordering the activation path had before the url strip moved
// ahead of it.
func TestActivationLogTextStripsUrlBeforeLiteralApiUrl(t *testing.T) {
	apiURL := "https://api.example.org/v1/extender/activate"
	text := activationLogText(
		`http request failed: Post "`+apiURL+`?code=query-secret#frag": EOF`,
		"test-jwt",
		apiURL,
	)
	for _, secret := range []string{"query-secret", "frag", "?"} {
		if strings.Contains(text, secret) {
			t.Fatalf("url component %q survived sanitization: %q", secret, text)
		}
	}
	if !strings.Contains(text, "api.example.org") {
		t.Fatalf("the host should survive: %q", text)
	}
}

// activationLogError is the no-jwt sanitizer used on the hello path.
func TestActivationLogErrorStripsUrlAndJwtShape(t *testing.T) {
	token := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ4eXoifQ.c2lnbmF0dXJlLXZhbHVl"
	text := activationLogError(`Post "https://user:pass@example.org/hello?token=hidden": ` + token)
	for _, secret := range []string{"hidden", "pass", token} {
		if strings.Contains(text, secret) {
			t.Fatalf("secret %q survived: %q", secret, text)
		}
	}
}

// A quote inside a query value used to truncate the url match, and the tail
// after the quote escaped the strip.
func TestActivationLogTextHandlesQuoteInsideQuery(t *testing.T) {
	// the quote sits INSIDE the query value, so a matcher that stops at a quote
	// truncates the url and leaves the rest of the secret in the text
	text := activationLogText(
		`http request failed: Post "https://api.example.org/a?code=quo"ted-secret": EOF`,
		"test-jwt",
		"https://api.example.org/a",
	)
	for _, fragment := range []string{"quo", "ted-secret", "code="} {
		if strings.Contains(text, fragment) {
			t.Fatalf("query fragment %q survived: %q", fragment, text)
		}
	}
	if !strings.Contains(text, "api.example.org") {
		t.Fatalf("host should survive: %q", text)
	}
}

// A scheme-less host carrying a query is a leftover; it must not survive.
func TestActivationLogTextStripsSchemalessQuery(t *testing.T) {
	text := activationLogText("left api.example.org/a?code=scheme-less-secret behind", "test-jwt", "")
	if strings.Contains(text, "scheme-less-secret") {
		t.Fatalf("scheme-less query survived: %q", text)
	}
}

// The refusal message is operator-supplied text and is persisted, so it needs
// the same treatment as a transport error.
func TestActivationLogTextSanitizesRefusalMessage(t *testing.T) {
	text := activationLogText("refused: https://api.example.org/activate?token=refusal-secret", "test-jwt", "")
	for _, secret := range []string{"refusal-secret", "token="} {
		if strings.Contains(text, secret) {
			t.Fatalf("%q survived in refusal text: %q", secret, text)
		}
	}
}

// An IP-literal endpoint has no alpha TLD, so a matcher that required one let
// the query through.
func TestActivationLogTextStripsSchemalessIpLiteral(t *testing.T) {
	for _, text := range []string{
		"left 1.2.3.4?code=ip-secret behind",
		"left [::1]?code=ip-secret behind",
	} {
		got := activationLogText(text, "", "")
		if strings.Contains(got, "ip-secret") {
			t.Fatalf("query survived for %q: %q", text, got)
		}
	}
}

// A probe error with no cause must render, not panic: this is a logging path.
func TestExtenderProbeErrorWithoutCause(t *testing.T) {
	e := &ExtenderProbeError{Carrier: "tcp", Port: 443, Phase: "dial"}
	got := e.Error()
	if !strings.Contains(got, "<nil>") || !strings.Contains(got, "carrier=tcp") {
		t.Fatalf("unexpected rendering: %q", got)
	}
}
