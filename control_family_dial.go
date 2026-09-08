package connect

import (
	"context"
	"net"
	"time"
)

// dialControlTlsWithFamilyFallback performs a control-plane dial and its
// handshake, and retries ONCE over the other address family when the handshake
// fails with a timeout of its own after the connect succeeded.
//
// That sequence -- connect succeeds, handshake times out -- is the signature of
// a path that carries small packets and drops large ones, which is what a
// tunnel with a reduced MTU and filtered ICMP Packet-Too-Big does. Happy
// Eyeballs cannot see it: it races only the tcp handshake, so the broken family
// WINS the race and then stalls.
//
// A timeout that arrives with the caller's own budget already spent is NOT
// counted: that is a request running out of time, which says nothing about
// the family, and there would be no time to retry either way. What remains to
// trigger a demotion is a handshake that hits its OWN timeout while the caller
// still has budget left, which is also the only case where a retry has
// anywhere to run.
//
// WHAT THAT OWN TIMEOUT IS, and why it is not TlsTimeout. Both production
// entry points hand this helper a caller deadline at or below TlsTimeout:
// http.Client.Timeout is RequestTimeout (15s, and it starts before the dial
// does) on the api path, and gorilla/websocket caps its dial context at
// Dialer.HandshakeTimeout (5s) on the platform control websocket. A handshake
// bounded only by TlsTimeout therefore never reaches its own timeout -- the
// caller's deadline always arrives first, the branch above declines the
// strike, and the retry never runs at all.
//
// So the first handshake is bounded by ControlFamilyFirstHandshakeTimeout, and
// THAT is the timeout a stalled handshake hits. It is a floor, not a fraction:
// an earlier version halved whatever the caller had left, which shortens the
// tls handshake tolerance this product deliberately does not shorten -- doing
// so risks false-positive demotion for users on genuinely slow links -- and
// did it hardest exactly where the budget was smallest -- 2.5s on the control
// websocket, which a congested mobile link reaches with a pinned P-384 chain
// and nothing wrong. A fixed 8s cannot scale down like that, and it is larger
// than the entire budget in which a shipping platform websocket dial already
// completes a connect, this handshake and an http upgrade.
//
// THE BOUND IS APPLIED ONLY WHERE TWO ATTEMPTS FIT: the caller must still have
// ControlFamilyFirstHandshakeTimeout + ControlFamilyRetryReserve left when the
// handshake starts. A bound that produces a timeout with no room to retry is
// strictly worse than no bound -- it converts a request that would have kept
// waiting into one that fails early and learns nothing. Below that threshold
// the first handshake keeps the caller's whole remaining budget and this
// helper behaves exactly as it did before the bound existed.
//
// That threshold is what decides, per attempt, whether this helper bounds
// anything. An api attempt that arrives with the whole RequestTimeout (~15s)
// is bounded -- which is every attempt the client strategy runs in parallel,
// including the parallel hello a strategy with no successful dialer yet falls
// straight through to, so a cold launch is bounded. The strategy's SERIAL
// attempts are not: parallelEval and serialEval give each remembered dialer an
// equal share of what is left (preferredEvalAttemptContext, net_http.go), and
// a share of 15s is under 8s + 5s, so the same "no room for two attempts"
// rule declines to bound them. Nor is the platform control websocket, which
// gorilla caps at HandshakeTimeout (5s).
//
// None of those paths needs a retry of its own: the demotion ledger is
// process-global (control_family.go), read inside every dial through
// controlDialNetwork and pickControlIPAddr, so a demotion learned on any
// bounded attempt is already in force for the control websocket, the h3/quic
// name path and the extenders. One attempt with enough budget is enough to
// LEARN; every path benefits. Raising the websocket's HandshakeTimeout
// instead would change a shared transport timeout for every user to buy a
// second attempt on a path that already inherits the answer.
//
// Exactly one retry, and only to the other family. The caller already sits
// inside the client strategy's serial and parallel dialer evaluation under a
// shared request budget, so a helper that retried repeatedly could consume the
// whole budget alone and starve the other dialers -- which is the failure this
// exists to prevent, not to reproduce. A second failure over the second family
// is also not a family problem, and the original error is returned unwrapped.
//
// CONNECT-TIME FALLBACK: dialControlTlsWithFamilyFallback's original retry
// (redialWithoutAContradictedDemotion, below) only fires when a live demotion
// is on record AND was itself contradicted -- i.e. when the family was already
// narrowed by a previous handshake-timeout strike. A hard connect-time failure
// on the FIRST attempt over a family-agnostic network ("tcp"/"udp") -- the
// signature of a blackholed path where sendto returns "network is unreachable"
// before any packet leaves the socket -- leaves no connection and no demotion,
// so the original code returned the error immediately with no retry over the
// other family at all. That is the AT&T-tunnel pattern: the kernel has an IPv6
// route but the tunnel swallows v6 traffic, so the v6 dial is rejected at
// connect time and the request hangs until the caller's own RequestTimeout
// (15s) fires, never trying IPv4.
//
// The fallback here closes exactly that gap: when the initial connect fails on
// a family-agnostic network with a timeout OR a network-unreachable-class
// error, retry ONCE over the explicit other family, then run the normal
// handshake on whatever that yields -- a connect success returns the conn for
// the handshake to complete, a second connect failure returns the original
// error unwrapped. Exactly one retry, gated on the same budget the
// handshake-timeout retry uses, so this helper cannot starve the caller's
// own parallel/strided dialer budget.
func dialControlTlsWithFamilyFallback(
	ctx context.Context,
	settings *ConnectSettings,
	network string,
	addr string,
	dial DialContextFunction,
	handshake func(ctx context.Context, conn net.Conn) (net.Conn, error),
) (net.Conn, error) {
	conn, err := dial(ctx, network, addr)
	if err != nil {
		conn, err = redialWithoutAContradictedDemotion(ctx, network, addr, dial, err)
		if err != nil {
			// No connection survived the first connect. If this was a
			// family-agnostic dial of a name (not a literal IP, not a
			// forced policy) and the failure is a connect-time blackhole
			// (sendto: network is unreachable, or a connect timeout), try
			// the other family once before giving up -- this is the
			// connect-time blackhole recovery, distinct from the
			// handshake-timeout demotion below.
			conn, err = connectTimeFamilyFallback(ctx, settings, network, addr, dial, err)
			if err != nil {
				return nil, err
			}
		}
	}
	// BEFORE the handshake, and before any Close: a closed net.TCPConn is not
	// required to keep answering RemoteAddr, and the family of the connection
	// we are about to lose is the whole point of the exercise.
	failed := connFamily(conn)

	// The bound goes on the handshake and NOT on the connect. The connect has
	// its own budget (ConnectTimeout) and its own second chance
	// (redialWithoutAContradictedDemotion, below), which a shortened context
	// would leave with a dead deadline to dial on. Measuring what is left here
	// rather than before the dial is also the honest reading: it is the budget
	// the retry will actually inherit.
	handshakeCtx, handshakeCancel := firstHandshakeContext(ctx, settings)
	tlsConn, err := handshake(handshakeCtx, conn)
	handshakeCancel()
	if err == nil {
		return tlsConn, nil
	}
	conn.Close()

	// only a family-agnostic dial has somewhere else to go
	if network != "tcp" && network != "udp" {
		return nil, err
	}
	// an IP literal fixes its own family. There is no other family to retry
	// onto -- `dial tcp6 1.1.1.1:443` is "no suitable address found" -- and no
	// name resolution whose family choice a strike could inform.
	// controlDialNetwork leaves those dials alone for the same reason.
	if isIPLiteralDialAddr(addr) {
		return nil, err
	}
	if !isPathTimeout(err) {
		return nil, err
	}
	if failed == 0 {
		return nil, err
	}
	// the timeout has to be the handshake's own -- the bound above, or
	// TlsTimeout when there was no room to apply one. The CALLER's context is
	// what is tested, never the bounded one: a caller whose budget is gone
	// gets no strike and no retry, because the strike records what this helper
	// is about to test and it cannot test anything with no time left.
	if ctx.Err() != nil {
		return nil, err
	}
	if !controlFamilyDemote(failed) {
		// refused: the other family is not usable on this device, so there is
		// nothing to retry onto
		return nil, err
	}

	retryNetwork := network + "4"
	if failed == 4 {
		retryNetwork = network + "6"
	}
	retryConn, retryErr := dial(ctx, retryNetwork, addr)
	if retryErr != nil {
		// The other family could not even connect, so the evidence does not
		// say "this family is blackholed", it says "this moment is bad": a
		// second failure over the second family is not a family problem.
		// Leaving the strike standing would narrow every control dial in the
		// process onto a family that just failed outright.
		controlFamilyUndemote(failed)
		return nil, err
	}
	retryTlsConn, retryErr := handshake(ctx, retryConn)
	if retryErr != nil {
		retryConn.Close()
		controlFamilyUndemote(failed)
		return nil, err
	}
	return retryTlsConn, nil
}

// connectTimeFamilyFallback retries a FAILED CONNECT (no connection ever
// established) over the other address family, exactly once. It is the
// connect-time-blackhole recovery: a path where sendto fails at the kernel
// routing step ("network is unreachable") before any packet is sent, so the
// failure is reported by the dial itself rather than by a handshake timeout.
//
// This is distinct from redialWithoutAContradictedDemotion (which recovers a
// demotion that narrowed a dial onto a family with no route) and from the
// handshake-timeout retry in dialControlTlsWithFamilyFallback (which runs only
// after a connect succeeded and the TLS handshake then stalled). Neither of
// those covers a clean connect-time failure on the very first attempt: that
// path returns the error immediately, and on a dual-stack device where v6 is
// blackholed and v4 works, the request never tries v4 at all.
//
// The retry dials explicit tcp4. The design already prefers IPv4 (see
// pickControlIPAddr), and the field-reported failure is IPv6-blackholed, so
// the other family is IPv4 in the case this exists to fix. Dialing the
// explicit family also sidesteps Go's RFC-6724 ordering -- net.Dialer with a
// family-agnostic "tcp" races/sticks with v6-first on a dual-stack device --
// so the retry cannot silently win the v6 race again.
//
// The gate is intentionally narrow and mirrors the handshake-timeout retry's
// constraints so it cannot expand its callers' budget consumption:
//   - network is family-agnostic ("tcp"/"udp"), not a forced literal family
//   - addr is not an IP literal (a literal fixes its own family)
//   - the policy is IpFamilyAuto (a force is a developer override and is obeyed)
//   - the caller still has budget left for one more connect attempt
//   - firstErr is a connect-time condition: isConnectNetworkUnreachable
//
// It returns the connection from the other-family connect (nil err) for the
// caller to proceed to handshake, or (nil, firstErr) when the guard declines or
// the retry itself fails -- so the original first-attempt error is always what
// the caller surfaces.
func connectTimeFamilyFallback(
	ctx context.Context,
	settings *ConnectSettings,
	network string,
	addr string,
	dial DialContextFunction,
	firstErr error,
) (net.Conn, error) {
	if network != "tcp" && network != "udp" {
		return nil, firstErr
	}
	if isIPLiteralDialAddr(addr) {
		return nil, firstErr
	}
	if ControlIpFamilyPolicy() != IpFamilyAuto {
		return nil, firstErr
	}
	if !isConnectNetworkUnreachable(firstErr) {
		return nil, firstErr
	}
	// the caller must still have room for one more connect before its budget
	// runs out; reuse the dial's own ConnectTimeout as the floor so the retry's
	// cost is charged against the same budget, not an invented one.
	budget := defaultConnectTimeout
	if settings != nil && settings.ConnectTimeout > 0 {
		budget = settings.ConnectTimeout
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < budget {
		return nil, firstErr
	}
	// probe that the other family is even usable on this device before burning
	// a connect on it; a v6-only path that has no v4 at all must not be made
	// to pay for an extra dial that is guaranteed to fail.
	if !controlFamilyUsable(4) {
		return nil, firstErr
	}
	retryNetwork := network + "4"
	retryConn, retryErr := dial(ctx, retryNetwork, addr)
	if retryErr != nil {
		// the other family failed too: not a family problem. surface the
		// ORIGINAL first-attempt error, unwrapped.
		return nil, firstErr
	}
	return retryConn, nil
}
//
// A context with NO deadline is bounded: an unbounded caller has room for two
// attempts by definition, and leaving it unbounded is the one shape where a
// stalled handshake would hang until the kernel gave up, which is minutes.
func firstHandshakeContext(
	ctx context.Context,
	settings *ConnectSettings,
) (context.Context, context.CancelFunc) {
	noBound := func() (context.Context, context.CancelFunc) {
		return ctx, func() {}
	}
	if settings == nil {
		return noBound()
	}
	bound := settings.ControlFamilyFirstHandshakeTimeout
	reserve := settings.ControlFamilyRetryReserve
	if bound <= 0 || reserve <= 0 {
		return noBound()
	}
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) < bound+reserve {
			return noBound()
		}
	}
	return context.WithTimeout(ctx, bound)
}

// redialWithoutAContradictedDemotion is the only route back from a demotion
// that took the user offline.
//
// The ledger is only ever WRITTEN after a connect succeeded and a handshake
// then timed out, so a demotion can be confirmed but never refuted: when the
// family we demoted ONTO cannot connect at all, the dial fails at the connect
// step and the ledger never hears about it. Until the entry expires -- five
// minutes, doubling to a six hour cap -- every control dial in the process
// keeps being steered onto the family that just proved it does not work.
//
// A connect failure over a network a live demotion was narrowing is exactly
// that refutation. It clears the entry and dials once more with the caller's
// original family-agnostic network, which also restores the Happy Eyeballs
// race that the narrowing had switched off -- the platform's own answer to a
// PRE-connect blackhole, which already works.
//
// A FORCE is never undone here. It is an explicit developer override whose
// entire purpose is to be obeyed against this client's judgement.
func redialWithoutAContradictedDemotion(
	ctx context.Context,
	network string,
	addr string,
	dial DialContextFunction,
	dialErr error,
) (net.Conn, error) {
	if network != "tcp" && network != "udp" {
		return nil, dialErr
	}
	// a literal is never narrowed, so its failure is not the narrowing's fault
	if isIPLiteralDialAddr(addr) {
		return nil, dialErr
	}
	if ControlIpFamilyPolicy() != IpFamilyAuto {
		return nil, dialErr
	}
	demoted := controlFamilyDemotedFamily()
	if demoted == 0 {
		return nil, dialErr
	}
	if !controlFamilyUndemote(demoted) {
		return nil, dialErr
	}
	conn, err := dial(ctx, network, addr)
	if err != nil {
		return nil, dialErr
	}
	return conn, nil
}
