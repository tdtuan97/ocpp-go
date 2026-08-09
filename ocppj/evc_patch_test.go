package ocppj

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lorenzodonini/ocpp-go/ws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression tests for the EVC OCPP PATCH sites in the server dispatcher. All of them cover
// the same shape of bug: a blocking channel send performed while holding d.mutex, which turned
// a stalled message pump into a server that could no longer read from any connection.

// stalledDispatcher returns a dispatcher that believes it is running but whose message pump
// is not draining anything — the state the pump reaches once it deadlocks against itself.
func stalledDispatcher(t *testing.T) *DefaultServerDispatcher {
	t.Helper()
	d := NewDefaultServerDispatcher(NewFIFOQueueMap(0))
	d.mutex.Lock()
	d.requestChannel = make(chan string, 1)
	d.running = true
	d.mutex.Unlock()
	d.requestChannel <- "backlog" // full, and nobody reads it
	return d
}

// DeleteClient runs on the disconnecting socket's own goroutine. Parking here left the socket
// teardown unfinished forever while holding d.mutex as a reader, which is how a stalled pump
// spread into the read path of every other connection.
func TestDeleteClient_DoesNotBlockOnStalledPump(t *testing.T) {
	d := stalledDispatcher(t)
	d.queueMap.GetOrCreate("CP-1")

	done := make(chan struct{})
	go func() {
		defer close(done)
		d.DeleteClient("CP-1")
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("DeleteClient blocked, a disconnecting charge point can never finish tearing down")
	}
}

// stubWsServer satisfies ws.Server so SendRequest gets past its network check. Nothing here
// is exercised: the dispatcher never reaches a write in these tests.
type stubWsServer struct{}

func (stubWsServer) Start(int, string)                                            {}
func (stubWsServer) Stop()                                                        {}
func (stubWsServer) StopConnection(string, websocket.CloseError) error            { return nil }
func (stubWsServer) Errors() <-chan error                                         { return nil }
func (stubWsServer) SetMessageHandler(ws.MessageHandler)                          {}
func (stubWsServer) SetNewClientHandler(ws.ConnectedHandler)                      {}
func (stubWsServer) SetDisconnectedClientHandler(func(ws.Channel))                {}
func (stubWsServer) SetTimeoutConfig(ws.ServerTimeoutConfig)                      {}
func (stubWsServer) Write(string, []byte) error                                   { return nil }
func (stubWsServer) AddSupportedSubprotocol(string)                               {}
func (stubWsServer) SetChargePointIdResolver(func(*http.Request) (string, error)) {}
func (stubWsServer) SetBasicAuthHandler(func(string, string) bool)                {}
func (stubWsServer) SetCheckOriginHandler(func(*http.Request) bool)               {}
func (stubWsServer) SetCheckClientHandler(ws.CheckClientHandler)                  {}
func (stubWsServer) Addr() *net.TCPAddr                                           { return nil }
func (stubWsServer) GetChannel(string) (ws.Channel, bool)                         { return nil, false }

// SendRequest reaches the dispatcher through a process-wide callback lock, so one caller
// parked here stopped every charge point's requests and responses — not just this client's.
// It now gives up after dispatchSignalTimeout and reports the stall.
func TestSendRequest_ReportsStalledPumpInsteadOfBlocking(t *testing.T) {
	d := stalledDispatcher(t)
	d.SetNetworkServer(stubWsServer{})
	d.queueMap.GetOrCreate("CP-1")

	type result struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		err := d.SendRequest("CP-1", RequestBundle{Call: &Call{UniqueId: "req-1"}})
		done <- result{err: err, elapsed: time.Since(start)}
	}()

	select {
	case got := <-done:
		require.Error(t, got.err, "want the stalled pump reported, not a silent success")
		assert.GreaterOrEqual(t, got.elapsed, dispatchSignalTimeout, "want the caller to have waited for the pump before giving up")
	case <-time.After(dispatchSignalTimeout + 5*time.Second):
		t.Fatal("SendRequest blocked indefinitely on a stalled message pump")
	}
}

// The pump is the only reader of readyForDispatch, yet it calls CompleteRequest itself on a
// request timeout and on a failed write. At the old capacity of 1, a concurrent completion
// from an inbound CallResult could occupy the slot and leave the pump blocked on a channel
// nobody else drains — a deadlock against itself.
func TestCompleteRequest_DoesNotDeadlockAgainstThePump(t *testing.T) {
	d := NewDefaultServerDispatcher(NewFIFOQueueMap(0))
	d.mutex.Lock()
	d.running = true
	d.mutex.Unlock()

	// Fill the signal channel without any pump running to drain it.
	for len(d.readyForDispatch) < cap(d.readyForDispatch) {
		d.readyForDispatch <- "backlog"
	}

	// CompleteRequest bails out before the send when no queue exists, so give it one. The
	// completion then reaches the signal send, which is the line under test.
	q := d.queueMap.GetOrCreate("CP-1")
	call := &Call{UniqueId: "req-1"}
	require.NoError(t, q.Push(RequestBundle{Call: call}))

	done := make(chan struct{})
	go func() {
		defer close(done)
		d.CompleteRequest("CP-1", "req-1")
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("CompleteRequest blocked on the signal channel, the message pump can deadlock against itself")
	}
}

// pendingRequestState used to share d.mutex, and it takes a write lock even for reads. Since
// the server calls GetClientState for every inbound frame from every charge point, one
// goroutine parked while holding d.mutex as a reader stopped the server reading at all.
func TestPendingRequestState_UsesItsOwnMutex(t *testing.T) {
	d := NewDefaultServerDispatcher(NewFIFOQueueMap(0))

	// Hold the dispatch lock as a writer, the way a stalled dispatcher would.
	d.mutex.Lock()
	defer d.mutex.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		d.pendingRequestState.GetClientState("CP-1")
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reading pending request state is blocked by the dispatch lock; inbound messages would stall")
	}
}
