package ws

import (
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression tests for the EVC OCPP PATCH sites. Each one reproduces a failure that was
// observed in production on evc-ocpp-worker, where a single unresponsive charge point took
// the whole central system down while the process kept answering health checks.

// newStuckSocket builds a webSocket whose send queue is full and whose writePump does not
// exist, i.e. a socket that has stopped draining — the state a charger reaches when its TCP
// connection is half-open and the peer has silently gone away.
func newStuckSocket(t *testing.T, id string) *webSocket {
	t.Helper()
	w := &webSocket{
		id: id,
		// Non-nil so WriteManual gets past its closed-connection check. Nothing in these tests
		// touches the wire: WriteManual only queues.
		connection:  &websocket.Conn{},
		outQueue:    make(chan message, 2), // same capacity newWebSocket uses
		pingC:       make(chan []byte, 1),
		closeC:      make(chan websocket.CloseError, 1),
		forceCloseC: make(chan error, 1),
		log:         log,
	}
	for len(w.outQueue) < cap(w.outQueue) {
		w.outQueue <- message{typ: websocket.TextMessage, data: []byte("backlog")}
	}
	return w
}

func newTestServer(sockets ...*webSocket) *server {
	s := &server{connections: map[string]*webSocket{}}
	for _, w := range sockets {
		s.connections[w.id] = w
	}
	return s
}

// The production failure, end to end: writing to a socket that has stopped draining used to
// park the writer while it held the connection-map lock as a reader. The next writer queued
// behind it and, because Go's RWMutex is writer-preferring, every later reader queued behind
// that — so the server could no longer look up, write to, or accept any connection, while
// staying otherwise perfectly healthy.
func TestWrite_StuckSocketDoesNotStarveConnectionMap(t *testing.T) {
	stuck := newStuckSocket(t, "stuck")
	other := newStuckSocket(t, "other")
	s := newTestServer(stuck, other)

	// A write to the wedged socket must report an error instead of waiting.
	writeDone := make(chan error, 1)
	go func() { writeDone <- s.Write("stuck", []byte("payload")) }()

	select {
	case err := <-writeDone:
		require.Error(t, err, "want an error once the send queue is full, not a blocked writer")
	case <-time.After(2 * time.Second):
		t.Fatal("Write blocked on a socket that stopped draining")
	}

	// A writer arriving next must not find the lock held, and readers after it must not be
	// starved. This is the exact sequence that froze the server.
	go s.handleDisconnect(other, nil)

	lookupDone := make(chan struct{})
	go func() {
		defer close(lookupDone)
		s.GetChannel("stuck")
	}()

	select {
	case <-lookupDone:
	case <-time.After(2 * time.Second):
		t.Fatal("connection-map readers are starved while a socket is wedged")
	}
}

// cleanup takes the socket's write lock. A writer parked on a full queue held that same lock
// as a reader, so teardown could never run and both goroutines leaked, permanently, together
// with the file descriptor.
func TestWriteManual_DoesNotBlockCleanup(t *testing.T) {
	w := newStuckSocket(t, "stuck")

	err := w.WriteManual(websocket.TextMessage, []byte("payload"))
	require.Error(t, err, "want a full send queue to be reported, not waited on")

	acquired := make(chan struct{})
	go func() {
		defer close(acquired)
		w.mutex.Lock()
		w.mutex.Unlock()
	}()

	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("the socket's write lock is unreachable, cleanup could never run")
	}
}

// closeC holds one slot. Kicking the same charge point twice from the operator page used to
// park the second caller forever while it held the socket's read lock, which wedged that
// socket for the lifetime of the pod.
func TestClose_RepeatedCallsDoNotBlock(t *testing.T) {
	w := newStuckSocket(t, "stuck")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 5; i++ {
			_ = w.Close(websocket.CloseError{Code: websocket.CloseNormalClosure})
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked when called more than once for the same socket")
	}
}

// A reconnecting charge point replaces its map entry while the previous socket is still
// tearing down on its own goroutine. Deleting by id alone removed the live replacement, so
// the charger was connected but unreachable — every write answered "No socket with id X is
// open" until it gave up and reconnected, starting the race again.
func TestHandleDisconnect_KeepsReplacementSocket(t *testing.T) {
	const cpID = "CP-1"
	old := newStuckSocket(t, cpID)
	replacement := newStuckSocket(t, cpID)

	s := newTestServer(replacement) // the reconnect already swapped the entry
	s.handleDisconnect(old, nil)

	got, ok := s.GetChannel(cpID)
	require.True(t, ok, "the live replacement was deleted by the old socket's teardown")
	assert.Same(t, replacement, got)
}

// The same teardown must still clean up when it is the current socket, otherwise entries
// accumulate for chargers that are long gone.
func TestHandleDisconnect_RemovesItsOwnSocket(t *testing.T) {
	const cpID = "CP-1"
	w := newStuckSocket(t, cpID)

	s := newTestServer(w)
	s.handleDisconnect(w, nil)

	_, ok := s.GetChannel(cpID)
	assert.False(t, ok, "the socket that disconnected should be gone from the map")
}

// onPing sends on a channel that cleanup closes under the write lock. Without holding the
// read lock the send could land on a closed channel, which panics — taking down every
// charger on the pod, not just this one.
//
// This one needs a genuine connection: onPing ends in conn.SetReadDeadline, which a
// hand-built websocket.Conn cannot serve. The socket therefore comes from a real handshake,
// and the teardown is the real cleanup rather than an imitation of it.
func TestOnPing_SafeAgainstConcurrentCleanup(t *testing.T) {
	const port = 18993

	s := NewServer().(*server)
	s.AddSupportedSubprotocol(defaultSubProtocol)
	s.SetMessageHandler(func(Channel, []byte) error { return nil })
	connected := make(chan string, 2)
	s.SetNewClientHandler(func(c Channel) { connected <- c.ID() })

	go s.Start(port, serverPath)
	defer s.Stop()

	client := dialClientAt(t, port, "CP-PING")
	defer client.Close()
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("server never registered the client")
	}

	s.connMutex.RLock()
	w := s.connections["CP-PING"]
	s.connMutex.RUnlock()
	require.NotNil(t, w)

	// Drive the teardown the way it actually happens — the peer goes away and writePump runs
	// cleanup once — rather than calling cleanup directly, which no production path does and
	// which would enter it a second time behind writePump's back.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			_ = w.onPing("data")
		}
	}()

	time.Sleep(100 * time.Millisecond)
	_ = client.Close()
	wg.Wait()
}

// A charge point on a flaky link redials the same pod before its old socket has finished
// tearing down. wsHandler replaces the map entry and invokes the disconnect callback inline
// for the old socket; that old socket's own writePump then reaches handleDisconnect a moment
// later, and before the identity gate it fired the callback a SECOND time.
//
// That second call is the damaging one. Everything downstream of disconnectedHandler is keyed
// by ID() alone (ocppj DeleteClient / ClearClientPendingRequest, and the 1.6 callbackQueue
// drain), so it tore down the dispatcher state of the replacement socket that now owns the ID.
// The charger stayed in the connection map reporting healthy, while every command to it failed
// with "no client %s exists" until it reconnected again.
//
// The old socket's writePump can sit in conn.WriteMessage for up to WriteWait against a
// half-open peer, so this interleaving is the expected one for a flaky link, not a rare race.
func TestHandleDisconnect_SupersededSocketFiresCallbackOnce(t *testing.T) {
	const port = 18994
	const cpID = "CP-DUP"

	s := NewServer().(*server)
	s.AddSupportedSubprotocol(defaultSubProtocol)
	s.SetMessageHandler(func(Channel, []byte) error { return nil })

	connected := make(chan Channel, 2)
	s.SetNewClientHandler(func(c Channel) { connected <- c })

	var mu sync.Mutex
	calls := map[Channel]int{}
	s.SetDisconnectedClientHandler(func(c Channel) {
		mu.Lock()
		calls[c]++
		mu.Unlock()
	})

	go s.Start(port, serverPath)
	defer s.Stop()

	clientA := dialClientAt(t, port, cpID)
	defer clientA.Close()
	var chA Channel
	select {
	case chA = <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("server never registered the first client")
	}

	// Same ID, so wsHandler takes the replace branch while the first socket is still live.
	clientB := dialClientAt(t, port, cpID)
	defer clientB.Close()
	var chB Channel
	select {
	case chB = <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("server never registered the replacement client")
	}
	require.NotSame(t, chA, chB, "replacement must be a distinct socket")

	// Wait for the superseded socket to finish its own teardown: cleanup() nils the
	// connection under the write lock, and handleDisconnect runs immediately after.
	wA := chA.(*webSocket)
	deadline := time.Now().Add(5 * time.Second)
	for {
		wA.mutex.RLock()
		torndown := wA.connection == nil
		wA.mutex.RUnlock()
		if torndown {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("superseded socket never finished cleanup")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// handleDisconnect runs just past the nil assignment; give it room to land.
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	callsA, callsB := calls[chA], calls[chB]
	mu.Unlock()

	assert.Equal(t, 1, callsA, "superseded socket must notify exactly once, not once per teardown path")
	assert.Equal(t, 0, callsB, "replacement socket must never be reported as disconnected")

	live, ok := s.GetChannel(cpID)
	require.True(t, ok, "replacement socket must still be registered")
	assert.Same(t, chB, live, "connection map must still hold the replacement")
}
