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
		outQueue:    make(chan message, outQueueSize),
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
func TestOnPing_SafeAgainstConcurrentCleanup(t *testing.T) {
	w := newStuckSocket(t, "stuck")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = w.onPing("data")
		}
	}()
	go func() {
		defer wg.Done()
		// Stand in for cleanup: closes the channels under the write lock.
		w.mutex.Lock()
		w.connection = nil
		close(w.outQueue)
		close(w.pingC)
		close(w.closeC)
		close(w.forceCloseC)
		w.mutex.Unlock()
	}()
	wg.Wait()
}
