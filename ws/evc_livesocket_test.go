package ws

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// End-to-end reproduction of the production incident, over real TCP rather than fabricated
// structs. A charge point that stops reading — the state a charger on a flaky mobile link
// reaches when it disappears without sending FIN or RST — must not be able to freeze the
// server for every other charger.
//
// This is the shape of the failure that took evc-ocpp-worker down: the pod kept accepting
// TCP, kept answering health checks, kept writing to the database, and could not complete a
// single WebSocket handshake or deliver a single response for the remaining hours of its life.

const livePort = 18987

// dialSilentClient completes the WebSocket handshake and then never reads. The kernel
// receive buffer fills, the server's writes stop draining, and the connection reaches the
// exact state that used to be fatal.
func dialSilentClient(t *testing.T, id string) *websocket.Conn {
	return dialClientAt(t, livePort, id)
}

func dialClientAt(t *testing.T, port int, id string) *websocket.Conn {
	t.Helper()
	u := url.URL{Scheme: "ws", Host: fmt.Sprintf("localhost:%d", port), Path: "/ws/" + id}
	dialer := websocket.Dialer{
		Subprotocols:     []string{defaultSubProtocol},
		HandshakeTimeout: 5 * time.Second,
		// A small read buffer makes the peer stop absorbing data sooner, so the test reaches
		// the stalled state in a bounded number of writes.
		ReadBufferSize: 1024,
	}
	// Retry rather than polling server.Addr(): that accessor reads a field Start writes
	// without synchronisation (an upstream race, unrelated to what this test covers).
	var conn *websocket.Conn
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, resp, err := dialer.Dial(u.String(), http.Header{})
		if resp != nil {
			_ = resp.Body.Close()
		}
		if err == nil {
			conn = c
			break
		}
		if time.Now().After(deadline) {
			require.NoError(t, err, "client %s could not connect", id)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return conn
}

func TestLiveSocket_StalledPeerDoesNotFreezeServer(t *testing.T) {
	s := NewServer().(*server)
	s.AddSupportedSubprotocol(defaultSubProtocol)
	s.SetMessageHandler(func(Channel, []byte) error { return nil })

	connected := make(chan string, 8)
	s.SetNewClientHandler(func(c Channel) { connected <- c.ID() })

	go s.Start(livePort, serverPath)
	defer s.Stop()

	stuck := dialSilentClient(t, "CP-STUCK")
	defer stuck.Close()
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("server never registered the first client")
	}

	// Push data at the silent peer until the socket stops accepting it. Every write must
	// return promptly — the moment one of them parks, it is holding the connection-map lock
	// and the server is finished.
	payload := make([]byte, 64*1024)
	sawBackpressure := false
	for i := 0; i < 500; i++ {
		done := make(chan error, 1)
		go func() { done <- s.Write("CP-STUCK", payload) }()
		select {
		case err := <-done:
			if err != nil {
				sawBackpressure = true
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Write parked on a peer that stopped reading — the connection-map lock is now held forever")
		}
		if sawBackpressure {
			break
		}
	}
	require.True(t, sawBackpressure, "expected the stalled peer to be reported through backpressure")

	// The whole point: with one connection wedged, everything else still works.
	lookupDone := make(chan struct{})
	go func() {
		defer close(lookupDone)
		s.GetChannel("CP-STUCK")
	}()
	select {
	case <-lookupDone:
	case <-time.After(5 * time.Second):
		t.Fatal("connection lookups are blocked; /connections would hang and liveness could not see it")
	}

	healthy := dialSilentClient(t, "CP-HEALTHY")
	defer healthy.Close()
	select {
	case id := <-connected:
		require.Equal(t, "CP-HEALTHY", id)
	case <-time.After(5 * time.Second):
		t.Fatal("a new charge point could not complete its handshake while another socket was wedged")
	}
}
