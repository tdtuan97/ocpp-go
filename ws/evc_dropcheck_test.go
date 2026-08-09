package ws

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// Measures how often a HEALTHY, actively-reading peer loses a message because the send queue
// was momentarily full. Since WriteManual now reports instead of waiting, the queue size is
// no longer a deadlock question — it is the line between "briefly slow" and "dropped", so it
// needs to be picked against real burst behaviour rather than guessed.
//
// The burst mimics how responses are actually produced: ocpp-go spawns one goroutine per
// inbound frame, so a charger that sends a batch of messages has that many writes racing to
// the same socket at once.
func TestDropRate_HealthyPeerUnderBurst(t *testing.T) {
	const (
		port      = 18991
		burst     = 64   // concurrent writes, as if a charger sent 64 frames back to back
		msgSize   = 2048 // representative OCPP frame (MeterValues with several sampled values)
		roundTrip = 20
	)

	s := NewServer().(*server)
	s.AddSupportedSubprotocol(defaultSubProtocol)
	s.SetMessageHandler(func(Channel, []byte) error { return nil })
	connected := make(chan string, 4)
	s.SetNewClientHandler(func(c Channel) { connected <- c.ID() })

	go s.Start(port, serverPath)
	defer s.Stop()

	conn := dialClientAt(t, port, "CP-HEALTHY")
	defer conn.Close()
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("server never registered the client")
	}

	// A peer that keeps reading, which is what "healthy" means here.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
	defer close(stop)

	payload := make([]byte, msgSize)
	dropped, slow, total := 0, 0, 0
	var mu sync.Mutex

	start := time.Now()
	for round := 0; round < roundTrip; round++ {
		var wg sync.WaitGroup
		for i := 0; i < burst; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				t0 := time.Now()
				err := s.Write("CP-HEALTHY", payload)
				// Anything that did not go straight into the queue took the slow path, where a
				// timer is allocated and the caller blocks. That is the cost the buffer buys down.
				took := time.Since(t0)
				mu.Lock()
				total++
				if err != nil {
					dropped++
				}
				if took > 200*time.Microsecond {
					slow++
				}
				mu.Unlock()
			}()
		}
		wg.Wait()
	}
	elapsed := time.Since(start)

	fmt.Printf("  cap=%-4d dropped %d/%d (%.1f%%)   slow-path %d/%d (%.1f%%)   tổng %s\n",
		cap(s.connections["CP-HEALTHY"].outQueue), dropped, total, float64(dropped)/float64(total)*100,
		slow, total, float64(slow)/float64(total)*100, elapsed.Round(time.Millisecond))
}
