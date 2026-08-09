// The package is a wrapper around gorilla websockets,
// aimed at simplifying the creation and usage of a websocket client/server.
//
// Check the client and server structure to get started.
package ws

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lorenzodonini/ocpp-go/logging"
)

const (
	// Time allowed to write a message to the peer.
	defaultWriteWait = 10 * time.Second
	// Time allowed to read the next pong message from the peer.
	defaultPongWait = 60 * time.Second
	// Time allowed to wait for a ping on the server, before closing a connection due to inactivity.
	defaultPingWait = defaultPongWait
	// Send pings to peer with this period. Must be less than pongWait.
	defaultPingPeriod = (defaultPongWait * 9) / 10
	// Time allowed for the initial handshake to complete.
	defaultHandshakeTimeout = 30 * time.Second
	// When the Charging Station is reconnecting, after a connection loss, it will use this variable for the amount of time
	// it will double the previous back-off time. When the maximum number of increments is reached, the Charging
	// Station keeps connecting with the same back-off time.
	defaultRetryBackOffRepeatTimes = 5
	// When the Charging Station is reconnecting, after a connection loss, it will use this variable as the maximum value
	// for the random part of the back-off time. It will add a new random value to every increasing back-off time,
	// including the first connection attempt (with this maximum), for the amount of times it will double the previous
	// back-off time. When the maximum number of increments is reached, the Charging Station will keep connecting
	// with the same back-off time.
	defaultRetryBackOffRandomRange = 15 // seconds
	// When the Charging Station is reconnecting, after a connection loss, it will use this variable as the minimum backoff
	// time, the first time it tries to reconnect.
	defaultRetryBackOffWaitMinimum = 10 * time.Second
)

// The internal verbose logger
var log logging.Logger

// Sets a custom Logger implementation, allowing the package to log events.
// By default, a VoidLogger is used, so no logs will be sent to any output.
//
// The function panics, if a nil logger is passed.
func SetLogger(logger logging.Logger) {
	if logger == nil {
		panic("cannot set a nil logger")
	}
	log = logger
}

// ServerTimeoutConfig contains optional configuration parameters for a websocket server.
// Setting the parameter allows to define custom timeout intervals for websocket network operations.
//
// To set a custom configuration, refer to the server's SetTimeoutConfig method.
// If no configuration is passed, a default configuration is generated via the NewServerTimeoutConfig function.
type ServerTimeoutConfig struct {
	WriteWait  time.Duration // The timeout for network write operations. After a timeout, the connection is closed.
	PingWait   time.Duration // The timeout for waiting for a ping from the client. After a timeout, the connection is closed.
	PingPeriod time.Duration // The interval for sending ping messages to a client. If set to 0, no pings are sent.
	PongWait   time.Duration // The timeout for waiting for a pong from the server. After a timeout, the connection is closed. Needs to be set, if server is configured to send ping messages.
}

// NewServerTimeoutConfig creates a default timeout configuration for a websocket endpoint.
// In the default configuration, server-side ping messages are disabled.
//
// You may change fields arbitrarily and pass the struct to a SetTimeoutConfig method.
func NewServerTimeoutConfig() ServerTimeoutConfig {
	return ServerTimeoutConfig{
		WriteWait:  defaultWriteWait,
		PingWait:   defaultPingWait,
		PingPeriod: 0,
		PongWait:   0,
	}
}

// ClientTimeoutConfig contains optional configuration parameters for a websocket client.
// Setting the parameter allows to define custom timeout intervals for websocket network operations.
//
// To set a custom configuration, refer to the client's SetTimeoutConfig method.
// If no configuration is passed, a default configuration is generated via the NewClientTimeoutConfig function.
type ClientTimeoutConfig struct {
	WriteWait               time.Duration // The timeout for network write operations. After a timeout, the connection is closed.
	HandshakeTimeout        time.Duration // The timeout for the initial handshake to complete.
	PongWait                time.Duration // The timeout for waiting for a pong from the server. After a timeout, the connection is closed. Needs to be set, if client is configured to send ping messages.
	PingPeriod              time.Duration // The interval for sending ping messages to a server. If set to 0, no pings are sent.
	RetryBackOffRepeatTimes int
	RetryBackOffRandomRange int
	RetryBackOffWaitMinimum time.Duration
}

// NewClientTimeoutConfig creates a default timeout configuration for a websocket endpoint.
//
// You may change fields arbitrarily and pass the struct to a SetTimeoutConfig method.
func NewClientTimeoutConfig() ClientTimeoutConfig {
	return ClientTimeoutConfig{
		WriteWait:               defaultWriteWait,
		HandshakeTimeout:        defaultHandshakeTimeout,
		PongWait:                defaultPongWait,
		PingPeriod:              defaultPingPeriod,
		RetryBackOffRepeatTimes: defaultRetryBackOffRepeatTimes,
		RetryBackOffRandomRange: defaultRetryBackOffRandomRange,
		RetryBackOffWaitMinimum: defaultRetryBackOffWaitMinimum,
	}
}

// Wraps a time.Ticker instance to provide a nullable ticker option.
// If no real ticker is instantiated, the struct will run/return no-ops.
type optTicker struct {
	c      chan time.Time
	ticker *time.Ticker
}

func newOptTicker(pingCfg *PingConfig) optTicker {
	if pingCfg != nil && pingCfg.PingPeriod > 0 {
		// Create regular ticker
		return optTicker{
			ticker: time.NewTicker(pingCfg.PingPeriod),
		}
	}
	// Ticker shall be dummy, as it doesn't trigger any actual events
	return optTicker{
		c: make(chan time.Time, 1),
	}
}

func (o optTicker) T() <-chan time.Time {
	if o.ticker != nil {
		return o.ticker.C
	}
	return o.c
}

func (o optTicker) Stop() {
	if o.ticker != nil {
		o.ticker.Stop()
	}
}

// Channel represents a bi-directional IP-based communication channel, which provides at least a unique ID.
type Channel interface {
	// ID returns the unique identifier of the client, which identifies this unique channel.
	ID() string
	// RemoteAddr returns the remote IP network address of the connected peer.
	RemoteAddr() net.Addr
	// TLSConnectionState returns information about the active TLS connection, if any.
	TLSConnectionState() *tls.ConnectionState
	// IsConnected returns true if the connection to the peer is active, false if it was closed already.
	IsConnected() bool
}

// WebSocketConfig is a utility config struct for a single webSocket.
// By default, it inherits values from respective the ClientTimeoutConfig or ServerTimeoutConfig.
// However, during creation, some fields may be overridden and customized on a websocket-basis.
type WebSocketConfig struct {
	// The timeout for network write operations.
	// After a timeout, the connection is closed.
	WriteWait time.Duration
	// The timeout for waiting for a message from the connected peer.
	// After a timeout, the connection is closed.
	// If ReadWait is zero, the websocket will not time out on read operations.
	//
	// Depending on the configuration, the websocket will either wait for incoming pings
	// or send pings to the connected peer.
	//
	// If PingConfig is set (i.e. the websocket is configured to send ping messages),
	// the ReadWait value should be omitted.
	// If provided, the websocket will accept ping messages, but the read timeout
	// configuration from the PingConfig will be prioritized.
	ReadWait time.Duration
	// Optional configuration for ping operations. If omitted, the websocket will not send any pings.
	PingConfig *PingConfig
	// Optional logger for the websocket. If omitted, the global logger is used.
	Logger logging.Logger
}

// PingConfig contains optional configuration parameters for websockets sending ping operations.
type PingConfig struct {
	PingPeriod time.Duration // The interval for sending ping messages to the connected peer.
	PongWait   time.Duration // The timeout for waiting for a pong from the connected peer. After a timeout, the connection is closed.
}

// NewDefaultWebSocketConfig creates a new websocket config struct with the passed values.
// If sendPing is set, the websocket will be configured to send out periodic pings.
//
// No custom configuration functions are run. Overrides need to be applied externally.
func NewDefaultWebSocketConfig(
	writeWait time.Duration,
	readWait time.Duration,
	pingPeriod time.Duration,
	pongWait time.Duration) WebSocketConfig {
	var pingCfg *PingConfig
	if pingPeriod > 0 {
		pingCfg = &PingConfig{
			PingPeriod: pingPeriod,
			PongWait:   pongWait,
		}
	}
	return WebSocketConfig{
		WriteWait:  writeWait,
		ReadWait:   readWait,
		PingConfig: pingCfg,
		Logger:     log,
	}
}

type MessageHandler func(c Channel, data []byte) error
type ConnectedHandler func(c Channel)
type DisconnectedHandler func(c Channel, err error)
type ErrorHandler func(c Channel, err error)

type message struct {
	typ  int
	data []byte
}

// webSocket is a wrapper for a single websocket channel.
// The connection itself is provided by the gorilla websocket package.
//
// Don't use a websocket directly, but refer to Server and Client.
type webSocket struct {
	connection         *websocket.Conn
	mutex              sync.RWMutex
	id                 string
	outQueue           chan message
	pingC              chan []byte
	closeC             chan websocket.CloseError // used to gracefully close a websocket connection.
	forceCloseC        chan error                // used by the readPump to notify a forcefully closed connection to the writePump.
	tlsConnectionState *tls.ConnectionState
	cfg                WebSocketConfig
	log                logging.Logger
	onClosed           DisconnectedHandler
	onError            ErrorHandler
	onMessage          MessageHandler
}

// EVC OCPP PATCH: how long a writer waits for room in a socket's send queue.
// Bounded, not zero and not infinite. Dropping the instant the queue is momentarily busy
// loses messages from perfectly healthy chargers — measured at 76-84% loss under a burst,
// and barely improved by a larger buffer (512 slots still lost 49%), because the queue
// depth is not what a saturated socket is short of. Waiting forever is what caused the
// original outage. A short wait covers the normal case, where the writer only needs the
// pump to finish the frame in front of it, and still bounds the damage when the peer has
// genuinely stopped reading: this is held under the socket's read lock, so it delays that
// one socket's cleanup by at most this long, and no longer blocks the server's connection
// map at all.
const writeQueueWait = time.Second

func newWebSocket(id string, conn *websocket.Conn, tlsState *tls.ConnectionState, cfg WebSocketConfig, onMessage MessageHandler, onClosed DisconnectedHandler, onError ErrorHandler) *webSocket {
	if conn == nil {
		panic("cannot create websocket with nil connection")
	}
	w := &webSocket{
		id:                 id,
		connection:         conn,
		mutex:              sync.RWMutex{},
		tlsConnectionState: tlsState,
		outQueue:           make(chan message, 2),
		pingC:              make(chan []byte, 1),
		closeC:             make(chan websocket.CloseError, 1),
		forceCloseC:        make(chan error, 1),
		onClosed:           onClosed,
		onError:            onError,
		onMessage:          onMessage,
	}
	w.updateConfig(cfg)
	return w
}

// Retrieves the unique Identifier of the websocket (typically, the URL suffix).
func (w *webSocket) ID() string {
	return w.id
}

// Returns the address of the remote peer.
func (w *webSocket) RemoteAddr() net.Addr {
	return w.connection.RemoteAddr()
}

// Returns the TLS connection state of the connection, if any.
func (w *webSocket) TLSConnectionState() *tls.ConnectionState {
	return w.tlsConnectionState
}

func (w *webSocket) IsConnected() bool {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	return w.connection != nil
}

func (w *webSocket) Write(data []byte) error {
	return w.WriteManual(websocket.TextMessage, data)
}

func (w *webSocket) WriteManual(messageTyp int, data []byte) error {
	msg := message{
		typ:  messageTyp,
		data: data,
	}
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	if w.connection == nil {
		return fmt.Errorf("cannot write to closed connection %s", w.id)
	}
	// OLD:
	// w.outQueue <- msg
	// return nil

	// EVC OCPP PATCH: never block here indefinitely.
	// This runs with w.mutex held as a reader, and the caller (server.Write) holds the
	// server's connection-map lock as a reader too. An unbounded send strands both: writePump
	// can only drain this queue by exiting into cleanup, and cleanup needs w.mutex as a
	// writer, which this reader is holding. That is a hard deadlock, and because Go's RWMutex
	// is writer-preferring the stranded connection-map reader then starves every later reader
	// — no charge point can connect, no response can be written, and nothing returns an error
	// to log.
	//
	// Fast path: room in the queue, nothing allocated. Where writes on a healthy socket land.
	select {
	case w.outQueue <- msg:
		return nil
	default:
	}
	// Slow path: the pump is behind. Wait briefly — only now is a timer worth allocating.
	timer := time.NewTimer(writeQueueWait)
	defer timer.Stop()
	select {
	case w.outQueue <- msg:
		return nil
	case <-timer.C:
		return fmt.Errorf("send queue full for connection %s after %s, dropping message", w.id, writeQueueWait)
	}
}

func (w *webSocket) Close(closeError websocket.CloseError) error {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	if w.connection == nil {
		return fmt.Errorf("cannot close already closed connection %s", w.id)
	}
	// OLD:
	// w.closeC <- closeError
	// return nil

	// EVC OCPP PATCH: non-blocking, for the same reason as WriteManual and because closeC holds a
	// single slot — a second close request for the same socket parked here forever holding
	// w.mutex as a reader, which prevented cleanup from ever running. One pending close is
	// all that is meaningful anyway: the socket cannot close twice.
	select {
	case w.closeC <- closeError:
		return nil
	default:
		return nil
	}
}

func (w *webSocket) updateConfig(cfg WebSocketConfig) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.cfg = cfg
	// Update logger
	if cfg.Logger != nil {
		w.log = cfg.Logger
	} else {
		w.log = log
	}
	// Update ping pong logic
	w.initPingPong()
}

func (w *webSocket) getReadTimeout() time.Time {
	var wait time.Duration
	// Prefer ping config, then read wait, then no timeout
	if w.cfg.PingConfig != nil && w.cfg.PingConfig.PongWait > 0 {
		wait = w.cfg.PingConfig.PongWait
	} else if w.cfg.ReadWait > 0 {
		wait = w.cfg.ReadWait
	} else {
		// No timeout configured
		return time.Time{}
	}
	return time.Now().Add(wait)
}

func (w *webSocket) initPingPong() {
	conn := w.connection
	if w.cfg.ReadWait > 0 {
		// Expect pings, reply with pongs
		conn.SetPingHandler(w.onPing)
	} else {
		conn.SetPingHandler(nil)
	}
	if w.cfg.PingConfig != nil {
		// Optionally send pings, expect pongs
		conn.SetPongHandler(w.onPong)
	} else {
		conn.SetPongHandler(nil)
	}
}

func (w *webSocket) onPing(appData string) error {
	// OLD:
	// conn := w.connection
	// w.log.Debugf("ping received from %s: %s", w.id, appData)
	// // Schedule pong message via dedicated channel
	// w.pingC <- []byte(appData)
	// w.log.Debugf("pong scheduled for %s", w.id)

	// EVC OCPP PATCH: hold the read lock for the whole handler, which every other channel sender on
	// this type already did. cleanup closes pingC under the write lock, so the unguarded send
	// could land on a closed channel and panic the entire process; reading w.connection
	// without the lock also races with cleanup nilling it.
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	conn := w.connection
	if conn == nil {
		return nil
	}
	w.log.Debugf("ping received from %s: %s", w.id, appData)
	// Schedule pong message via dedicated channel. Non-blocking: a pong that cannot be queued
	// because writePump has stopped draining is worthless, and waiting for it would park the
	// read loop of a socket that is already dead.
	select {
	case w.pingC <- []byte(appData):
		w.log.Debugf("pong scheduled for %s", w.id)
	default:
		w.log.Debugf("pong dropped for %s, writer is not draining", w.id)
	}
	// Reset read interval after receiving a ping
	return conn.SetReadDeadline(w.getReadTimeout())
}

func (w *webSocket) onPong(appData string) error {
	// OLD:
	// conn := w.connection

	// EVC OCPP PATCH: same guard as onPing — cleanup nils w.connection under the write lock, so
	// reading it without the lock is a data race with a nil dereference at the end of it.
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	conn := w.connection
	if conn == nil {
		return nil
	}
	w.log.Debugf("pong received from %s: %s", w.id, appData)
	// Reset read interval after receiving a pong
	return conn.SetReadDeadline(w.getReadTimeout())
}

func (w *webSocket) cleanup(err error) {
	w.mutex.Lock()
	// Properly close the connection
	if e := w.connection.Close(); e != nil {
		log.Errorf("failed to close connection for %s: %v", w.id, e)
	}
	w.connection = nil
	close(w.outQueue)
	close(w.pingC)
	close(w.closeC)
	close(w.forceCloseC)
	w.mutex.Unlock()
	// Invoke callback to notify the websocket was closed.
	// If err is not nil, the disconnect is considered forced (i.e. not user-initiated).
	w.onClosed(w, err)
}

func (w *webSocket) run() {
	go w.readPump()
	go w.writePump()
}

// The readPump is a dedicated routine that awaits the next incoming message up until a deadline.
func (w *webSocket) readPump() {
	w.mutex.RLock()
	conn := w.connection
	w.mutex.RUnlock()
	if conn == nil {
		err := fmt.Errorf("readPump started for %s with nil connection", w.id)
		w.onError(w, err)
		return
	}

	_ = conn.SetReadDeadline(w.getReadTimeout())
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				w.onError(w, fmt.Errorf("read failed unexpectedly for %s: %w", w.id, err))
			}
			// Verify whether the disconnect was already dealt with
			w.mutex.RLock()
			if w.connection == nil {
				// Connection cleaned up, read simply got notified of the close -> ignore
				w.log.Debugf("readPump stopped for %s due to closed connection", w.id)
				w.mutex.RUnlock()
				return
			}
			// Notify writePump of error. Force close will be handled there.
			// OLD:
			// w.forceCloseC <- err

			// EVC OCPP PATCH: non-blocking. forceCloseC holds one slot, and if writePump has
			// already stopped draining, parking here holds w.mutex as a reader forever and
			// blocks the cleanup that is the only thing able to finish this teardown.
			w.log.Debugf("handling read error for %s: %v", w.id, err.Error())
			select {
			case w.forceCloseC <- err:
			default:
			}
			w.mutex.RUnlock()
			return
		}

		// Forward message to handler.
		// Errors during the handling don't interrupt the websocket routine but will be reported.
		err = w.onMessage(w, msg)
		if err != nil {
			w.onError(w, err)
		}
		_ = conn.SetReadDeadline(w.getReadTimeout())
	}
}

// All actions and events are handled within this centralized control flow function.
func (w *webSocket) writePump() {
	conn := w.connection
	ticker := newOptTicker(w.cfg.PingConfig)

	closure := func(err error) {
		ticker.Stop()
		w.cleanup(err)
	}

	for {
		select {
		case <-ticker.T():
			// Send periodic ping
			_ = conn.SetWriteDeadline(time.Now().Add(w.cfg.WriteWait))
			err := conn.WriteMessage(websocket.PingMessage, []byte{})
			if err != nil {
				w.onError(w, fmt.Errorf("failed to send ping message for %s: %w", w.id, err))
				// Invoking cleanup, as socket was forcefully closed
				closure(err)
				return
			}
			log.Debugf("ping sent for %s", w.id)
		case ping := <-w.pingC:
			// Reply with pong message
			_ = conn.SetWriteDeadline(time.Now().Add(w.cfg.WriteWait))
			err := conn.WriteMessage(websocket.PongMessage, ping)
			if err != nil {
				w.onError(w, fmt.Errorf("failed to send pong message %s: %w", w.id, err))
				// Invoking cleanup, as socket was forcefully closed
				closure(err)
				return
			}
			log.Debugf("pong sent for %s: %s", w.id, string(ping))
		case msg, ok := <-w.outQueue:
			// New data needs to be written out (also invoked for pong messages)
			if !ok {
				// Unexpected closed queue, should never happen.
				// Don't invoke any cleanup but just exit routine.
				w.onError(w, fmt.Errorf("output queue for socket %v was closed, ignoring and existing", w.id))
				return
			}
			// Send data
			_ = conn.SetWriteDeadline(time.Now().Add(w.cfg.WriteWait))
			err := conn.WriteMessage(msg.typ, msg.data)
			if err != nil {
				w.onError(w, fmt.Errorf("write failed for %s: %w", w.id, err))
				// Invoking cleanup, as socket was forcefully closed
				closure(err)
				return
			}
			log.Debugf("written %d bytes to %s", len(msg.data), w.id)
		case closeErr := <-w.closeC:
			// webSocket is being gracefully closed by user command
			w.log.Debugf("closing connection for %s: %d - %s", w.id, closeErr.Code, closeErr.Text)
			// Send explicit close message
			err := conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(closeErr.Code, closeErr.Text),
				time.Now().Add(w.cfg.WriteWait))
			if err != nil {
				// At this point the connection is considered to be forcefully closed,
				// but we still continue with the intended flow.
				w.onError(w, fmt.Errorf("failed to write close message for connection %s: %w", w.id, err))
			}
			// Invoking cleanup, but signal that this is an intended operation,
			// preventing automatic reconnection attempts.
			closure(nil)
			return
		case closed, _ := <-w.forceCloseC:
			if closed == nil {
				closed = fmt.Errorf("websocket read channel closed abruptly")
			}
			// webSocket is being forcefully closed, triggered by readPump encountering a failed read.
			log.Debugf("handling forced close signal for %s, caused by: %v", w.id, closed.Error())
			// Connection was forcefully closed, invoke cleanup
			closure(closed)
			return
		}
	}
}

// HttpConnectionError is a websocket-specific error propagated to the upper
// layers when opening a websocket fails.
type HttpConnectionError struct {
	Message    string
	HttpStatus string
	HttpCode   int
	Details    string
}

func (e HttpConnectionError) Error() string {
	return fmt.Sprintf("%v, http status: %v", e.Message, e.HttpStatus)
}

func init() {
	log = &logging.VoidLogger{}
}
