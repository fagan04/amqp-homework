// Package amqpx contains the client-side building blocks shared by the
// producer and the consumer: node list handling, failover dialing with
// exponential backoff, queue topology declaration and structured logging.
package amqpx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// QueueName is the quorum queue used by the whole test suite.
	QueueName = "homework"
	// DLQName receives messages that exceeded the quorum delivery-limit
	// (configured by the "homework-qq" operator policy).
	DLQName = "homework.dlq"
)

// DefaultNodes are the AMQP listeners published by docker-compose
// (rabbit1 -> 5672, rabbit2 -> 5673, rabbit3 -> 5674).
var DefaultNodes = "amqp://admin:admin@localhost:5672/,amqp://admin:admin@localhost:5673/,amqp://admin:admin@localhost:5674/"

// ParseNodes splits a comma-separated list of AMQP URLs.
func ParseNodes(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// HostOf returns host:port of an AMQP URL (credentials are never logged).
func HostOf(u string) string {
	if pu, err := url.Parse(u); err == nil {
		return pu.Host
	}
	return "?"
}

// NewLogger returns a JSON structured logger. Every record carries the
// component name so producer and consumer logs can be merged and filtered.
func NewLogger(component string, level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("component", component)
}

// Dialer connects to the first reachable node, starting at a given index and
// walking the node list round-robin. Between full rounds it backs off
// exponentially (capped), so a fully unavailable cluster is not hammered.
type Dialer struct {
	Nodes       []string
	Log         *slog.Logger
	DialTimeout time.Duration
	Heartbeat   time.Duration
	MaxBackoff  time.Duration
	// WriteTimeout bounds every socket write (default 5 s); see deadlineDial.
	WriteTimeout time.Duration
	ConnName     string
}

// Session is one AMQP connection plus one channel on it.
type Session struct {
	Conn      *amqp.Connection
	Ch        *amqp.Channel
	NodeIndex int
	Node      string
	ConnClose chan *amqp.Error
	ChClose   chan *amqp.Error
}

// Close closes the channel and the connection, ignoring errors.
func (s *Session) Close() {
	if s == nil {
		return
	}
	if s.Ch != nil {
		_ = s.Ch.Close()
	}
	if s.Conn != nil {
		_ = s.Conn.Close()
	}
}

// Abandon closes the session in the background. Used on failure paths:
// amqp091's Close waits for the broker's close-ok, and a broker whose channel
// is still waiting on an unreachable quorum-queue leader can take as long as
// net_ticktime to answer. The client must not wait for that before failing over.
func (s *Session) Abandon() {
	if s != nil {
		go s.Close()
	}
}

// Closed returns a channel that fires when either the connection or the
// channel is closed (by the broker, a network failure, or a node pause).
func (s *Session) Closed() <-chan *amqp.Error {
	out := make(chan *amqp.Error, 1)
	go func() {
		select {
		case e := <-s.ConnClose:
			out <- e
		case e := <-s.ChClose:
			out <- e
		}
	}()
	return out
}

func (d *Dialer) config() amqp.Config {
	dt := d.DialTimeout
	if dt == 0 {
		dt = 3 * time.Second
	}
	hb := d.Heartbeat
	if hb == 0 {
		hb = 5 * time.Second
	}
	props := amqp.NewConnectionProperties()
	if d.ConnName != "" {
		props.SetClientConnectionName(d.ConnName)
	}
	wt := d.WriteTimeout
	if wt == 0 {
		wt = 5 * time.Second
	}
	return amqp.Config{
		Heartbeat:  hb,
		Locale:     "en_US",
		Dial:       deadlineDial(dt, wt),
		Properties: props,
	}
}

// deadlineDial dials with a connect/handshake timeout (like amqp.DefaultDial)
// and wraps the socket so that every write must complete within wt.
//
// Why: during a silent network partition (packets dropped, no RST) a TCP
// write to the isolated broker blocks once the socket buffer is full. In
// amqp091-go that blocked write holds the connection's writer and also delays
// heartbeat-based failure detection, so the publisher hangs until the
// partition heals. With a write deadline the write fails after wt, the
// library closes the connection, NotifyClose fires, and the client fails over.
func deadlineDial(dt, wt time.Duration) func(network, addr string) (net.Conn, error) {
	return func(network, addr string) (net.Conn, error) {
		c, err := net.DialTimeout(network, addr, dt)
		if err != nil {
			return nil, err
		}
		// Handshake deadline; amqp091 clears it once the connection is open.
		if err := c.SetDeadline(time.Now().Add(dt)); err != nil {
			_ = c.Close()
			return nil, err
		}
		return &deadlineConn{Conn: c, wt: wt}, nil
	}
}

type deadlineConn struct {
	net.Conn
	wt time.Duration
}

func (c *deadlineConn) Write(b []byte) (int, error) {
	if err := c.Conn.SetWriteDeadline(time.Now().Add(c.wt)); err != nil {
		return 0, err
	}
	return c.Conn.Write(b)
}

// DialOne opens a session on exactly one node (no failover).
func (d *Dialer) DialOne(idx int) (*Session, error) {
	node := d.Nodes[idx]
	conn, err := amqp.DialConfig(node, d.config())
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &Session{
		Conn:      conn,
		Ch:        ch,
		NodeIndex: idx,
		Node:      HostOf(node),
		ConnClose: conn.NotifyClose(make(chan *amqp.Error, 1)),
		ChClose:   ch.NotifyClose(make(chan *amqp.Error, 1)),
	}, nil
}

// Connect tries every node once starting at `start`, then backs off and
// repeats until it succeeds or ctx is cancelled.
func (d *Dialer) Connect(ctx context.Context, start int) (*Session, error) {
	backoff := 250 * time.Millisecond
	maxB := d.MaxBackoff
	if maxB == 0 {
		maxB = 5 * time.Second
	}
	for attempt := 1; ; attempt++ {
		for i := 0; i < len(d.Nodes); i++ {
			idx := (start + i) % len(d.Nodes)
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			s, err := d.DialOne(idx)
			if err == nil {
				d.Log.Info("connected", "node", s.Node, "attempt", attempt)
				return s, nil
			}
			d.Log.Warn("dial failed", "node", HostOf(d.Nodes[idx]), "err", err.Error())
		}
		d.Log.Warn("all nodes unreachable, backing off", "backoff_ms", backoff.Milliseconds())
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxB {
			backoff = maxB
		}
	}
}

// Reachable reports whether a TCP connection to the node's AMQP port can be
// opened. Used by the consumer's switch-back probe (cheap, no AMQP handshake).
func Reachable(node string, timeout time.Duration) bool {
	c, err := net.DialTimeout("tcp", HostOf(node), timeout)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// DeclareTopology declares the durable quorum work queue and its dead-letter
// queue. Delivery-limit and dead-lettering are applied by the operator policy
// (see rabbitmq-cluster/init/init.sh) so queue arguments stay compatible with
// queues created by earlier versions of the clients.
func DeclareTopology(ch *amqp.Channel) error {
	args := amqp.Table{"x-queue-type": "quorum"}
	if _, err := ch.QueueDeclare(QueueName, true, false, false, false, args); err != nil {
		return fmt.Errorf("declare %s: %w", QueueName, err)
	}
	if _, err := ch.QueueDeclare(DLQName, true, false, false, false, args); err != nil {
		return fmt.Errorf("declare %s: %w", DLQName, err)
	}
	return nil
}

// IsClosedErr reports whether err means the channel/connection is gone.
func IsClosedErr(err error) bool {
	return errors.Is(err, amqp.ErrClosed)
}
