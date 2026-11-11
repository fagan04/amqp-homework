// Consumer: validating consumer for the RabbitMQ quorum queue.
//
// Guarantees implemented:
//   - Manual acknowledgments: a message is acked only after processing; if
//     the connection dies before the ack, the broker re-delivers it.
//   - Prefetch (QoS) window to bound unacked messages per consumer.
//   - Node failover in preference order (default: rabbit2 -> rabbit3 ->
//     rabbit1) and automatic switch-back to the preferred node once it is
//     reachable again.
//   - Idempotent processing: duplicates (from producer re-publishes or broker
//     re-deliveries) are detected by (producer_id, seq) and acked without
//     being processed twice.
//   - Validation: per-producer ordering, gaps, duplicates, missing messages
//     (loss) and end-to-end latency, written as a JSON summary + CSV series.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"amqp-homework/internal/amqpx"
	"amqp-homework/internal/stats"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Default preference order kept from the original consumer: node 2 primary,
// node 3 fallback, node 1 last resort.
var nodes = "amqp://admin:admin@localhost:5673/,amqp://admin:admin@localhost:5674/,amqp://admin:admin@localhost:5672/"

type config struct {
	nodes         []string
	prefetch      int
	runID         string
	expect        int
	expectPer     int
	idleTimeout   time.Duration
	workMs        int
	probeInterval time.Duration
	noSwitchBack  bool
	statsCSV      string
	summary       string
	timeout       time.Duration
	verbose       bool
}

// producerState tracks validation data for one producer stream.
type producerState struct {
	seen       map[int64]struct{}
	lastSeq    int64
	outOfOrder int64 // seq < lastSeq (and not a duplicate): arrived late
	gaps       int64 // seq > lastSeq+1 at arrival time
}

type validator struct {
	mu        sync.Mutex
	producers map[string]*producerState
	foreign   int64 // messages from other runs (ignored)
	lat       stats.Recorder
}

var (
	received    atomic.Int64 // deliveries incl. duplicates
	unique      atomic.Int64
	duplicates  atomic.Int64
	redelivered atomic.Int64
	acked       atomic.Int64
	ackErrors   atomic.Int64
	failovers   atomic.Int64
	switchBacks atomic.Int64
	lastMsgAt   atomic.Int64
	curNode     atomic.Value
)

type event struct {
	T    float64 `json:"t_s"`
	Type string  `json:"type"`
	Node string  `json:"node"`
	Info string  `json:"info,omitempty"`
}

var (
	evMu   sync.Mutex
	events []event
	start  = time.Now()
)

func addEvent(typ, node, info string) {
	evMu.Lock()
	events = append(events, event{T: time.Since(start).Seconds(), Type: typ, Node: node, Info: info})
	evMu.Unlock()
}

func main() {
	cfg := &config{}
	var nodeList string
	flag.StringVar(&nodeList, "nodes", nodes, "comma-separated AMQP URLs in preference order (first = preferred)")
	flag.IntVar(&cfg.prefetch, "prefetch", 50, "QoS prefetch count (max unacked deliveries)")
	flag.StringVar(&cfg.runID, "run-id", "", "only validate messages of this run (others are acked and counted as foreign)")
	flag.IntVar(&cfg.expect, "expect", 0, "exit after this many unique messages (0 = run until idle-timeout/signal)")
	flag.IntVar(&cfg.expectPer, "expect-per-producer", 0, "messages each producer sent; enables missing-message detection")
	flag.DurationVar(&cfg.idleTimeout, "idle-timeout", 0, "exit when no message arrived for this long (0 = never)")
	flag.IntVar(&cfg.workMs, "work-ms", 0, "simulated processing time per message")
	flag.DurationVar(&cfg.probeInterval, "probe-interval", 3*time.Second, "how often to probe the preferred node while failed over")
	flag.BoolVar(&cfg.noSwitchBack, "no-switch-back", false, "stay on the failover node")
	flag.StringVar(&cfg.statsCSV, "stats-csv", "", "write per-second stats to this CSV")
	flag.StringVar(&cfg.summary, "summary", "", "write JSON summary to this file")
	flag.DurationVar(&cfg.timeout, "timeout", 15*time.Minute, "abort after this long")
	flag.BoolVar(&cfg.verbose, "v", false, "debug logging (one line per message)")
	flag.Parse()
	cfg.nodes = amqpx.ParseNodes(nodeList)
	lvl := slog.LevelInfo
	if cfg.verbose {
		lvl = slog.LevelDebug
	}
	log := amqpx.NewLogger("consumer", lvl)
	curNode.Store("-")

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; log.Warn("signal received, stopping"); cancel() }()

	v := &validator{producers: map[string]*producerState{}}
	series, _ := stats.NewSeries(cfg.statsCSV, []string{"epoch_ms", "t_s", "unique_total", "unique_per_s", "duplicates", "redelivered", "node"})

	// Stats ticker + idle / expect exit conditions.
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		var last int64
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				u := unique.Load()
				el := now.Sub(start).Seconds()
				series.Row(now.UnixMilli(), el, u, float64(u-last), duplicates.Load(), redelivered.Load(), curNode.Load().(string))
				log.Info("stats", "t_s", int(el), "unique", u, "unique_per_s", u-last, "duplicates", duplicates.Load(),
					"redelivered", redelivered.Load(), "node", curNode.Load())
				last = u
				if cfg.idleTimeout > 0 && u > 0 && time.Since(time.Unix(0, lastMsgAt.Load())) > cfg.idleTimeout {
					log.Info("idle timeout reached")
					cancel()
				}
			}
		}
	}()

	d := &amqpx.Dialer{Nodes: cfg.nodes, Log: log, ConnName: "consumer"}
	startIdx := 0
	for ctx.Err() == nil {
		reason := consumeFrom(ctx, cfg, d, startIdx, v, log)
		if ctx.Err() != nil || reason.kind == "done" {
			break
		}
		switch reason.kind {
		case "switch_back":
			startIdx = 0
		case "lost":
			failovers.Add(1)
			addEvent("failover", reason.node, reason.info)
			startIdx = (reason.idx + 1) % len(cfg.nodes)
			time.Sleep(200 * time.Millisecond)
		}
	}
	series.Close()
	sum := v.summary(cfg)
	_ = stats.WriteJSON(cfg.summary, sum)
	log.Info("done", "summary", sum)
	if sum["zero_loss"] == false {
		os.Exit(2)
	}
}

type exitReason struct {
	kind string // lost | switch_back | done
	node string
	idx  int
	info string
}

// consumeFrom runs one consuming session and returns why it ended.
func consumeFrom(ctx context.Context, cfg *config, d *amqpx.Dialer, startIdx int, v *validator, log *slog.Logger) (reason exitReason) {
	s, err := d.Connect(ctx, startIdx)
	if err != nil {
		return exitReason{kind: "done"}
	}
	defer func() {
		if reason.kind == "lost" {
			// Close in the background so a failover never waits on an
			// unresponsive broker (see amqpx.Session.Abandon).
			s.Abandon()
		} else {
			s.Close() // clean close: all acks reach the broker before exit
		}
	}()
	log = log.With("node", s.Node)
	curNode.Store(s.Node)
	addEvent("connected", s.Node, "")

	if err := amqpx.DeclareTopology(s.Ch); err != nil {
		log.Warn("topology declare failed", "err", err.Error())
		return exitReason{kind: "lost", node: s.Node, idx: s.NodeIndex, info: err.Error()}
	}
	if err := s.Ch.Qos(cfg.prefetch, 0, false); err != nil {
		return exitReason{kind: "lost", node: s.Node, idx: s.NodeIndex, info: err.Error()}
	}
	tag := fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	msgs, err := s.Ch.Consume(amqpx.QueueName, tag, false /*manual ack*/, false, false, false, nil)
	if err != nil {
		return exitReason{kind: "lost", node: s.Node, idx: s.NodeIndex, info: err.Error()}
	}
	log.Info("consuming", "prefetch", cfg.prefetch)
	closed := s.Closed()

	// Switch-back probe: only when we are not on the preferred node.
	switchBack := make(chan struct{})
	if s.NodeIndex != 0 && !cfg.noSwitchBack {
		go func() {
			t := time.NewTicker(cfg.probeInterval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-closed:
					return
				case <-t.C:
					if amqpx.Reachable(cfg.nodes[0], time.Second) {
						// Make sure the node really accepts AMQP sessions
						// (a paused minority node refuses them).
						if ps, err := d.DialOne(0); err == nil {
							ps.Close()
							close(switchBack)
							return
						}
					}
				}
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			_ = s.Ch.Cancel(tag, false)
			return exitReason{kind: "done"}
		case e := <-closed:
			log.Warn("session lost, failing over", "err", fmt.Sprint(e))
			return exitReason{kind: "lost", node: s.Node, idx: s.NodeIndex, info: fmt.Sprint(e)}
		case <-switchBack:
			log.Info("preferred node is back, switching back", "preferred", amqpx.HostOf(cfg.nodes[0]))
			switchBacks.Add(1)
			addEvent("switch_back", amqpx.HostOf(cfg.nodes[0]), "from "+s.Node)
			// Graceful hand-over: cancel the subscription so the broker
			// stops delivering, then finish processing (and acking) the
			// deliveries already prefetched before disconnecting. This
			// avoids requeues and so preserves per-producer ordering.
			_ = s.Ch.Cancel(tag, false)
			drained := drain(cfg, v, msgs, closed, log)
			log.Info("drained prefetched deliveries before switch-back", "count", drained)
			return exitReason{kind: "switch_back"}
		case msg, ok := <-msgs:
			if !ok {
				log.Warn("delivery channel closed, failing over")
				return exitReason{kind: "lost", node: s.Node, idx: s.NodeIndex, info: "delivery channel closed"}
			}
			handle(cfg, v, msg, log)
			if cfg.expect > 0 && unique.Load() >= int64(cfg.expect) {
				log.Info("expected message count reached", "unique", unique.Load())
				return exitReason{kind: "done"}
			}
		}
	}
}

func drain(cfg *config, v *validator, msgs <-chan amqp.Delivery, closed <-chan *amqp.Error, log *slog.Logger) int {
	n := 0
	timeout := time.After(10 * time.Second)
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return n
			}
			handle(cfg, v, msg, log)
			n++
		case <-closed:
			return n
		case <-timeout:
			return n
		case <-time.After(500 * time.Millisecond):
			return n // nothing more buffered
		}
	}
}

func handle(cfg *config, v *validator, msg amqp.Delivery, log *slog.Logger) {
	received.Add(1)
	lastMsgAt.Store(time.Now().UnixNano())
	if msg.Redelivered {
		redelivered.Add(1)
	}
	pid, _ := msg.Headers["producer_id"].(string)
	run, _ := msg.Headers["run_id"].(string)
	seq, _ := msg.Headers["seq"].(int64)
	sentNs, _ := msg.Headers["sent_ts_ns"].(int64)

	if pid == "" || (cfg.runID != "" && run != cfg.runID) {
		v.mu.Lock()
		v.foreign++
		v.mu.Unlock()
		ack(msg, log)
		return
	}

	v.mu.Lock()
	ps := v.producers[pid]
	if ps == nil {
		ps = &producerState{seen: map[int64]struct{}{}}
		v.producers[pid] = ps
	}
	if _, dup := ps.seen[seq]; dup {
		v.mu.Unlock()
		duplicates.Add(1)
		log.Debug("duplicate skipped", "msg_id", msg.MessageId, "redelivered", msg.Redelivered)
		ack(msg, log) // idempotent: already processed, just ack
		return
	}
	ps.seen[seq] = struct{}{}
	switch {
	case seq < ps.lastSeq:
		ps.outOfOrder++
	case seq > ps.lastSeq+1:
		ps.gaps++
	}
	if seq > ps.lastSeq {
		ps.lastSeq = seq
	}
	v.mu.Unlock()

	if sentNs > 0 {
		v.lat.Add(time.Since(time.Unix(0, sentNs)))
	}
	// "Process" the message.
	if cfg.workMs > 0 {
		time.Sleep(time.Duration(cfg.workMs) * time.Millisecond)
	}
	unique.Add(1)
	log.Debug("processed", "msg_id", msg.MessageId, "seq", seq)
	ack(msg, log)
}

func ack(msg amqp.Delivery, log *slog.Logger) {
	if err := msg.Ack(false); err != nil {
		// Channel is gone; the broker will re-deliver this message and the
		// duplicate check above will make re-processing a no-op.
		ackErrors.Add(1)
		log.Warn("ack failed (will be redelivered)", "msg_id", msg.MessageId, "err", err.Error())
		return
	}
	acked.Add(1)
}

func (v *validator) summary(cfg *config) map[string]any {
	v.mu.Lock()
	defer v.mu.Unlock()
	per := map[string]any{}
	var missingTotal, ooo, gaps int64
	names := make([]string, 0, len(v.producers))
	for k := range v.producers {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		ps := v.producers[k]
		entry := map[string]any{"unique": len(ps.seen), "max_seq": ps.lastSeq, "out_of_order": ps.outOfOrder, "gaps_seen": ps.gaps}
		if cfg.expectPer > 0 {
			var missing []int64
			for i := int64(1); i <= int64(cfg.expectPer); i++ {
				if _, ok := ps.seen[i]; !ok {
					missing = append(missing, i)
				}
			}
			missingTotal += int64(len(missing))
			entry["missing_count"] = len(missing)
			if len(missing) > 20 {
				missing = missing[:20]
			}
			entry["missing_sample"] = missing
		}
		ooo += ps.outOfOrder
		gaps += ps.gaps
		per[k] = entry
	}
	evMu.Lock()
	ev := append([]event(nil), events...)
	evMu.Unlock()
	el := time.Since(start).Seconds()
	out := map[string]any{
		"run_id":           cfg.runID,
		"received":         received.Load(),
		"unique":           unique.Load(),
		"duplicates":       duplicates.Load(),
		"redelivered_flag": redelivered.Load(),
		"acked":            acked.Load(),
		"ack_errors":       ackErrors.Load(),
		"foreign":          v.foreign,
		"producers":        len(v.producers),
		"out_of_order":     ooo,
		"gaps_seen":        gaps,
		"failovers":        failovers.Load(),
		"switch_backs":     switchBacks.Load(),
		"events":           ev,
		"elapsed_s":        el,
		"e2e_latency":      v.lat.Summary(),
		"per_producer":     per,
	}
	if cfg.expectPer > 0 {
		out["missing_total"] = missingTotal
		wantProducers := 1
		if cfg.expect > 0 {
			wantProducers = cfg.expect / cfg.expectPer
		}
		out["expected_producers"] = wantProducers
		out["zero_loss"] = missingTotal == 0 && len(v.producers) >= wantProducers
	}
	return out
}
