// Producer: multi-worker stress-test publisher for the RabbitMQ quorum queue.
//
// Guarantees implemented:
//   - Publisher confirms (asynchronous, bounded in-flight window per worker).
//   - At-least-once delivery: every message stays in the worker's pending
//     window until the broker acks it. On nack, channel loss or node failure
//     the whole pending window is re-published in sequence order on a new
//     connection (possibly to another node). Duplicates this may create are
//     removed by the consumer's idempotency check.
//   - Connection failover across all cluster nodes with exponential backoff.
//   - Per-producer monotonically increasing sequence numbers so the consumer
//     can validate ordering, gaps, duplicates and loss.
//   - Structured JSON logs, per-second CSV time series and a JSON summary.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"amqp-homework/internal/amqpx"
	"amqp-homework/internal/stats"

	amqp "github.com/rabbitmq/amqp091-go"
)

type config struct {
	nodes     []string
	workers   int
	count     int
	rate      float64
	size      int
	inflight  int
	runID     string
	startNode int
	statsCSV  string
	summary   string
	timeout   time.Duration
	verbose   bool
}

// Global counters (shared by workers, read by the stats ticker).
var (
	published   atomic.Int64 // basic.publish calls incl. re-publishes
	confirmed   atomic.Int64 // unique messages acked by the broker
	republished atomic.Int64 // messages re-sent after nack/failure
	nacked      atomic.Int64
	returned    atomic.Int64
	reconnects  atomic.Int64
	confirmLat  stats.Recorder
)

type outMsg struct {
	seq    uint64
	pub    amqp.Publishing
	dc     *amqp.DeferredConfirmation
	sentAt time.Time
}

type worker struct {
	id     int
	cfg    *config
	log    *slog.Logger
	dialer *amqpx.Dialer
	node   atomic.Value // current node host
}

func main() {
	cfg := &config{}
	var nodes string
	flag.StringVar(&nodes, "nodes", amqpx.DefaultNodes, "comma-separated AMQP URLs of all cluster nodes")
	flag.IntVar(&cfg.workers, "workers", 4, "number of concurrent publisher workers (one connection+channel each)")
	flag.IntVar(&cfg.count, "count", 10000, "messages per worker")
	flag.Float64Var(&cfg.rate, "rate", 0, "total target publish rate in msg/s across all workers (0 = unlimited)")
	flag.IntVar(&cfg.size, "size", 512, "message body size in bytes")
	flag.IntVar(&cfg.inflight, "inflight", 100, "max unconfirmed messages per worker (1 = synchronous confirms)")
	flag.StringVar(&cfg.runID, "run-id", "", "run identifier stamped on every message (default: timestamp)")
	flag.IntVar(&cfg.startNode, "start-node", 0, "index of the node workers try first; workers are spread round-robin from here")
	flag.StringVar(&cfg.statsCSV, "stats-csv", "", "write per-second stats to this CSV file")
	flag.StringVar(&cfg.summary, "summary", "", "write final JSON summary to this file")
	flag.DurationVar(&cfg.timeout, "timeout", 10*time.Minute, "abort the run after this long")
	flag.BoolVar(&cfg.verbose, "v", false, "debug logging (one line per confirm)")
	flag.Parse()
	cfg.nodes = amqpx.ParseNodes(nodes)
	if cfg.runID == "" {
		cfg.runID = time.Now().Format("20060102-150405")
	}
	lvl := slog.LevelInfo
	if cfg.verbose {
		lvl = slog.LevelDebug
	}
	log := amqpx.NewLogger("producer", lvl).With("run_id", cfg.runID)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; log.Warn("signal received, stopping"); cancel() }()

	// Declare topology once (any node).
	d := &amqpx.Dialer{Nodes: cfg.nodes, Log: log, ConnName: "producer-setup"}
	s, err := d.Connect(ctx, cfg.startNode)
	if err != nil {
		log.Error("setup failed", "err", err.Error())
		os.Exit(1)
	}
	if err := amqpx.DeclareTopology(s.Ch); err != nil {
		log.Error("topology failed", "err", err.Error())
		os.Exit(1)
	}
	s.Close()

	log.Info("starting", "workers", cfg.workers, "count_per_worker", cfg.count, "rate", cfg.rate,
		"size", cfg.size, "inflight", cfg.inflight, "nodes", len(cfg.nodes))

	workers := make([]*worker, cfg.workers)
	for i := range workers {
		w := &worker{id: i, cfg: cfg, log: log.With("worker", i)}
		w.dialer = &amqpx.Dialer{Nodes: cfg.nodes, Log: w.log, ConnName: fmt.Sprintf("producer-%s-w%d", cfg.runID, i)}
		w.node.Store("-")
		workers[i] = w
	}

	series, err := stats.NewSeries(cfg.statsCSV, []string{"epoch_ms", "t_s", "confirmed_total", "confirmed_per_s", "published_total", "republished_total", "reconnects", "nodes"})
	if err != nil {
		log.Error("stats csv", "err", err.Error())
	}
	start := time.Now()
	stopTicker := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		var last int64
		for {
			select {
			case <-stopTicker:
				return
			case now := <-t.C:
				c := confirmed.Load()
				var ns []string
				for _, w := range workers {
					ns = append(ns, w.node.Load().(string))
				}
				el := now.Sub(start).Seconds()
				series.Row(now.UnixMilli(), el, c, float64(c-last), published.Load(), republished.Load(), reconnects.Load(), strings.Join(ns, "|"))
				log.Info("stats", "t_s", int(el), "confirmed", c, "confirmed_per_s", c-last,
					"republished", republished.Load(), "reconnects", reconnects.Load())
				last = c
			}
		}
	}()

	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		go func(w *worker) { defer wg.Done(); w.run(ctx) }(w)
	}
	wg.Wait()
	close(stopTicker)
	series.Close()

	elapsed := time.Since(start)
	total := int64(cfg.workers * cfg.count)
	sum := map[string]any{
		"run_id":           cfg.runID,
		"workers":          cfg.workers,
		"count_per_worker": cfg.count,
		"expected_total":   total,
		"confirmed_total":  confirmed.Load(),
		"published_calls":  published.Load(),
		"republished":      republished.Load(),
		"nacked":           nacked.Load(),
		"returned":         returned.Load(),
		"reconnects":       reconnects.Load(),
		"size_bytes":       cfg.size,
		"inflight":         cfg.inflight,
		"target_rate":      cfg.rate,
		"elapsed_s":        elapsed.Seconds(),
		"throughput_msg_s": float64(confirmed.Load()) / elapsed.Seconds(),
		"confirm_latency":  confirmLat.Summary(),
		"all_confirmed":    confirmed.Load() == total,
	}
	_ = stats.WriteJSON(cfg.summary, sum)
	log.Info("done", "summary", sum)
	if confirmed.Load() != total {
		os.Exit(2)
	}
}

func (w *worker) run(ctx context.Context) {
	cfg := w.cfg
	var pending []*outMsg
	next := uint64(1)
	done := uint64(0)
	body := make([]byte, cfg.size)
	for i := range body {
		body[i] = byte('a' + i%26)
	}
	var interval time.Duration
	if cfg.rate > 0 {
		interval = time.Duration(float64(time.Second) * float64(cfg.workers) / cfg.rate)
	}
	nextSend := time.Now()
	startIdx := (cfg.startNode + w.id) % len(cfg.nodes)
	first := true

	for done < uint64(cfg.count) {
		if ctx.Err() != nil {
			return
		}
		s, err := w.dialer.Connect(ctx, startIdx)
		if err != nil {
			return
		}
		if !first {
			reconnects.Add(1)
		}
		first = false
		w.node.Store(s.Node)
		if err := s.Ch.Confirm(false); err != nil {
			w.log.Warn("confirm.select failed", "err", err.Error())
			s.Abandon()
			continue
		}
		returns := s.Ch.NotifyReturn(make(chan amqp.Return, 64))
		go func() {
			for r := range returns {
				returned.Add(1)
				w.log.Warn("message returned (unroutable)", "msg_id", r.MessageId, "reply", r.ReplyText)
			}
		}()
		closed := s.Closed()

		publish := func(m *outMsg) error {
			m.sentAt = time.Now()
			m.pub.Headers["sent_ts_ns"] = m.sentAt.UnixNano()
			pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
			defer pcancel()
			dc, err := s.Ch.PublishWithDeferredConfirmWithContext(pctx, "", amqpx.QueueName, true, false, m.pub)
			if err != nil {
				return err
			}
			m.dc = dc
			published.Add(1)
			return nil
		}

		// sessionOK=false means the connection/channel is gone -> fail over
		// to the next node. A broker nack (e.g. the quorum queue had no
		// leader during an election) -> back off and re-send the window on a
		// fresh channel to the same node.
		sessionOK := true
		for sessionOK && done < uint64(cfg.count) {
			// 1) Re-publish everything still unconfirmed, in original order.
			if len(pending) > 0 {
				w.log.Info("re-publishing unconfirmed window", "count", len(pending), "first_seq", pending[0].seq, "node", s.Node)
				for _, m := range pending {
					if err := publish(m); err != nil {
						sessionOK = false
						break
					}
					republished.Add(1)
				}
			}
			// 2) Normal pipelined publishing loop.
			nack := false
			for sessionOK && !nack && done < uint64(cfg.count) {
				// Publish while the window has room and (when rate limited)
				// the next send slot has arrived. Never sleep here: confirms
				// must be processed while we wait for the next slot.
				for len(pending) < cfg.inflight && next <= uint64(cfg.count) {
					if interval > 0 {
						if time.Now().Before(nextSend) {
							break
						}
						nextSend = nextSend.Add(interval)
					}
					m := &outMsg{seq: next, pub: amqp.Publishing{
						DeliveryMode: amqp.Persistent,
						ContentType:  "application/octet-stream",
						MessageId:    fmt.Sprintf("%s-p%d-%d", cfg.runID, w.id, next),
						Timestamp:    time.Now(),
						Headers: amqp.Table{
							"run_id":      cfg.runID,
							"producer_id": fmt.Sprintf("%s-p%d", cfg.runID, w.id),
							"seq":         int64(next),
						},
						Body: body,
					}}
					pending = append(pending, m) // kept until acked
					next++
					if err := publish(m); err != nil {
						sessionOK = false
						w.log.Warn("publish failed", "seq", m.seq, "err", err.Error())
						break
					}
				}
				if !sessionOK || (len(pending) == 0 && next > uint64(cfg.count)) {
					break
				}
				// Wait for whichever comes first: the oldest confirm, the
				// next rate-limit slot, or a session failure.
				var headDone <-chan struct{}
				if len(pending) > 0 {
					headDone = pending[0].dc.Done()
				}
				var slot <-chan time.Time
				if interval > 0 && len(pending) < cfg.inflight && next <= uint64(cfg.count) {
					slot = time.After(time.Until(nextSend))
				}
				select {
				case <-ctx.Done():
					s.Close()
					return
				case <-slot:
				case e := <-closed:
					w.log.Warn("session lost, failing over", "node", s.Node, "err", fmt.Sprint(e), "unconfirmed", len(pending))
					sessionOK = false
				case <-headDone:
					head := pending[0]
					if head.dc.Acked() {
						confirmLat.Add(time.Since(head.sentAt))
						confirmed.Add(1)
						done++
						w.log.Debug("confirmed", "seq", head.seq)
						pending = pending[1:]
					} else {
						nacked.Add(1)
						w.log.Warn("nack received, backing off and re-publishing window", "seq", head.seq, "unconfirmed", len(pending), "node", s.Node)
						nack = true
					}
				}
			}
			if nack && sessionOK {
				// Re-open a fresh channel on the SAME node after a short
				// back-off: a channel that was nacked while the quorum queue
				// had no leader can keep nacking after the election, while a
				// new channel discovers the new leader immediately.
				break
			}
		}
		ok := sessionOK
		if sessionOK && done < uint64(cfg.count) { // nack path
			s.Abandon()
			select {
			case <-ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
			}
			startIdx = s.NodeIndex
			continue
		}
		if ok {
			s.Close() // finished normally: clean close
		} else {
			s.Abandon() // failure path: don't block on a broker that may not answer
		}
		if !ok {
			startIdx = (s.NodeIndex + 1) % len(cfg.nodes) // try the next node first
			time.Sleep(200 * time.Millisecond)
		}
	}
	w.log.Info("worker finished", "confirmed", done)
}
