// clustermon polls the RabbitMQ management HTTP API of every node and records
// node availability and the quorum queue's Raft leader / online members at a
// high frequency. It is used during chaos tests to measure leader-election
// and recovery times with millisecond resolution.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"amqp-homework/internal/amqpx"
	"amqp-homework/internal/stats"
)

type queueInfo struct {
	Leader  string   `json:"leader"`
	Members []string `json:"members"`
	Online  []string `json:"online"`
	State   string   `json:"state"`
	Msgs    int64    `json:"messages"`
}

func main() {
	var mgmt, queue, user, pass, out string
	var interval, duration time.Duration
	flag.StringVar(&mgmt, "mgmt", "http://localhost:15672,http://localhost:15673,http://localhost:15674", "management API base URLs, one per node")
	flag.StringVar(&queue, "queue", amqpx.QueueName, "quorum queue to watch")
	flag.StringVar(&user, "user", "admin", "management user")
	flag.StringVar(&pass, "pass", "admin", "management password")
	flag.StringVar(&out, "csv", "", "output CSV")
	flag.DurationVar(&interval, "interval", 250*time.Millisecond, "poll interval")
	flag.DurationVar(&duration, "duration", 10*time.Minute, "stop after")
	flag.Parse()
	log := amqpx.NewLogger("clustermon", slog.LevelInfo)
	urls := amqpx.ParseNodes(mgmt)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; cancel() }()

	hdr := []string{"epoch_ms", "t_s", "leader", "online_members", "messages"}
	for i := range urls {
		hdr = append(hdr, fmt.Sprintf("node%d_up", i+1))
	}
	series, err := stats.NewSeries(out, hdr)
	if err != nil {
		log.Error("csv", "err", err.Error())
		os.Exit(1)
	}
	defer series.Close()

	client := &http.Client{Timeout: 800 * time.Millisecond}
	get := func(base, path string, v any) error {
		req, _ := http.NewRequestWithContext(ctx, "GET", base+path, nil)
		req.SetBasicAuth(user, pass)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return fmt.Errorf("status %d", resp.StatusCode)
		}
		if v == nil {
			return nil
		}
		return json.NewDecoder(resp.Body).Decode(v)
	}
	start := time.Now()
	prevLeader := ""
	t := time.NewTicker(interval)
	defer t.Stop()
	qpath := "/api/queues/" + url.PathEscape("/") + "/" + queue
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			// Query all nodes in parallel. Node liveness uses a node-local
			// health check; the queue view (leader/members) is cluster-wide
			// and can block during a partition until net_ticktime expires,
			// so it is taken from whichever node answers first.
			type res struct {
				i  int
				up bool
				q  *queueInfo
			}
			ch := make(chan res, len(urls))
			for i, u := range urls {
				go func(i int, u string) {
					r := res{i: i}
					if get(u, "/api/health/checks/local-alarms", nil) == nil {
						r.up = true
						var q queueInfo
						if get(u, qpath, &q) == nil {
							r.q = &q
						}
					}
					ch <- r
				}(i, u)
			}
			ups := make([]any, len(urls))
			var qi *queueInfo
			for range urls {
				r := <-ch
				if r.up {
					ups[r.i] = 1
				} else {
					ups[r.i] = 0
				}
				if qi == nil && r.q != nil {
					qi = r.q
				}
			}
			leader, online, msgs := "unknown", 0, int64(-1)
			if qi != nil {
				leader, online, msgs = short(qi.Leader), len(qi.Online), qi.Msgs
			}
			row := []any{now.UnixMilli(), now.Sub(start).Seconds(), leader, online, msgs}
			row = append(row, ups...)
			series.Row(row...)
			if leader != prevLeader {
				log.Info("leader changed", "from", prevLeader, "to", leader, "online_members", online)
				prevLeader = leader
			}
		}
	}
}

func short(n string) string { return strings.TrimPrefix(n, "rabbit@") }
