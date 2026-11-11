// Package stats provides a thread-safe latency recorder with percentile
// summaries and a per-second time-series writer (CSV) used by the benchmark
// and chaos reports.
package stats

import (
	"encoding/csv"
	"encoding/json"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Recorder stores latency samples in microseconds.
type Recorder struct {
	mu      sync.Mutex
	samples []float64
}

func (r *Recorder) Add(d time.Duration) {
	r.mu.Lock()
	r.samples = append(r.samples, float64(d.Microseconds()))
	r.mu.Unlock()
}

// Summary is a latency distribution in milliseconds.
type Summary struct {
	Count int     `json:"count"`
	MinMs float64 `json:"min_ms"`
	AvgMs float64 `json:"avg_ms"`
	P50Ms float64 `json:"p50_ms"`
	P95Ms float64 `json:"p95_ms"`
	P99Ms float64 `json:"p99_ms"`
	P999  float64 `json:"p999_ms"`
	MaxMs float64 `json:"max_ms"`
}

func (r *Recorder) Summary() Summary {
	r.mu.Lock()
	s := append([]float64(nil), r.samples...)
	r.mu.Unlock()
	if len(s) == 0 {
		return Summary{}
	}
	sort.Float64s(s)
	sum := 0.0
	for _, v := range s {
		sum += v
	}
	p := func(q float64) float64 {
		i := int(math.Ceil(q*float64(len(s)))) - 1
		if i < 0 {
			i = 0
		}
		return round(s[i] / 1000)
	}
	return Summary{
		Count: len(s),
		MinMs: round(s[0] / 1000),
		AvgMs: round(sum / float64(len(s)) / 1000),
		P50Ms: p(0.50), P95Ms: p(0.95), P99Ms: p(0.99), P999: p(0.999),
		MaxMs: round(s[len(s)-1] / 1000),
	}
}

func round(v float64) float64 { return math.Round(v*1000) / 1000 }

// Series writes one CSV row per tick.
type Series struct {
	mu sync.Mutex
	f  *os.File
	w  *csv.Writer
}

func NewSeries(path string, header []string) (*Series, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := csv.NewWriter(f)
	_ = w.Write(header)
	return &Series{f: f, w: w}, nil
}

func (s *Series) Row(vals ...any) {
	if s == nil {
		return
	}
	row := make([]string, len(vals))
	for i, v := range vals {
		switch t := v.(type) {
		case string:
			row[i] = t
		case int:
			row[i] = strconv.Itoa(t)
		case int64:
			row[i] = strconv.FormatInt(t, 10)
		case uint64:
			row[i] = strconv.FormatUint(t, 10)
		case float64:
			row[i] = strconv.FormatFloat(t, 'f', 3, 64)
		default:
			b, _ := json.Marshal(t)
			row[i] = string(b)
		}
	}
	s.mu.Lock()
	_ = s.w.Write(row)
	s.w.Flush()
	s.mu.Unlock()
}

func (s *Series) Close() {
	if s == nil {
		return
	}
	s.w.Flush()
	_ = s.f.Close()
}

// WriteJSON writes v as indented JSON to path (no-op if path is empty).
func WriteJSON(path string, v any) error {
	if path == "" {
		return nil
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
