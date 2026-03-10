// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/internal/humanize"
)

const (
	debugReadDiagnosticsEnv       = "PEBBLE_DEBUG_READ_DIAGNOSTICS"
	readDiagnosticsSampleInterval = time.Second
	readDiagnosticsMaxSamples     = 64
)

type envSetting uint8

const (
	envSettingUnset envSetting = iota
	envSettingEnabled
	envSettingDisabled
)

var (
	readDiagnosticsOnce     sync.Once
	readDiagnosticsEnabledV bool
)

func parseBoolEnvSetting(name string) envSetting {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return envSettingUnset
	}
	switch strings.ToLower(value) {
	case "1", "t", "true", "y", "yes", "on":
		return envSettingEnabled
	case "0", "f", "false", "n", "no", "off":
		return envSettingDisabled
	default:
		return envSettingEnabled
	}
}

func readDiagnosticsEnabled() bool {
	readDiagnosticsOnce.Do(func() {
		switch parseBoolEnvSetting(debugReadDiagnosticsEnv) {
		case envSettingEnabled:
			readDiagnosticsEnabledV = true
			return
		case envSettingDisabled:
			readDiagnosticsEnabledV = false
			return
		}
		slowWritesEnabled, _ := slowWriteDiagnosticsConfig()
		readDiagnosticsEnabledV = slowWritesEnabled || writeStallDiagnosticsEnabled()
	})
	return readDiagnosticsEnabledV
}

type readOpKind uint8

const (
	readOpGet readOpKind = iota
	readOpIterOpen
	readOpIterStep
	readOpIterValue
	readOpKindCount
)

func (k readOpKind) shortLabel() string {
	switch k {
	case readOpGet:
		return "get"
	case readOpIterOpen:
		return "open"
	case readOpIterStep:
		return "step"
	case readOpIterValue:
		return "value"
	default:
		return fmt.Sprintf("kind-%d", k)
	}
}

type readDiagnosticsCounters struct {
	count              atomic.Uint64
	totalLatencyNanos  atomic.Uint64
	slow1msCount       atomic.Uint64
	slow10msCount      atomic.Uint64
	slow100msCount     atomic.Uint64
	maxLatencyNanos    atomic.Uint64
	maxIntervalLatency atomic.Uint64
}

type readDiagnosticsOpSample struct {
	count               uint64
	totalLatencyNanos   uint64
	slow1msCount        uint64
	slow10msCount       uint64
	slow100msCount      uint64
	maxLatencyNanos     uint64
	maxIntervalLatencyN uint64
}

type readDiagnosticsSample struct {
	at time.Time

	ops [readOpKindCount]readDiagnosticsOpSample

	blockHits    int64
	blockMisses  int64
	tableHits    int64
	tableMisses  int64
	filterHits   int64
	filterMisses int64

	blockSize     int64
	blockReserved int64
	blockTarget   int64
	memReserved   int64
}

type readDiagnostics struct {
	db *DB

	ops [readOpKindCount]readDiagnosticsCounters

	mu struct {
		sync.Mutex
		next  int
		count int
		ring  [readDiagnosticsMaxSamples]readDiagnosticsSample
	}
}

type readDiagnosticsWindowSummary struct {
	window time.Duration
	span   time.Duration

	totalCount         uint64
	getCount           uint64
	iterOpenCount      uint64
	iterStepCount      uint64
	iterValueCount     uint64
	totalLatencyNanos  uint64
	slow1msCount       uint64
	slow10msCount      uint64
	slow100msCount     uint64
	maxLatencyNanos    uint64
	blockHits          int64
	blockMisses        int64
	tableHits          int64
	tableMisses        int64
	filterHits         int64
	filterMisses       int64
	currentReserved    int64
	maxReserved        int64
	currentFreeTarget  int64
	minFreeTarget      int64
	currentMemReserved int64
	maxMemReserved     int64
}

func newReadDiagnostics(d *DB) *readDiagnostics {
	return &readDiagnostics{db: d}
}

func (r *readDiagnostics) start() {
	r.captureSample(time.Now())
	go r.sampleLoop()
}

func (r *readDiagnostics) sampleLoop() {
	ticker := time.NewTicker(readDiagnosticsSampleInterval)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			r.captureSample(t)
		case <-r.db.closedCh:
			return
		}
	}
}

func updateAtomicMax(dst *atomic.Uint64, value uint64) {
	for {
		current := dst.Load()
		if current >= value {
			return
		}
		if dst.CompareAndSwap(current, value) {
			return
		}
	}
}

func (r *readDiagnostics) record(kind readOpKind, duration time.Duration) {
	if r == nil {
		return
	}
	nanos := uint64(duration)
	counters := &r.ops[kind]
	counters.count.Add(1)
	counters.totalLatencyNanos.Add(nanos)
	if duration > time.Millisecond {
		counters.slow1msCount.Add(1)
	}
	if duration > 10*time.Millisecond {
		counters.slow10msCount.Add(1)
	}
	if duration > 100*time.Millisecond {
		counters.slow100msCount.Add(1)
	}
	updateAtomicMax(&counters.maxLatencyNanos, nanos)
	updateAtomicMax(&counters.maxIntervalLatency, nanos)
}

func (r *readDiagnostics) snapshot(now time.Time, resetIntervalMax bool) readDiagnosticsSample {
	blockMetrics := r.db.opts.Cache.Metrics()
	tableMetrics, filterMetrics := r.db.tableCache.metrics()
	snapshot := readDiagnosticsSample{
		at:            now,
		blockHits:     blockMetrics.Hits,
		blockMisses:   blockMetrics.Misses,
		tableHits:     tableMetrics.Hits,
		tableMisses:   tableMetrics.Misses,
		filterHits:    filterMetrics.Hits,
		filterMisses:  filterMetrics.Misses,
		blockSize:     blockMetrics.Size,
		blockReserved: blockMetrics.ReservedSize,
		blockTarget:   blockMetrics.TargetSize,
		memReserved:   r.db.memTableReserved.Load(),
	}
	for kind := readOpKind(0); kind < readOpKindCount; kind++ {
		counters := &r.ops[kind]
		maxInterval := uint64(0)
		if resetIntervalMax {
			maxInterval = counters.maxIntervalLatency.Swap(0)
		} else {
			maxInterval = counters.maxIntervalLatency.Load()
		}
		snapshot.ops[kind] = readDiagnosticsOpSample{
			count:               counters.count.Load(),
			totalLatencyNanos:   counters.totalLatencyNanos.Load(),
			slow1msCount:        counters.slow1msCount.Load(),
			slow10msCount:       counters.slow10msCount.Load(),
			slow100msCount:      counters.slow100msCount.Load(),
			maxLatencyNanos:     counters.maxLatencyNanos.Load(),
			maxIntervalLatencyN: maxInterval,
		}
	}
	return snapshot
}

func (r *readDiagnostics) captureSample(now time.Time) {
	snapshot := r.snapshot(now, true /* resetIntervalMax */)
	r.mu.Lock()
	r.mu.ring[r.mu.next] = snapshot
	r.mu.next = (r.mu.next + 1) % len(r.mu.ring)
	if r.mu.count < len(r.mu.ring) {
		r.mu.count++
	}
	r.mu.Unlock()
}

func (r *readDiagnostics) orderedSamples() []readDiagnosticsSample {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.count == 0 {
		return nil
	}
	samples := make([]readDiagnosticsSample, 0, r.mu.count)
	start := (r.mu.next - r.mu.count + len(r.mu.ring)) % len(r.mu.ring)
	for i := 0; i < r.mu.count; i++ {
		samples = append(samples, r.mu.ring[(start+i)%len(r.mu.ring)])
	}
	return samples
}

func (r *readDiagnostics) summarize(window time.Duration) readDiagnosticsWindowSummary {
	now := time.Now()
	current := r.snapshot(now, false /* resetIntervalMax */)
	samples := r.orderedSamples()

	summary := readDiagnosticsWindowSummary{
		window:             window,
		currentReserved:    current.blockReserved,
		maxReserved:        current.blockReserved,
		currentFreeTarget:  current.blockTarget - current.blockSize,
		minFreeTarget:      current.blockTarget - current.blockSize,
		currentMemReserved: current.memReserved,
		maxMemReserved:     current.memReserved,
	}

	start := now.Add(-window)
	var base readDiagnosticsSample
	haveBase := false
	for _, sample := range samples {
		if !sample.at.After(start) {
			base = sample
			haveBase = true
		}
		if sample.at.After(start) {
			freeTarget := sample.blockTarget - sample.blockSize
			if sample.blockReserved > summary.maxReserved {
				summary.maxReserved = sample.blockReserved
			}
			if freeTarget < summary.minFreeTarget {
				summary.minFreeTarget = freeTarget
			}
			if sample.memReserved > summary.maxMemReserved {
				summary.maxMemReserved = sample.memReserved
			}
		}
	}
	if !haveBase && len(samples) > 0 {
		base = samples[0]
		haveBase = true
	}
	if haveBase {
		summary.span = current.at.Sub(base.at)
	}

	summary.blockHits = current.blockHits - base.blockHits
	summary.blockMisses = current.blockMisses - base.blockMisses
	summary.tableHits = current.tableHits - base.tableHits
	summary.tableMisses = current.tableMisses - base.tableMisses
	summary.filterHits = current.filterHits - base.filterHits
	summary.filterMisses = current.filterMisses - base.filterMisses

	for kind := readOpKind(0); kind < readOpKindCount; kind++ {
		deltaCount := current.ops[kind].count - base.ops[kind].count
		deltaLatency := current.ops[kind].totalLatencyNanos - base.ops[kind].totalLatencyNanos
		deltaSlow1ms := current.ops[kind].slow1msCount - base.ops[kind].slow1msCount
		deltaSlow10ms := current.ops[kind].slow10msCount - base.ops[kind].slow10msCount
		deltaSlow100ms := current.ops[kind].slow100msCount - base.ops[kind].slow100msCount

		summary.totalCount += deltaCount
		summary.totalLatencyNanos += deltaLatency
		summary.slow1msCount += deltaSlow1ms
		summary.slow10msCount += deltaSlow10ms
		summary.slow100msCount += deltaSlow100ms

		switch kind {
		case readOpGet:
			summary.getCount = deltaCount
		case readOpIterOpen:
			summary.iterOpenCount = deltaCount
		case readOpIterStep:
			summary.iterStepCount = deltaCount
		case readOpIterValue:
			summary.iterValueCount = deltaCount
		}

		maxLatency := current.ops[kind].maxIntervalLatencyN
		for _, sample := range samples {
			if sample.at.After(start) && sample.ops[kind].maxIntervalLatencyN > maxLatency {
				maxLatency = sample.ops[kind].maxIntervalLatencyN
			}
		}
		if maxLatency > summary.maxLatencyNanos {
			summary.maxLatencyNanos = maxLatency
		}
	}
	return summary
}

func formatReadDurationAverage(totalNanos uint64, count uint64) time.Duration {
	if count == 0 {
		return 0
	}
	return time.Duration(totalNanos / count)
}

func (s readDiagnosticsWindowSummary) format() string {
	blockHitRate := percent(s.blockHits, s.blockHits+s.blockMisses)
	tableHitRate := percent(s.tableHits, s.tableHits+s.tableMisses)
	filterHitRate := percent(s.filterHits, s.filterHits+s.filterMisses)
	return fmt.Sprintf(
		"%s span=%s ops=%s(get=%s open=%s step=%s value=%s) avg=%s max=%s >1ms=%s >10ms=%s >100ms=%s cache={block=%s/%s %.1f%% table=%.1f%% filter=%.1f%% reserved=%s max-reserved=%s free=%s min-free=%s mem-reserved=%s max-mem=%s}",
		s.window,
		s.span,
		humanize.Count.Uint64(s.totalCount),
		humanize.Count.Uint64(s.getCount),
		humanize.Count.Uint64(s.iterOpenCount),
		humanize.Count.Uint64(s.iterStepCount),
		humanize.Count.Uint64(s.iterValueCount),
		formatReadDurationAverage(s.totalLatencyNanos, s.totalCount),
		time.Duration(s.maxLatencyNanos),
		humanize.Count.Uint64(s.slow1msCount),
		humanize.Count.Uint64(s.slow10msCount),
		humanize.Count.Uint64(s.slow100msCount),
		humanize.Count.Int64(s.blockHits),
		humanize.Count.Int64(s.blockMisses),
		blockHitRate,
		tableHitRate,
		filterHitRate,
		signedBytesForWriteStallDiagnostics(s.currentReserved),
		signedBytesForWriteStallDiagnostics(s.maxReserved),
		signedBytesForWriteStallDiagnostics(s.currentFreeTarget),
		signedBytesForWriteStallDiagnostics(s.minFreeTarget),
		signedBytesForWriteStallDiagnostics(s.currentMemReserved),
		signedBytesForWriteStallDiagnostics(s.maxMemReserved),
	)
}

func (r *readDiagnostics) formatForEvent() string {
	oneSecond := r.summarize(time.Second)
	fiveSeconds := r.summarize(5 * time.Second)
	return fmt.Sprintf("read-window={1s %s; 5s %s}", oneSecond.format(), fiveSeconds.format())
}

func (r *readDiagnostics) populateMetrics(metrics *Metrics) {
	if r == nil || metrics == nil {
		return
	}
	current := r.snapshot(time.Now(), false /* resetIntervalMax */)
	for kind := readOpKind(0); kind < readOpKindCount; kind++ {
		sample := current.ops[kind]
		metrics.Read.Count += sample.count
		metrics.Read.TotalDuration += time.Duration(sample.totalLatencyNanos)
		metrics.Read.Slow1msCount += sample.slow1msCount
		metrics.Read.Slow10msCount += sample.slow10msCount
		metrics.Read.Slow100msCount += sample.slow100msCount
		if maxDuration := time.Duration(sample.maxLatencyNanos); maxDuration > metrics.Read.MaxDuration {
			metrics.Read.MaxDuration = maxDuration
		}
		switch kind {
		case readOpGet:
			metrics.Read.GetCount = sample.count
		case readOpIterOpen:
			metrics.Read.IterOpenCount = sample.count
		case readOpIterStep:
			metrics.Read.IterStepCount = sample.count
		case readOpIterValue:
			metrics.Read.IterValueCount = sample.count
		}
	}
}

func (d *DB) maybeInitReadDiagnostics() {
	if !readDiagnosticsEnabled() || d.readDiagnostics != nil {
		return
	}
	d.readDiagnostics = newReadDiagnostics(d)
	d.readDiagnostics.start()
}

func (d *DB) maybeRecordRead(kind readOpKind, duration time.Duration) {
	if d == nil || d.readDiagnostics == nil {
		return
	}
	d.readDiagnostics.record(kind, duration)
}

func (d *DB) formatReadDiagnosticsForEvent() string {
	if d == nil || d.readDiagnostics == nil {
		return "read-window={disabled}"
	}
	return d.readDiagnostics.formatForEvent()
}

func (i *Iterator) readDiagnosticsDB() *DB {
	if i.readState != nil {
		return i.readState.db
	}
	if i.batch != nil {
		return i.batch.db
	}
	return nil
}

func (i *Iterator) startReadOperation(kind readOpKind) (*DB, time.Time) {
	db := i.readDiagnosticsDB()
	if db == nil || db.readDiagnostics == nil {
		return nil, time.Time{}
	}
	return db, time.Now()
}

func finishReadOperation(db *DB, kind readOpKind, start time.Time) {
	if db == nil {
		return
	}
	db.maybeRecordRead(kind, time.Since(start))
}
