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
	debugWriteStallsEnv             = "PEBBLE_DEBUG_WRITE_STALLS"
	debugSlowWritesEnv              = "PEBBLE_DEBUG_SLOW_WRITES"
	writeStallDiagnosticLogInterval = 5 * time.Second
	defaultSlowWriteLogThreshold    = 100 * time.Millisecond
)

var (
	writeStallDiagnosticsOnce     sync.Once
	writeStallDiagnosticsEnabledV bool
	writeStallDiagnosticCounter   atomic.Uint64
	writeStallDiagnosticActive    atomic.Bool

	slowWriteDiagnosticsOnce      sync.Once
	slowWriteDiagnosticsEnabledV  bool
	slowWriteDiagnosticThresholdV time.Duration
)

func writeStallDiagnosticsEnabled() bool {
	writeStallDiagnosticsOnce.Do(func() {
		switch strings.ToLower(strings.TrimSpace(os.Getenv(debugWriteStallsEnv))) {
		case "1", "t", "true", "y", "yes", "on":
			writeStallDiagnosticsEnabledV = true
		}
	})
	return writeStallDiagnosticsEnabledV
}

func nextWriteStallDiagnosticID() uint64 {
	return writeStallDiagnosticCounter.Add(1)
}

func bytesForWriteStallDiagnostics(v uint64) string {
	return string(humanize.Bytes.Uint64(v))
}

func writeStallFlushDiagnosticsEnabled() bool {
	return writeStallDiagnosticsEnabled() && writeStallDiagnosticActive.Load()
}

func slowWriteDiagnosticsConfig() (enabled bool, threshold time.Duration) {
	slowWriteDiagnosticsOnce.Do(func() {
		value := strings.TrimSpace(os.Getenv(debugSlowWritesEnv))
		if value == "" {
			return
		}
		switch strings.ToLower(value) {
		case "0", "f", "false", "n", "no", "off":
			return
		case "1", "t", "true", "y", "yes", "on":
			slowWriteDiagnosticsEnabledV = true
			slowWriteDiagnosticThresholdV = defaultSlowWriteLogThreshold
			return
		}
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			slowWriteDiagnosticsEnabledV = true
			slowWriteDiagnosticThresholdV = defaultSlowWriteLogThreshold
			return
		}
		slowWriteDiagnosticsEnabledV = true
		slowWriteDiagnosticThresholdV = d
	})
	return slowWriteDiagnosticsEnabledV, slowWriteDiagnosticThresholdV
}

func (d *DB) writeStallBlockersLocked(reason string) string {
	var blockers []string

	var queueTotal uint64
	var readyPrefix int
	var readyTotal uint64
	for i := range d.mu.mem.queue {
		queueTotal += d.mu.mem.queue[i].totalBytes()
		if i < len(d.mu.mem.queue)-1 && readyPrefix == i && d.mu.mem.queue[i].readyForFlush() {
			readyPrefix++
			readyTotal += d.mu.mem.queue[i].totalBytes()
		}
	}

	memThreshold := uint64(d.opts.MemTableStopWritesThreshold) * d.opts.MemTableSize
	blockers = append(blockers,
		fmt.Sprintf("memtable-limit: queued-bytes=%s >= %s and flushable work exists (ready-prefix=%d, ready-total=%s)",
			bytesForWriteStallDiagnostics(queueTotal),
			bytesForWriteStallDiagnostics(memThreshold),
			readyPrefix,
			bytesForWriteStallDiagnostics(readyTotal)))

	l0ReadAmp := d.mu.versions.currentVersion().L0Sublevels.ReadAmplification()
	if l0ReadAmp >= d.opts.L0StopWritesThreshold {
		blockers = append(blockers,
			fmt.Sprintf("l0-limit: read-amp=%d >= %d with %d compactions in progress (%s)",
				l0ReadAmp,
				d.opts.L0StopWritesThreshold,
				d.mu.compact.compactingCount,
				bytesForWriteStallDiagnostics(uint64(d.mu.versions.atomicInProgressBytes.Load()))))
	}

	return strings.Join(blockers, "; ")
}

func (d *DB) describeWriteStallEntryLocked(index int, entry *flushableEntry) string {
	desc := fmt.Sprintf("#%d type=%T ready=%t forced=%t total=%s inuse=%s log=%s log-seq=%d",
		index,
		entry.flushable,
		entry.readyForFlush(),
		entry.flushForced,
		bytesForWriteStallDiagnostics(entry.totalBytes()),
		bytesForWriteStallDiagnostics(entry.inuseBytes()),
		entry.logNum,
		entry.logSeqNum)

	switch t := entry.flushable.(type) {
	case *memTable:
		desc = fmt.Sprintf("%s writer-refs=%d", desc, t.writerRefs.Load())
	case *flushableBatch:
		desc = fmt.Sprintf("%s batch-count=%d seq=%d", desc, len(t.offsets), t.seqNum)
	case *ingestedFlushable:
		desc = fmt.Sprintf("%s files=%d", desc, len(t.files))
	}
	return desc
}

func (d *DB) writeStallStateLocked(reason string) string {
	var queueTotal uint64
	var queueInUse uint64
	var readyPrefix int
	var readyTotal uint64
	var readyInUse uint64
	var head []string

	for i := range d.mu.mem.queue {
		entry := d.mu.mem.queue[i]
		queueTotal += entry.totalBytes()
		queueInUse += entry.inuseBytes()
		if i < len(d.mu.mem.queue)-1 && readyPrefix == i && entry.readyForFlush() {
			readyPrefix++
			readyTotal += entry.totalBytes()
			readyInUse += entry.inuseBytes()
		}
		if i < 4 {
			head = append(head, d.describeWriteStallEntryLocked(i, entry))
		}
	}
	if len(d.mu.mem.queue) > 4 {
		head = append(head, "...")
	}

	oldestUnready := "none"
	if readyPrefix < len(d.mu.mem.queue)-1 {
		oldestUnready = d.describeWriteStallEntryLocked(readyPrefix, d.mu.mem.queue[readyPrefix])
	}

	mutable := "none"
	if len(d.mu.mem.queue) > 0 {
		mutable = d.describeWriteStallEntryLocked(len(d.mu.mem.queue)-1, d.mu.mem.queue[len(d.mu.mem.queue)-1])
	}

	return fmt.Sprintf(
		"blockers=%s queue=%d total=%s inuse=%s mem-threshold=%s ready-prefix=%d ready-total=%s ready-inuse=%s l0-read-amp=%d/%d flushing=%t compacting=%d in-progress=%s oldest-unready=%s mutable=%s head=[%s]",
		d.writeStallBlockersLocked(reason),
		len(d.mu.mem.queue),
		bytesForWriteStallDiagnostics(queueTotal),
		bytesForWriteStallDiagnostics(queueInUse),
		bytesForWriteStallDiagnostics(uint64(d.opts.MemTableStopWritesThreshold)*d.opts.MemTableSize),
		readyPrefix,
		bytesForWriteStallDiagnostics(readyTotal),
		bytesForWriteStallDiagnostics(readyInUse),
		d.mu.versions.currentVersion().L0Sublevels.ReadAmplification(),
		d.opts.L0StopWritesThreshold,
		d.mu.compact.flushing,
		d.mu.compact.compactingCount,
		bytesForWriteStallDiagnostics(uint64(d.mu.versions.atomicInProgressBytes.Load())),
		oldestUnready,
		mutable,
		strings.Join(head, "; "),
	)
}

func (d *DB) logWriteStallBeginLocked(stallID uint64, reason string) {
	if !writeStallDiagnosticsEnabled() {
		return
	}
	writeStallDiagnosticActive.Store(true)
	d.opts.Logger.Infof("write stall %d begin: reason=%s; %s", stallID, reason, d.writeStallStateLocked(reason))
}

func (d *DB) logWriteStallReasonChangeLocked(
	stallID uint64, prevReason string, newReason string, totalDuration time.Duration, wakeups int,
) {
	if !writeStallDiagnosticsEnabled() {
		return
	}
	d.opts.Logger.Infof(
		"write stall %d reason change after %s (wakeups=%d): %s -> %s; %s",
		stallID,
		totalDuration,
		wakeups,
		prevReason,
		newReason,
		d.writeStallStateLocked(newReason),
	)
}

func (d *DB) logWriteStallWakeLocked(
	stallID uint64, reason string, waitDuration time.Duration, totalDuration time.Duration, wakeups int,
) {
	if !writeStallDiagnosticsEnabled() {
		return
	}
	d.opts.Logger.Infof(
		"write stall %d woke after wait=%s total=%s wakeups=%d: reason=%s; %s",
		stallID,
		waitDuration,
		totalDuration,
		wakeups,
		reason,
		d.writeStallStateLocked(reason),
	)
}

func (d *DB) logWriteStallEndLocked(
	stallID uint64, reason string, totalDuration time.Duration, wakeups int,
) {
	if !writeStallDiagnosticsEnabled() {
		return
	}
	d.opts.Logger.Infof(
		"write stall %d end after %s (wakeups=%d): %s",
		stallID,
		totalDuration,
		wakeups,
		d.writeStallStateLocked(reason),
	)
	writeStallDiagnosticActive.Store(false)
}

func (d *DB) logFlushForWriteStallLocked(
	jobID int, phase string, inputs int, inputBytes uint64, ingest bool, err error,
) {
	if !writeStallFlushDiagnosticsEnabled() {
		return
	}
	errText := "nil"
	if err != nil {
		errText = err.Error()
	}
	d.opts.Logger.Infof(
		"write stall flush %s: job=%d inputs=%d input-bytes=%s ingest=%t err=%s; %s",
		phase,
		jobID,
		inputs,
		bytesForWriteStallDiagnostics(inputBytes),
		ingest,
		errText,
		d.writeStallStateLocked(""),
	)
}

func (d *DB) maybeLogSlowWrite(batch *Batch, syncWAL bool, noSyncWait bool, phase string) {
	enabled, threshold := slowWriteDiagnosticsConfig()
	if !enabled || batch == nil {
		return
	}

	stats := batch.CommitStats()
	if stats.TotalDuration < threshold {
		return
	}

	other := stats.TotalDuration -
		stats.SemaphoreWaitDuration -
		stats.WALQueueWaitDuration -
		stats.MemTableWriteStallDuration -
		stats.L0ReadAmpWriteStallDuration -
		stats.WALRotationDuration -
		stats.CommitWaitDuration
	if other < 0 {
		other = 0
	}

	d.mu.Lock()
	state := d.writeStallStateLocked("")
	d.mu.Unlock()

	d.opts.Logger.Infof(
		"slow write detected: phase=%s total=%s threshold=%s sync=%t no-sync-wait=%t count=%d repr=%s memtable-est=%s flushable=%t stats=[semaphore=%s wal-queue=%s memtable-stall=%s l0-stall=%s wal-rotation=%s commit-wait=%s other=%s]; %s",
		phase,
		stats.TotalDuration,
		threshold,
		syncWAL,
		noSyncWait,
		batch.Count(),
		bytesForWriteStallDiagnostics(uint64(len(batch.Repr()))),
		bytesForWriteStallDiagnostics(batch.memTableSize),
		batch.flushable != nil,
		stats.SemaphoreWaitDuration,
		stats.WALQueueWaitDuration,
		stats.MemTableWriteStallDuration,
		stats.L0ReadAmpWriteStallDuration,
		stats.WALRotationDuration,
		stats.CommitWaitDuration,
		other,
		state,
	)
}
