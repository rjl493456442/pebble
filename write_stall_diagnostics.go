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
	debugSlowFlushesEnv             = "PEBBLE_DEBUG_SLOW_FLUSHES"
	writeStallDiagnosticLogInterval = 5 * time.Second
	defaultSlowWriteLogThreshold    = 100 * time.Millisecond
	defaultSlowFlushLogThreshold    = 5 * time.Second
)

var (
	writeStallDiagnosticsOnce     sync.Once
	writeStallDiagnosticsEnabledV bool
	writeStallDiagnosticCounter   atomic.Uint64
	writeStallDiagnosticActive    atomic.Bool

	slowWriteDiagnosticsOnce      sync.Once
	slowWriteDiagnosticsEnabledV  bool
	slowWriteDiagnosticThresholdV time.Duration

	slowFlushDiagnosticsOnce      sync.Once
	slowFlushDiagnosticsEnabledV  bool
	slowFlushDiagnosticThresholdV time.Duration
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

func signedBytesForWriteStallDiagnostics(v int64) string {
	if v < 0 {
		return "-" + bytesForWriteStallDiagnostics(uint64(-v))
	}
	return bytesForWriteStallDiagnostics(uint64(v))
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

func slowFlushDiagnosticsConfig() (enabled bool, threshold time.Duration) {
	slowFlushDiagnosticsOnce.Do(func() {
		value := strings.TrimSpace(os.Getenv(debugSlowFlushesEnv))
		if value == "" {
			return
		}
		switch strings.ToLower(value) {
		case "0", "f", "false", "n", "no", "off":
			return
		case "1", "t", "true", "y", "yes", "on":
			slowFlushDiagnosticsEnabledV = true
			slowFlushDiagnosticThresholdV = defaultSlowFlushLogThreshold
			return
		}
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			slowFlushDiagnosticsEnabledV = true
			slowFlushDiagnosticThresholdV = defaultSlowFlushLogThreshold
			return
		}
		slowFlushDiagnosticsEnabledV = true
		slowFlushDiagnosticThresholdV = d
	})
	return slowFlushDiagnosticsEnabledV, slowFlushDiagnosticThresholdV
}

func detailedCommitBreakdownEnabled() bool {
	enabled, _ := slowWriteDiagnosticsConfig()
	return enabled || writeStallDiagnosticsEnabled()
}

// flushStepTiming tracks the duration of each major step in a flush operation.
type flushStepTiming struct {
	scanQueue      time.Duration
	runCompaction  time.Duration
	logLockWait    time.Duration
	runIngestFlush time.Duration
	logAndApply    time.Duration
	clearState     time.Duration
	updateMemQueue time.Duration
	updateReadState time.Duration
	deleteObsolete time.Duration
	readerUnref    time.Duration
	markFlushed    time.Duration
}

func (d *DB) logSlowFlushStepLocked(jobID int, step string, inputBytes uint64) {
	enabled, _ := slowFlushDiagnosticsConfig()
	if !enabled {
		return
	}
	d.opts.Logger.Infof(
		"slow flush step | job=%d step=%s input-bytes=%s | %s",
		jobID,
		step,
		bytesForWriteStallDiagnostics(inputBytes),
		d.writeStallStateLocked(""),
	)
}

func (d *DB) logSlowFlushStepUnlocked(jobID int, step string, inputBytes uint64) {
	enabled, _ := slowFlushDiagnosticsConfig()
	if !enabled {
		return
	}
	d.opts.Logger.Infof(
		"slow flush step | job=%d step=%s input-bytes=%s",
		jobID,
		step,
		bytesForWriteStallDiagnostics(inputBytes),
	)
}

func (d *DB) logSlowFlushBreakdownLocked(
	jobID int, inputs int, inputBytes uint64, ingest bool, totalDuration time.Duration,
	timing flushStepTiming, err error,
) {
	enabled, threshold := slowFlushDiagnosticsConfig()
	if !enabled || totalDuration < threshold {
		return
	}
	errText := "nil"
	if err != nil {
		errText = err.Error()
	}
	residual := totalDuration -
		timing.scanQueue -
		timing.runCompaction -
		timing.logLockWait -
		timing.runIngestFlush -
		timing.logAndApply -
		timing.clearState -
		timing.updateMemQueue -
		timing.updateReadState -
		timing.deleteObsolete -
		timing.readerUnref -
		timing.markFlushed
	if residual < 0 {
		residual = 0
	}
	d.opts.Logger.Infof(
		"slow flush breakdown | job=%d total=%s threshold=%s inputs=%d input-bytes=%s ingest=%t err=%s | "+
			"scan-queue=%s run-compaction=%s log-lock-wait=%s ingest-flush=%s log-and-apply=%s "+
			"clear-state=%s update-mem-queue=%s update-read-state=%s delete-obsolete=%s "+
			"reader-unref=%s mark-flushed=%s residual=%s | %s",
		jobID,
		totalDuration,
		threshold,
		inputs,
		bytesForWriteStallDiagnostics(inputBytes),
		ingest,
		errText,
		timing.scanQueue,
		timing.runCompaction,
		timing.logLockWait,
		timing.runIngestFlush,
		timing.logAndApply,
		timing.clearState,
		timing.updateMemQueue,
		timing.updateReadState,
		timing.deleteObsolete,
		timing.readerUnref,
		timing.markFlushed,
		residual,
		d.writeStallStateLocked(""),
	)
}

func (d *DB) logFlushNoReadyLocked() {
	enabled, _ := slowFlushDiagnosticsConfig()
	if !enabled {
		return
	}
	if len(d.mu.mem.queue) <= 1 {
		return
	}
	var entries []string
	for i := 0; i < len(d.mu.mem.queue)-1 && i < 5; i++ {
		entries = append(entries, d.describeWriteStallEntryLocked(i, d.mu.mem.queue[i]))
	}
	d.opts.Logger.Infof(
		"slow flush no-ready | queue-len=%d flushing=%t | entries=[%s]",
		len(d.mu.mem.queue),
		d.mu.compact.flushing,
		strings.Join(entries, " | "),
	)
}

func flushableTypeLabel(f flushable) string {
	switch f.(type) {
	case *memTable:
		return "mem"
	case *flushableBatch:
		return "fbatch"
	case *ingestedFlushable:
		return "ingest"
	default:
		return fmt.Sprintf("%T", f)
	}
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
	if queueTotal >= memThreshold {
		switch {
		case readyPrefix == 0 && len(d.mu.mem.queue) > 1:
			blockers = append(blockers,
				fmt.Sprintf("memtable-limit queued=%s >= %s, oldest-immutable-not-ready",
					bytesForWriteStallDiagnostics(queueTotal),
					bytesForWriteStallDiagnostics(memThreshold)))
		case d.mu.compact.flushing:
			blockers = append(blockers,
				fmt.Sprintf("memtable-limit queued=%s >= %s, flush=in-progress, ready=%d/%s",
					bytesForWriteStallDiagnostics(queueTotal),
					bytesForWriteStallDiagnostics(memThreshold),
					readyPrefix,
					bytesForWriteStallDiagnostics(readyTotal)))
		case readyPrefix > 0:
			blockers = append(blockers,
				fmt.Sprintf("memtable-limit queued=%s >= %s, flush=not-running, ready=%d/%s",
					bytesForWriteStallDiagnostics(queueTotal),
					bytesForWriteStallDiagnostics(memThreshold),
					readyPrefix,
					bytesForWriteStallDiagnostics(readyTotal)))
		default:
			blockers = append(blockers,
				fmt.Sprintf("memtable-limit queued=%s >= %s",
					bytesForWriteStallDiagnostics(queueTotal),
					bytesForWriteStallDiagnostics(memThreshold)))
		}
	}

	l0ReadAmp := d.mu.versions.currentVersion().L0Sublevels.ReadAmplification()
	if l0ReadAmp >= d.opts.L0StopWritesThreshold {
		blockers = append(blockers,
			fmt.Sprintf("l0-limit amp=%d >= %d, compactions=%d, in-progress=%s",
				l0ReadAmp,
				d.opts.L0StopWritesThreshold,
				d.mu.compact.compactingCount,
				bytesForWriteStallDiagnostics(uint64(d.mu.versions.atomicInProgressBytes.Load()))))
	}

	if len(blockers) == 0 {
		if reason == "" {
			return "cleared"
		}
		return reason + " cleared"
	}
	return strings.Join(blockers, " | ")
}

func (d *DB) describeWriteStallEntryLocked(index int, entry *flushableEntry) string {
	parts := []string{
		fmt.Sprintf("#%d:%s", index, flushableTypeLabel(entry.flushable)),
		fmt.Sprintf("total=%s", bytesForWriteStallDiagnostics(entry.totalBytes())),
		fmt.Sprintf("inuse=%s", bytesForWriteStallDiagnostics(entry.inuseBytes())),
		fmt.Sprintf("log=%s", entry.logNum),
	}
	if entry.readyForFlush() {
		parts = append(parts, "ready")
	}
	if entry.flushForced {
		parts = append(parts, "forced")
	}

	switch t := entry.flushable.(type) {
	case *memTable:
		parts = append(parts, fmt.Sprintf("refs=%d", t.writerRefs.Load()))
	case *flushableBatch:
		parts = append(parts, fmt.Sprintf("count=%d", len(t.offsets)))
	case *ingestedFlushable:
		parts = append(parts, fmt.Sprintf("files=%d", len(t.files)))
	}
	return strings.Join(parts, ",")
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
		mutable = d.describeWriteStallEntryLocked(len(d.mu.mem.queue)-1, d.mu.mem.queue[len(d.mu.mem.queue)-1]) + ",mutable"
	}

	return fmt.Sprintf(
		"blockers=[%s] | queue={n=%d total=%s inuse=%s threshold=%s ready=%d ready-total=%s ready-inuse=%s} | activity={l0=%d/%d flushing=%t compacting=%d in-progress=%s} | oldest-unready={%s} | mutable={%s} | head=[%s]",
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
		strings.Join(head, " | "),
	)
}

func (d *DB) logWriteStallBeginLocked(stallID uint64, reason string) {
	if !writeStallDiagnosticsEnabled() {
		return
	}
	writeStallDiagnosticActive.Store(true)
	d.opts.Logger.Infof(
		"write stall begin | id=%d reason=%s | %s | %s",
		stallID,
		reason,
		d.writeStallStateLocked(reason),
		d.formatReadDiagnosticsForEvent(),
	)
}

func (d *DB) logWriteStallReasonChangeLocked(
	stallID uint64, prevReason string, newReason string, totalDuration time.Duration, wakeups int,
) {
	if !writeStallDiagnosticsEnabled() {
		return
	}
	d.opts.Logger.Infof(
		"write stall reason-change | id=%d total=%s wakeups=%d from=%s to=%s | %s | %s",
		stallID,
		totalDuration,
		wakeups,
		prevReason,
		newReason,
		d.writeStallStateLocked(newReason),
		d.formatReadDiagnosticsForEvent(),
	)
}

func (d *DB) logWriteStallWakeLocked(
	stallID uint64, reason string, waitDuration time.Duration, totalDuration time.Duration, wakeups int,
) {
	if !writeStallDiagnosticsEnabled() {
		return
	}
	d.opts.Logger.Infof(
		"write stall wake | id=%d wait=%s total=%s wakeups=%d reason=%s | %s | %s",
		stallID,
		waitDuration,
		totalDuration,
		wakeups,
		reason,
		d.writeStallStateLocked(reason),
		d.formatReadDiagnosticsForEvent(),
	)
}

func (d *DB) logWriteStallEndLocked(
	stallID uint64, reason string, totalDuration time.Duration, wakeups int,
) {
	if !writeStallDiagnosticsEnabled() {
		return
	}
	d.opts.Logger.Infof(
		"write stall end | id=%d total=%s wakeups=%d | %s | %s",
		stallID,
		totalDuration,
		wakeups,
		d.writeStallStateLocked(reason),
		d.formatReadDiagnosticsForEvent(),
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
		"write stall flush | phase=%s job=%d inputs=%d input-bytes=%s ingest=%t err=%s | %s",
		phase,
		jobID,
		inputs,
		bytesForWriteStallDiagnostics(inputBytes),
		ingest,
		errText,
		d.writeStallStateLocked(""),
	)
}

func formatSlowWriteDBWorkBreakdown(stats BatchCommitStats, dbWorkDuration time.Duration) string {
	breakdown := stats.DBWorkBreakdown
	makeRoomResidual := breakdown.MakeRoomForWriteDuration -
		breakdown.MutablePrepareDuration -
		breakdown.QueueScanDuration -
		breakdown.RotateMemtableDuration
	if makeRoomResidual < 0 {
		makeRoomResidual = 0
	}
	rotateResidual := breakdown.RotateMemtableDuration -
		breakdown.FlushableBatchEnqueueDuration -
		breakdown.NewMemTableDuration -
		breakdown.ReadStateDuration -
		breakdown.MaybeScheduleFlushDuration
	if rotateResidual < 0 {
		rotateResidual = 0
	}
	dbResidual := dbWorkDuration -
		breakdown.MakeRoomForWriteDuration -
		breakdown.LogBytesAccountingDuration
	if dbResidual < 0 {
		dbResidual = 0
	}

	return fmt.Sprintf(
		"db-work-detail={total=%s make-room=%s prepare=%s queue-scan=%s make-room-residual=%s log-account=%s residual=%s} | rotate-mem={total=%s fbatch-enqueue=%s fbatch-reserve=%s new-mem=%s reused=%t alloc=%s reserve=%s init=%s read-state=%s lock-wait=%s install=%s old-unref=%s maybe-schedule-flush=%s residual=%s}",
		dbWorkDuration,
		breakdown.MakeRoomForWriteDuration,
		breakdown.MutablePrepareDuration,
		breakdown.QueueScanDuration,
		makeRoomResidual,
		breakdown.LogBytesAccountingDuration,
		dbResidual,
		breakdown.RotateMemtableDuration,
		breakdown.FlushableBatchEnqueueDuration,
		breakdown.FlushableBatchCacheReserveDuration,
		breakdown.NewMemTableDuration,
		breakdown.NewMemTableReused,
		breakdown.NewMemTableArenaAllocDuration,
		breakdown.NewMemTableCacheReserveDuration,
		breakdown.NewMemTableInitDuration,
		breakdown.ReadStateDuration,
		breakdown.ReadStateLockWaitDuration,
		breakdown.ReadStateInstallDuration,
		breakdown.ReadStateOldUnrefDuration,
		breakdown.MaybeScheduleFlushDuration,
		rotateResidual,
	)
}

func formatSlowWriteWALWriteBreakdown(stats BatchCommitStats) string {
	breakdown := stats.WALWriteBreakdown
	residual := stats.WALWriteDuration -
		breakdown.EmitFragmentDuration -
		breakdown.QueueBlockDuration
	if residual < 0 {
		residual = 0
	}

	return fmt.Sprintf(
		"wal-write-detail={total=%s emit=%s queue-block=%s residual=%s frags=%d queued-blocks=%d log=%s->%s}",
		stats.WALWriteDuration,
		breakdown.EmitFragmentDuration,
		breakdown.QueueBlockDuration,
		residual,
		breakdown.FragmentCount,
		breakdown.QueuedBlockCount,
		bytesForWriteStallDiagnostics(breakdown.LogSizeBefore),
		bytesForWriteStallDiagnostics(breakdown.LogSizeAfter),
	)
}

func formatSlowWriteWALRotationBreakdown(stats BatchCommitStats) string {
	breakdown := stats.WALRotationBreakdown
	closeResidual := breakdown.CloseDuration -
		breakdown.CloseEmitEOFTrailerDuration -
		breakdown.CloseDrainDuration -
		breakdown.CloseSyncDuration -
		breakdown.CloseFileDuration
	if closeResidual < 0 {
		closeResidual = 0
	}
	residual := stats.WALRotationDuration -
		breakdown.CloseDuration -
		breakdown.MetricsMergeDuration -
		breakdown.RecycleLookupDuration -
		breakdown.ReuseDuration -
		breakdown.CreateDuration -
		breakdown.StatDuration -
		breakdown.DirSyncDuration -
		breakdown.WrapDuration -
		breakdown.RecyclerPopDuration -
		breakdown.InstallDuration
	if residual < 0 {
		residual = 0
	}

	return fmt.Sprintf(
		"wal-rotation-detail={total=%s close=%s merge-metrics=%s recycle-lookup=%s recycled=%t reuse=%s create=%s stat=%s dir-sync=%s wrap=%s recycler-pop=%s install=%s prev=%s new=%s residual=%s} | wal-close={total=%s eof=%s drain=%s sync=%s file-close=%s residual=%s}",
		stats.WALRotationDuration,
		breakdown.CloseDuration,
		breakdown.MetricsMergeDuration,
		breakdown.RecycleLookupDuration,
		breakdown.Recycled,
		breakdown.ReuseDuration,
		breakdown.CreateDuration,
		breakdown.StatDuration,
		breakdown.DirSyncDuration,
		breakdown.WrapDuration,
		breakdown.RecyclerPopDuration,
		breakdown.InstallDuration,
		bytesForWriteStallDiagnostics(breakdown.PreviousLogSize),
		bytesForWriteStallDiagnostics(breakdown.NewLogSize),
		residual,
		breakdown.CloseDuration,
		breakdown.CloseEmitEOFTrailerDuration,
		breakdown.CloseDrainDuration,
		breakdown.CloseSyncDuration,
		breakdown.CloseFileDuration,
		closeResidual,
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

	dbWorkDuration := stats.DBMutexHoldDuration -
		stats.MemTableWriteStallDuration -
		stats.L0ReadAmpWriteStallDuration -
		stats.WALRotationDuration
	if dbWorkDuration < 0 {
		dbWorkDuration = 0
	}

	other := stats.TotalDuration -
		stats.SemaphoreWaitDuration -
		stats.CommitPipelineMutexWaitDuration -
		stats.DBMutexWaitDuration -
		stats.WALQueueWaitDuration -
		stats.WALWriteDuration -
		dbWorkDuration -
		stats.MemTableWriteStallDuration -
		stats.L0ReadAmpWriteStallDuration -
		stats.WALRotationDuration -
		stats.MemTableApplyDuration -
		stats.CommitWaitDuration
	if other < 0 {
		other = 0
	}

	blockCacheMetrics := d.opts.Cache.Metrics()
	tableCacheMetrics, filterMetrics := d.tableCache.metrics()

	d.mu.Lock()
	state := d.writeStallStateLocked("")
	memTableReserved := d.memTableReserved.Load()
	d.mu.Unlock()

	d.opts.Logger.Infof(
		"slow write | phase=%s total=%s threshold=%s sync=%t no-sync-wait=%t | batch={count=%d repr=%s memtable-est=%s flushable=%t} | stats={semaphore=%s commit-pipeline-lock=%s db-lock=%s db-work=%s wal-queue=%s wal-write=%s memtable-stall=%s l0-stall=%s wal-rotation=%s memtable-apply=%s commit-wait=%s other=%s} | %s | %s | %s | cache={block=%s/%s reserved=%s target=%s free-target=%s hit-rate=%.1f%% table-hit-rate=%.1f%% filter-utility=%.1f%% memtable-reserved=%s} | %s | %s",
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
		stats.CommitPipelineMutexWaitDuration,
		stats.DBMutexWaitDuration,
		dbWorkDuration,
		stats.WALQueueWaitDuration,
		stats.WALWriteDuration,
		stats.MemTableWriteStallDuration,
		stats.L0ReadAmpWriteStallDuration,
		stats.WALRotationDuration,
		stats.MemTableApplyDuration,
		stats.CommitWaitDuration,
		other,
		formatSlowWriteDBWorkBreakdown(stats, dbWorkDuration),
		formatSlowWriteWALWriteBreakdown(stats),
		formatSlowWriteWALRotationBreakdown(stats),
		signedBytesForWriteStallDiagnostics(blockCacheMetrics.Size),
		signedBytesForWriteStallDiagnostics(blockCacheMetrics.MaxSize),
		signedBytesForWriteStallDiagnostics(blockCacheMetrics.ReservedSize),
		signedBytesForWriteStallDiagnostics(blockCacheMetrics.TargetSize),
		signedBytesForWriteStallDiagnostics(blockCacheMetrics.TargetSize-blockCacheMetrics.Size),
		hitRate(blockCacheMetrics.Hits, blockCacheMetrics.Misses),
		hitRate(tableCacheMetrics.Hits, tableCacheMetrics.Misses),
		hitRate(filterMetrics.Hits, filterMetrics.Misses),
		signedBytesForWriteStallDiagnostics(memTableReserved),
		state,
		d.formatReadDiagnosticsForEvent(),
	)
}
