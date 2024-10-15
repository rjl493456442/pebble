package readlist

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
)

type Reader interface {
	ReadAt(ctx context.Context, p []byte, off int64) error
}

type readTask struct {
	ctx    context.Context
	dest   []byte
	offset int64
	done   chan error
	reader Reader
	comp   bool
}

type ReadList struct {
	wg     sync.WaitGroup
	closed chan struct{}
	wake   chan struct{} // Wake channel if a new task is scheduled

	readTasks []*readTask
	compTasks []*readTask
	lock      sync.RWMutex

	readN            atomic.Int64
	readDuration     atomic.Int64
	readPureDuration atomic.Int64
	readSize         atomic.Int64

	compN            atomic.Int64
	compDuration     atomic.Int64
	compPureDuration atomic.Int64
	compSize         atomic.Int64
}

func NewReadList() *ReadList {
	x := &ReadList{
		closed: make(chan struct{}),
		wake:   make(chan struct{}, 1),
	}
	for i := 0; i < runtime.NumCPU(); i++ {
		x.wg.Add(1)
		go x.run()
	}
	x.wg.Add(1)
	go x.report()
	return x
}

func (l *ReadList) report() {
	defer l.wg.Done()

	timer := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-timer.C:
			var avgComp, avgPureComp time.Duration
			var avgCompSize int64
			if l.compN.Load() > 0 {
				avgComp = time.Duration(l.compDuration.Load() / l.compN.Load())
				avgPureComp = time.Duration(l.compPureDuration.Load() / l.compN.Load())
				avgCompSize = l.compSize.Load() / l.compN.Load()
			}
			var avgRead, avgPureRead time.Duration
			var avgReadSize int64
			if l.readN.Load() > 0 {
				avgRead = time.Duration(l.readDuration.Load() / l.readN.Load())
				avgPureRead = time.Duration(l.readPureDuration.Load() / l.readN.Load())
				avgReadSize = l.readSize.Load() / l.readN.Load()
			}
			fmt.Println(
				"comp tasks", l.compN.Load(), "compTime", time.Duration(l.compDuration.Load()), "compAvg", avgComp, "compPure", avgPureComp, "compSize", avgCompSize,
				"read tasks", l.readN.Load(), time.Duration(l.readDuration.Load()), "readAvg", avgRead, "readPure", avgPureRead, "readSize", avgReadSize,
			)
			l.readN.Store(0)
			l.readDuration.Store(0)
			l.readPureDuration.Store(0)
			l.readSize.Store(0)

			l.compN.Store(0)
			l.compDuration.Store(0)
			l.compPureDuration.Store(0)
			l.compSize.Store(0)

		case <-l.closed:
			return
		}
	}
}

func (l *ReadList) Close() {
	select {
	case <-l.closed:
		return
	}
	close(l.closed)
	l.wg.Wait()
}

func (l *ReadList) Read(ctx context.Context, dest []byte, offset int64, reader Reader) error {
	l.readN.Add(1)
	s := time.Now()
	err := l.read(ctx, dest, offset, reader, false)
	l.readDuration.Add(time.Since(s).Nanoseconds())
	return err
}

func (l *ReadList) CompRead(ctx context.Context, dest []byte, offset int64, reader Reader) error {
	l.compN.Add(1)
	s := time.Now()
	err := l.read(ctx, dest, offset, reader, true)
	l.compDuration.Add(time.Since(s).Nanoseconds())
	return err
}

func (l *ReadList) read(ctx context.Context, dest []byte, offset int64, reader Reader, comp bool) error {
	select {
	case <-l.closed:
		return errors.New("ReadList closed")
	default:
	}
	l.lock.Lock()

	done := make(chan error, 1)
	if !comp {
		l.readTasks = append(l.readTasks, &readTask{
			ctx:    ctx,
			dest:   dest,
			offset: offset,
			reader: reader,
			done:   done,
			comp:   false,
		})
	} else {
		l.compTasks = append(l.compTasks, &readTask{
			ctx:    ctx,
			dest:   dest,
			offset: offset,
			reader: reader,
			done:   done,
			comp:   true,
		})
	}
	l.lock.Unlock()
	// Notify the background thread to execute scheduled tasks
	select {
	case l.wake <- struct{}{}:
		// Wake signal sent
	default:
		// Wake signal not sent as a previous one is already queued
	}
	return <-done
}

func (l *ReadList) process(t *readTask) {
	s := time.Now()
	t.done <- t.reader.ReadAt(t.ctx, t.dest, t.offset)
	d := time.Since(s)
	if t.comp {
		l.compPureDuration.Add(d.Nanoseconds())
		l.compSize.Add(int64(len(t.dest)))
	} else {
		l.readPureDuration.Add(d.Nanoseconds())
		l.readSize.Add(int64(len(t.dest)))
	}
}

func (l *ReadList) run() {
	defer l.wg.Done()

	for {
		select {
		case <-l.closed:
			// Termination is requested, abort if no more tasks are pending. If
			// there are some, exhaust them first.
			l.lock.Lock()
			done := len(l.readTasks) + len(l.compTasks)
			l.lock.Unlock()

			if done == 0 {
				return
			}
		case <-l.wake:
			l.lock.Lock()
			var reads []*readTask
			if len(l.readTasks) > 100 {
				reads = l.readTasks[:100]
				l.readTasks = l.readTasks[100:]
			} else {
				reads = l.readTasks
				l.readTasks = nil
			}
			l.lock.Unlock()

			for _, read := range reads {
				l.process(read)
			}

			l.lock.Lock()
			var t *readTask
			if len(l.compTasks) > 0 {
				t = l.compTasks[0]
				l.compTasks = l.compTasks[1:]
			}
			l.lock.Unlock()
			if t != nil {
				l.process(t)
			}
		}
	}
}
