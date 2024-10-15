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
}

type ReadList struct {
	wg     sync.WaitGroup
	closed chan struct{}
	wake   chan struct{} // Wake channel if a new task is scheduled

	readTasks []*readTask
	compTasks []*readTask
	lock      sync.RWMutex

	readN atomic.Int64
	compN atomic.Int64
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
			fmt.Println("comp tasks", l.compN.Load(), "read tasks", l.readN.Load())
			l.compN.Store(0)
			l.readN.Store(0)
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
	return l.read(ctx, dest, offset, reader, false)
}

func (l *ReadList) CompRead(ctx context.Context, dest []byte, offset int64, reader Reader) error {
	l.compN.Add(1)
	return l.read(ctx, dest, offset, reader, true)
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
		})
	} else {
		l.compTasks = append(l.compTasks, &readTask{
			ctx:    ctx,
			dest:   dest,
			offset: offset,
			reader: reader,
			done:   done,
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
	t.done <- t.reader.ReadAt(t.ctx, t.dest, t.offset)
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
			reads := l.readTasks
			l.readTasks = nil
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
