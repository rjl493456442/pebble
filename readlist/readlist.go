package readlist

import (
	"context"
	"runtime"
	"sync"

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
	readOpts chan *readTask
	compOpts chan *readTask
	wg       sync.WaitGroup
	closed   chan struct{}
}

func NewReadList() *ReadList {
	x := &ReadList{
		readOpts: make(chan *readTask),
		compOpts: make(chan *readTask),
		closed:   make(chan struct{}),
	}
	for i := 0; i < runtime.NumCPU(); i++ {
		x.wg.Add(1)
		go x.run()
	}
	return x
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
	return l.read(ctx, l.readOpts, dest, offset, reader)
}

func (l *ReadList) CompRead(ctx context.Context, dest []byte, offset int64, reader Reader) error {
	return l.read(ctx, l.compOpts, dest, offset, reader)
}

func (l *ReadList) read(ctx context.Context, sink chan *readTask, dest []byte, offset int64, reader Reader) error {
	done := make(chan error, 1)
	select {
	case <-l.closed:
		return errors.New("ReadList closed")
	case sink <- &readTask{
		ctx:    ctx,
		dest:   dest,
		offset: offset,
		reader: reader,
		done:   done,
	}:
		return <-done
	}
}

func (l *ReadList) process(t *readTask) {
	t.done <- t.reader.ReadAt(t.ctx, t.dest, t.offset)
}

func (l *ReadList) run() {
	defer l.wg.Done()

	for {
		select {
		case t := <-l.readOpts:
			l.process(t)
		case <-l.closed:
			return
		default:
			select {
			case t := <-l.readOpts:
				l.process(t)
			case t := <-l.compOpts:
				l.process(t)
			case <-l.closed:
				return
			}
		}
	}
}
