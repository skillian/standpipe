package standpipe

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/skillian/errors"
)

// V1Pipe is the first implementation of a standpipe.
type V1Pipe struct {
	cond sync.Cond
	rbuf *buffer
	wbuf *buffer
	offs []int64 // offsets into the file.
	offi int     // offset into offs of the next read.
	rwsc ReadWriteSeekCloser
	free []int64 // available offsets into the file
	roff int64
	woff int64
	flag v1flag
	head V1Header
}

// NewV1Pipe creates a new stand pipe from the given rwsc.
func NewV1Pipe(rwsc ReadWriteSeekCloser, pageSize int) *V1Pipe {
	return &V1Pipe{
		cond: *sync.NewCond(new(Mutex)),
		roff: int64(pageSize),
		rbuf: newBuffer(pageSize),
		woff: 0,
		wbuf: newBuffer(pageSize),
		rwsc: rwsc,
		head: V1Header{
			HeaderCommon: HeaderCommon{
				Magic: Magic,
				Version: Version{
					Major:   0,
					Minor:   1,
					Release: 0,
					Build:   0,
				},
			},
			PageSize:         int64(pageSize),
			TableOffset:      0,
			TableLength:      0,
			ReadBufferOffset: 0,
			ReadBufferIndex:  0,
		},
	}
}

// Close persists the state of the pipe so it can be resumed later.
func (pp *V1Pipe) Close() error {
	return errors.Errorf("not implemented")
}

func (pp *V1Pipe) Read(p []byte) (n int, err error) {
	for t := p; len(t) > 0; t = p[n:] {
		var m int
		m, err = pp.rbuf.Read(t)
		n += m
		if err != nil {
			if err != errShortRead {
				err = errors.ErrorfWithCause(
					err, "error while reading from buffer")
				return
			}
			if err = pp.nextRBuf(); err != nil {
				return
			}
		}
	}
	return
}

func (pp *V1Pipe) Write(p []byte) (n int, err error) {
	for s := p; len(s) > 0; s = p[n:] {
		var m int
		m, err = pp.wbuf.Write(s)
		n += m
		if err != nil {
			if err != errShortWrite {
				err = errors.ErrorfWithCause(
					err, "failed to write into write buffer")
				return
			}
			if err = pp.nextWBuf(); err != nil {
				err = errors.ErrorfWithCause(
					err, "failed to initialize next write "+
						"buffer")
				return
			}
		}
	}
	return
}

func (pp *V1Pipe) nextRBuf() error {
	if pp.isQuit() {
		return errShortRead
	}
	pp.cond.L.Lock()
	if !pp.waitRead() {
		return errShortRead
	}
	offset := pp.offs[pp.offi]
	pp.offi++
	_, err := pp.rwsc.Seek(offset, io.SeekStart)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to seek to offset %d in rwsc", offset)
	}
	_, err = pp.rbuf.ReadFrom(pp.rwsc)
	if err != nil && err != errShortRead {
		return errors.ErrorfWithCause(
			err, "failed to read from rwsc into read buffer")
	}
	pp.cond.L.Unlock()
	return nil
}

func (pp *V1Pipe) waitRead() bool {
	for {
		if pp.isQuit() {
			return false
		}
		if len(pp.offs) > pp.offi {
			return true
		}
		pp.cond.Wait()
	}
}

func (pp *V1Pipe) nextWBuf() (err error) {
	if pp.isQuit() {
		return errShortWrite
	}
	pp.cond.L.Lock()
	if err = pp.flushWBuf(); err != nil {
		return errors.ErrorfWithCause(
			err, "error while flushing write buffer")
	}
	// after flushing the buffer, we have to reacquire the lock so we can
	// get the next write location from rwsc.
	var offset int64
	var whence int
	length := len(pp.free)
	if length > 0 {
		offset = pp.free[length-1]
		pp.free = pp.free[:length-1]
		whence = io.SeekStart
	} else {
		offset = 0
		whence = io.SeekEnd
	}
	offset, err = pp.rwsc.Seek(offset, whence)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to seek to end of %v for writing",
			pp.rwsc)
	}
	pp.woff = offset
	// wbuf should be reset by flushing the buffer.
	pp.cond.Signal()
	pp.cond.L.Unlock()
	return nil
}

func (pp *V1Pipe) flushWBuf() (err error) {
	_, err = pp.rwsc.Seek(pp.woff, io.SeekStart)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to seek to write buffer offset %d",
			pp.woff)
	}
	_, err = pp.wbuf.WriteTo(pp.rwsc)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "error while writing from write buffer into %v",
			pp.rwsc)
	}
	pp.offs = append(pp.offs, pp.woff)
	pp.wbuf.Reset()
	return nil
}

func (pp *V1Pipe) isQuit() bool {
	return pp.flag.value()&v1quit == v1quit
}

func (pp *V1Pipe) doQuit() {
	pp.flag.setFlag(v1quit)
}

type v1flag uint32

const (
	v1done v1flag = 1 << iota
	v1quit
)

// value atomically loads the flag value
func (f *v1flag) value() v1flag {
	return v1flag(atomic.LoadUint32((*uint32)(f)))
}

func (f *v1flag) setFlag(v v1flag) {
	for i := 0; i < 1000; i++ {
		fv := f.value()
		if atomic.CompareAndSwapUint32(
			(*uint32)(f), uint32(fv), uint32(fv|v)) {
			return
		}
	}
	panic(errors.Errorf("failed to set flag %x at %p", v, f))
}
