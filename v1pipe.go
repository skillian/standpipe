package standpipe

import (
	"io"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/skillian/errors"
)

// V1Pipe is the first implementation of a standpipe.  The version is included
// in the datatype because the same standpipe executable needs to be able to
// handle multiple standpipe versions.
type V1Pipe struct {
	// cond is the condition variable signalled when there is data
	// available in the standpipe to be read or if no more data will be
	// written.
	cond sync.Cond

	// flag holds flags to indicate the pipe's state.
	flag v1flag

	// rbuf is the in-memory buffer of data currently being read from
	// the standpipe.
	rbuf *buffer

	// wbuf is the in-memory buffer of the data to be written to the
	// standpipe.
	wbuf *buffer

	// offs is an ordered slice of offsets into the standpipe file with
	// data yet to be read out.
	offs []int64

	// offi is the offset index
	offi int

	rwsc ReadWriteSeekCloser
	free []int64 // available offsets into the file
	head V1Header
}

// V1PipeReader reads from the V1Pipe
type V1PipeReader V1Pipe

func (r *V1PipeReader) pp() *V1Pipe                { return (*V1Pipe)(r) }
func (r *V1PipeReader) Read(p []byte) (int, error) { return r.pp().Read(p) }

// Close the reader.  The writer will proceed with its writing into the pipe.
func (r *V1PipeReader) Close() error {
	pp := r.pp()
	pp.flag.setFlags(v1DumpDone)
	return pp.Close()
}

// V1PipeWriter writes to the V1Pipe
type V1PipeWriter V1Pipe

func (w *V1PipeWriter) pp() *V1Pipe                 { return (*V1Pipe)(w) }
func (w *V1PipeWriter) Write(p []byte) (int, error) { return w.pp().Write(p) }

// Close the writer.  The reader will continue to read from the pipe.
func (w *V1PipeWriter) Close() error {
	pp := w.pp()
	pp.flag.setFlags(v1LoadDone)

	// make sure the write buffer is flushed:
	pp.cond.L.Lock()
	length := pp.wbuf.Len()
	pp.cond.L.Unlock()
	if length > 0 {
		pp.head.LastBufferLength = int64(length)
		if err := pp.nextWBuf(); err != nil {
			return err
		}
	}
	return pp.Close()
}

// NewV1Pipe creates a new stand pipe from the given rwsc.
func NewV1Pipe(rwsc ReadWriteSeekCloser, pageSize int) (*V1PipeReader, *V1PipeWriter, error) {
	pp := &V1Pipe{
		cond: *sync.NewCond(new(Mutex)),
		rbuf: newBuffer(pageSize),
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
	if err := pp.seekAndWriteHeader(); err != nil {
		return nil, nil, err
	}
	return (*V1PipeReader)(pp), (*V1PipeWriter)(pp), nil
}

// Close implements io.Closer.
func (pp *V1Pipe) Close() (err error) {
	if !pp.flag.hasAllFlags(v1LoadDone | v1DumpDone) {
		logger.Debug1("    %v.Close():  one pipe half closed.", Repr(pp))
		return nil
	}
	logger.Debug1(" -> %v.Close()", Repr(pp))
	defer func() { logger.Debug2(" <- %v.Close() %v", Repr(pp), err) }()
	pp.cond.L.Lock()
	defer pp.cond.L.Unlock()
	toff, err := pp.rwsc.Seek(0, io.SeekEnd)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to seek to end of file to write table")
	}
	if err = pp.destructivelyWriteInt64s(pp.offs); err != nil {
		return errors.ErrorfWithCause(
			err, "failed to write offsets")
	}
	pp.head.TableOffset = toff
	pp.head.TableLength = int64(8 * len(pp.offs))
	if err = pp.destructivelyWriteInt64s(pp.free); err != nil {
		return errors.ErrorfWithCause(
			err, "failed to write offsets")
	}
	return pp.seekAndWriteHeader()
}

func (pp *V1Pipe) Read(p []byte) (n int, err error) {
	logger.Debug3(" -> %v.%s(<%d bytes>)", Repr(pp), "Read", len(p))
	defer func() { logger.Debug(" <- %v.%s(<%d bytes>) (%d, %v)", Repr(pp), "Read", len(p), n, err) }()
	for t := p; len(t) > 0; t = p[n:] {
		var m int
		m, err = pp.rbuf.Read(t)
		n += m
		if err != nil {
			if err != errBufferEmpty {
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
	logger.Debug3(" -> %v.%s(<%d bytes>)", Repr(pp), "Write", len(p))
	defer func() { logger.Debug(" <- %v.%s(<%d bytes>) (%d, %v)", Repr(pp), "Write", len(p), n, err) }()
	for s := p; len(s) > 0; s = p[n:] {
		var m int
		m, err = pp.wbuf.Write(s)
		n += m
		if err != nil {
			if err != errBufferFull {
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

func (pp *V1Pipe) seekAndWriteHeader() error {
	p, err := Marshal(pp.head)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to marshal V1 pipe header")
	}
	_, err = pp.rwsc.Seek(0, io.SeekStart)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to seek to beginning of file to write "+
				"header")
	}
	n, err := pp.rwsc.Write(p)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to write initial header")
	}
	if n != len(p) {
		return errors.Errorf(
			"Write didn't write the right number of bytes but " +
				"reported no error.")
	}
	return nil
}

func (pp *V1Pipe) destructivelyWriteInt64s(vs []int64) error {
	b := *((*[]byte)(unsafe.Pointer(&vs)))
	b = b[:8*len(pp.offs)]
	for i, off := range pp.offs {
		i *= 8
		byteOrder.PutUint64(b[i:i+8], uint64(off))
	}
	n, err := pp.rwsc.Write(b)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to write offsets")
	}
	if n != len(b) {
		return errors.Errorf(
			"expected write to be %d bytes, not %d", len(b), n)
	}
	return nil
}

func (pp *V1Pipe) nextRBuf() error {
	pp.cond.L.Lock()
	if !pp.waitRead() {
		return io.EOF
	}
	offset := pp.offs[pp.offi]
	pp.offi++
	_, err := pp.rwsc.Seek(offset, io.SeekStart)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to seek to offset %d in rwsc", offset)
	}
	pp.rbuf.Reset()
	_, err = pp.rbuf.ReadFrom(pp.rwsc)
	if err != nil && err != errBufferFull {
		return errors.ErrorfWithCause(
			err, "failed to read from rwsc into read buffer")
	}
	pp.cond.L.Unlock()
	return nil
}

func (pp *V1Pipe) waitRead() bool {
	for {
		if len(pp.offs) > pp.offi {
			return true
		}
		if pp.flag.hasAllFlags(v1LoadDone) {
			return false
		}
		pp.cond.Wait()
	}
}

func (pp *V1Pipe) nextWBuf() (err error) {
	pp.cond.L.Lock()
	offset, whence := pp.nextWBufSeek()
	offset, err = pp.rwsc.Seek(offset, whence)
	if err != nil {
		return errors.ErrorfWithCause(
			err, "failed to seek to end of %v for writing",
			pp.rwsc)
	}
	_, err = pp.wbuf.WriteTo(pp.rwsc)
	if err != nil && err != errBufferEmpty {
		return errors.ErrorfWithCause(
			err, "error while writing from write buffer into %v",
			pp.rwsc)
	}
	if len(pp.offs) == cap(pp.offs) && pp.offi > 0 {
		copy(pp.offs[0:], pp.offs[pp.offi:])
		pp.offs = pp.offs[:len(pp.offs)-pp.offi]
		pp.offi = 0
	}
	pp.offs = append(pp.offs, offset)
	pp.wbuf.Reset()
	err = pp.seekAndWriteHeader()
	pp.cond.Signal()
	pp.cond.L.Unlock()
	return nil
}

// nextWBufSeek gets the Seek parameters for the next write buffer.  It tries
// to use the last buffer read from because hopefully that location is still
// in an OS and/or disk cache somewhere.  Otherwise, it just seeks to the end
// of the file.
func (pp *V1Pipe) nextWBufSeek() (offset int64, whence int) {
	length := len(pp.free)
	if length > 0 {
		offset = pp.free[length-1]
		pp.free = pp.free[:length-1]
		whence = io.SeekStart
		return
	}
	offset = 0
	whence = io.SeekEnd
	return
}

type v1flag uint32

const (
	// v1LoadDone is set when loading data into the pipe is finished but
	// the pipe still has to be emptied.
	v1LoadDone v1flag = 1 << iota

	// v1DumpDone is set when writing data out of the pipe is done.
	v1DumpDone
)

// value atomically loads the flag value
func (f *v1flag) value() v1flag {
	return v1flag(atomic.LoadUint32((*uint32)(f)))
}

// hasAllFlags checks if the flag has all of the flags given as its parameter
func (f *v1flag) hasAllFlags(v v1flag) bool {
	return f.value()&v == v
}

// hasAnyFlags returns true if any of v's bits are set in the flag.
func (f *v1flag) hasAnyFlags(v v1flag) bool {
	return f.value()&v != 0
}

func (f *v1flag) setFlags(v v1flag) {
	for i := 0; i < 1000; i++ {
		fv := f.value()
		if fv&v == v {
			return
		}
		if atomic.CompareAndSwapUint32(
			(*uint32)(f), uint32(fv), uint32(fv|v)) {
			return
		}
	}
	panic(errors.Errorf("failed to set flag %x at %p", v, f))
}
