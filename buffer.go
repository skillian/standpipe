package standpipe

import (
	"io"
	"unsafe"
)

const pointerSize = unsafe.Sizeof(unsafe.Pointer(nil))

var (
	errShortRead  = io.ErrShortBuffer
	errShortWrite = io.ErrShortWrite
)

type buffer struct {
	buf []byte
	off int
	cch [cacheLineSize - (4 * pointerSize)]byte
}

func newBuffer(capacity int) *buffer {
	b := new(buffer)
	if capacity < len(b.cch) {
		b.buf = b.cch[:0]
	} else {
		b.buf = make([]byte, 0, capacity)
	}
	return b
}

func (b *buffer) Read(p []byte) (n int, err error) {
	unread := b.buf[b.off:]
	n = minInt(len(p), len(unread))
	copy(p[:n], unread[:n])
	b.off += n
	if n < len(p) {
		err = errShortRead
	}
	return
}

func (b *buffer) Write(p []byte) (n int, err error) {
	if b.availableForWrite() < len(p) && b.off > 0 {
		copy(b.buf[0:], b.buf[b.off:])
		b.buf = b.buf[:len(b.buf)-b.off]
		b.off = 0
	}
	n = minInt(b.availableForWrite(), len(p))
	b.buf = b.buf[:len(b.buf)+n]
	copy(b.buf[len(b.buf)-n:], p[:n])
	if n < len(p) {
		err = errShortWrite
	}
	return
}

// ReadFrom implements the io.ReaderFrom interface by reading right into the
// target buffer.
func (b *buffer) ReadFrom(r io.Reader) (int64, error) {
	target := b.buf[len(b.buf):cap(b.buf)]
	n, err := r.Read(target)
	b.buf = b.buf[:len(b.buf)+n]
	return int64(n), err
}

// WriteTo implements the io.WriterTo interface by writing right into the writer
// from the buffer's underlying byte slice.
func (b *buffer) WriteTo(w io.Writer) (int64, error) {
	source := b.buf[b.off:]
	n, err := w.Write(source)
	b.buf = b.buf[b.off+n:]
	return int64(n), err
}

// Reset the buffer so it's ready for writing again.
func (b *buffer) Reset() {
	b.buf = b.buf[:0]
	b.off = 0
}

func (b *buffer) availableForWrite() int {
	return cap(b.buf) - len(b.buf)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
