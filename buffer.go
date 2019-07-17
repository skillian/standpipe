package standpipe

import (
	"errors"
	"io"
	"reflect"
	"unsafe"
)

const pointerSize = unsafe.Sizeof(unsafe.Pointer(nil))

var (
	errBufferEmpty = errors.New("buffer empty")
	errBufferFull  = errors.New("buffer full")
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
		// hack the capacity:
		((*reflect.SliceHeader)(unsafe.Pointer(&b.buf))).Cap = capacity
	} else {
		b.buf = make([]byte, 0, capacity)
	}
	return b
}

// Len gets the number of bytes yet to be read from the buffer:
func (b *buffer) Len() int { return len(b.unread()) }

func (b *buffer) Read(p []byte) (n int, err error) {
	logger.Debug3(" -> %v.%s(<%d bytes>)", Repr(b), "Read", len(p))
	defer func() { logger.Debug(" <- %v.%s(<%d bytes>) (%d, %v)", Repr(b), "Read", len(p), n, err) }()
	unread := b.buf[b.off:]
	n = minInt(len(p), len(unread))
	copy(p[:n], unread[:n])
	b.off += n
	if n < len(p) {
		err = errBufferEmpty
	}
	return
}

func (b *buffer) Write(p []byte) (n int, err error) {
	logger.Debug3(" -> %v.%s(<%d bytes>)", Repr(b), "Write", len(p))
	defer func() { logger.Debug(" <- %v.%s(<%d bytes>) (%d, %v)", Repr(b), "Write", len(p), n, err) }()
	logger.Debug2("    %v.unwritten: <%d bytes>", Repr(b), len(b.unwritten()))
	if len(b.unwritten()) < len(p) {
		logger.Debug4("    %v shifting {off: %d, len: %d, cap: %d}...", Repr(b), b.off, len(b.buf), cap(b.buf))
		b.shift()
		logger.Debug4("    %v shifted {off: %d, len: %d, cap: %d}...", Repr(b), b.off, len(b.buf), cap(b.buf))
	}
	target := b.unwritten()
	n = minInt(len(target), len(p))
	logger.Debug2("    %v n: %d", Repr(b), n)
	b.buf = b.buf[:len(b.buf)+n]
	copy(b.buf[len(b.buf)-n:], p[:n])
	if n < len(p) {
		err = errBufferFull
	}
	return
}

// ReadFrom implements the io.ReaderFrom interface by reading right into the
// target buffer.
func (b *buffer) ReadFrom(r io.Reader) (n int64, err error) {
	logger.Debug3(" -> %v.%s(%v)", Repr(b), "ReadFrom", Repr(r))
	defer func() { logger.Debug(" <- %v.%s(%v) (%d, %v)", Repr(b), "ReadFrom", Repr(r), n, err) }()
	m, err := r.Read(b.unwritten())
	b.buf = b.buf[:len(b.buf)+m]
	n = int64(m)
	if len(b.unwritten()) == 0 {
		err = errBufferFull
	}
	return
}

// WriteTo implements the io.WriterTo interface by writing right into the writer
// from the buffer's underlying byte slice.
func (b *buffer) WriteTo(w io.Writer) (n int64, err error) {
	logger.Debug3(" -> %v.%s(%v)", Repr(b), "WriteTo", Repr(w))
	defer func() { logger.Debug(" <- %v.%s(%v) (%d, %v)", Repr(b), "WriteTo", Repr(w), n, err) }()
	m, err := w.Write(b.unread())
	b.off += m
	n = int64(m)
	if len(b.unread()) == 0 {
		err = errBufferEmpty
	}
	return
}

// Reset the buffer so it's ready for writing again.
func (b *buffer) Reset() {
	b.buf = b.buf[:0]
	b.off = 0
}

// shift unread contents to the beginning of the buffer so there's more room
// to write to the end.
func (b *buffer) shift() {
	if b.off > 0 {
		copy(b.buf[0:], b.buf[b.off:])
		b.buf = b.buf[:len(b.buf)-b.off]
		b.off = 0
	}
}

// unread gets the slice of unread bytes from the buffer:
func (b *buffer) unread() []byte {
	return b.buf[b.off:]
}

func (b *buffer) unwritten() []byte {
	return b.buf[len(b.buf):cap(b.buf)]
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
