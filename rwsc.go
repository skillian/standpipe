package standpipe

import "io"

// ReadWriteSeekCloser bundles the method sets of io.ReadWriteSeeker and
// io.Closer.
type ReadWriteSeekCloser interface {
	io.ReadWriteSeeker
	io.Closer
}

// ReadWriteSeekCloserer gets a ReadWriteSeekCloser
type ReadWriteSeekCloserer interface {
	ReadWriteSeekCloser() (ReadWriteSeekCloser, error)
}

// ReadWriteSeekClosererFunc implements ReadWriteSeekCloserer through a
// function value.
type ReadWriteSeekClosererFunc func() (ReadWriteSeekCloser, error)

// ReadWriteSeekCloser implements the ReadWriteSeekCloserer interface.
func (f ReadWriteSeekClosererFunc) ReadWriteSeekCloser() (ReadWriteSeekCloser, error) {
	return f()
}
