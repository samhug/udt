package truncatereader

import (
	"bufio"
	"bytes"
	"io"
)

// NewTruncReader returns a new Reader piping data from r that will strip the given searchPattern from
// the end of the stream
func NewTruncReader(r io.Reader, searchPattern []byte) io.Reader {
	return &truncReader{
		r:             bufio.NewReader(r),
		searchPattern: searchPattern,
	}
}

type truncReader struct {
	r             *bufio.Reader
	searchPattern []byte
}

// Read implements the io.Reader interface.
func (r *truncReader) Read(p []byte) (int, error) {

	for i := 0; i < len(p); i++ {
		buf, err := r.r.Peek(len(r.searchPattern) + 1)
		if len(buf) == 0 || err == bufio.ErrBufferFull {
			return i, err
		}

		if bytes.Equal(r.searchPattern, buf) {
			if err == io.EOF {
				r.r.Discard(len(r.searchPattern))
				return i, io.EOF
			}
			return r.r.Read(p[i:])
		}
		if p[i], err = r.r.ReadByte(); err != nil {
			return i, err
		}
	}

	return len(p), nil
}
