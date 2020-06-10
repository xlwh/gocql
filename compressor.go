package gocql

import (
	"sync"

	"github.com/golang/snappy"
)

type Compressor interface {
	Name() string
	Encode(data []byte) ([]byte, error)
	Decode(data []byte) ([]byte, error)
}

// SnappyCompressor implements the Compressor interface and can be used to
// compress incoming and outgoing frames. The snappy compression algorithm
// aims for very high speeds and reasonable compression.
type SnappyCompressor struct{ bufferSize int }

func (s SnappyCompressor) Name() string {
	return "snappy"
}

func (s SnappyCompressor) Encode(data []byte) ([]byte, error) {
	var buf = p.Get().([]byte)[:0]
	if (s.bufferSize != 0 && cap(buf) < s.bufferSize) || cap(buf) < len(data) {
		buf = make([]byte, 0, max(s.bufferSize, len(data)))
	}

	buf = append(buf, data...)
	var ret = snappy.Encode(data, buf)

	p.Put(buf)
	return ret, nil
}

func (s SnappyCompressor) Decode(data []byte) ([]byte, error) {
	var buf = p.Get().([]byte)[:0]
	if (s.bufferSize != 0 && cap(buf) < s.bufferSize) || cap(buf) < len(data) {
		buf = make([]byte, 0, max(s.bufferSize, len(data)))
	}

	buf = append(buf, data...)
	var ret, err = snappy.Decode(data, buf)

	p.Put(buf)
	return ret, err
}

var p = sync.Pool{New: func() interface{} { return make([]byte, 0, 1024) }}

var max = func(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
