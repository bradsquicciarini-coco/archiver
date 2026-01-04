package tarreader

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
)

const headerSize = 512

type RangeGetter interface {
	Get(ctx context.Context, offset, length int64) ([]byte, error)
}

type Entry struct {
	Name         string
	Size         int64
	TypeFlag     byte
	HeaderOffset int64
}

func Walk(ctx context.Context, g RangeGetter, fn func(Entry, io.Reader) error) error {
	if g == nil {
		return errors.New("nil RangeGetter")
	}
	if fn == nil {
		return errors.New("nil walk func")
	}
	var offset int64
	for {
		hdr, err := g.Get(ctx, offset, headerSize)
		if err != nil {
			return err
		}
		if isZeroBlock(hdr) {
			return nil
		}
		entry, err := parseHeader(hdr, offset)
		if err != nil {
			return err
		}
		r := &rangeReader{
			ctx:    ctx,
			g:      g,
			off:    offset + headerSize,
			remain: entry.Size,
			chunk:  64 * 1024,
		}
		if err := fn(entry, r); err != nil {
			return err
		}
		offset = offset + headerSize + roundUp(entry.Size, headerSize)
	}
}

type rangeReader struct {
	ctx    context.Context
	g      RangeGetter
	off    int64
	remain int64
	chunk  int64
}

func (r *rangeReader) Read(p []byte) (int, error) {
	if r.remain <= 0 {
		return 0, io.EOF
	}
	want := int64(len(p))
	if want > r.remain {
		want = r.remain
	}
	if r.chunk > 0 && want > r.chunk {
		want = r.chunk
	}
	b, err := r.g.Get(r.ctx, r.off, want)
	if err != nil {
		return 0, err
	}
	n := int64(len(b))
	if n == 0 {
		return 0, io.EOF
	}
	copy(p, b)
	r.off += n
	r.remain -= n
	return int(n), nil
}

func parseHeader(hdr []byte, offset int64) (Entry, error) {
	if len(hdr) != headerSize {
		return Entry{}, errors.New("short tar header")
	}
	name := cString(hdr[0:100])
	prefix := cString(hdr[345:500])
	if prefix != "" {
		name = prefix + "/" + name
	}
	size, err := parseOctal(hdr[124:136])
	if err != nil {
		return Entry{}, err
	}
	return Entry{
		Name:         name,
		Size:         size,
		TypeFlag:     hdr[156],
		HeaderOffset: offset,
	}, nil
}

func parseOctal(b []byte) (int64, error) {
	s := strings.TrimRight(string(b), "\x00 ")
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 8, 64)
}

func cString(b []byte) string {
	s := string(b)
	if i := strings.IndexByte(s, 0); i >= 0 {
		s = s[:i]
	}
	return s
}

func isZeroBlock(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func roundUp(n, block int64) int64 {
	if n%block == 0 {
		return n
	}
	return n + block - (n % block)
}
