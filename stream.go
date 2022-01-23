package ks

import (
	"bytes"
	"fmt"
	"io"
	"strconv"

	"github.com/myml/ks/storage"
	"github.com/myml/ks/storage/file"

	"go.uber.org/zap"
)

type WriteReaderAt interface {
	io.ReaderAt
	io.WriterAt
}

var _ WriteReaderAt = &Stream{}

func NewStream(opts ...func(stream *Stream)) *Stream {
	stream := &Stream{storage: &file.Storage{}, logger: zap.NewExample(), chunkSize: 1024 * 1024 * 4}
	for i := range opts {
		opts[i](stream)
	}
	return stream
}

type Stream struct {
	chunkSize int64
	storage   storage.Storage
	logger    *zap.Logger
	debug     bool
}

func (index *Stream) key(id int64) string {
	idStr := strconv.FormatInt(id, 10)
	return fmt.Sprintf("chunks/%c/%s", idStr[0], idStr)
}

func (index *Stream) getChunk(id int64, offset int64, limit int64) (io.ReadCloser, error) {
	r, err := index.storage.Get(index.key(id), offset, limit)
	if err != nil {
		return nil, err
	}
	// 没有chunk时，生成空的
	if r == nil {
		size := index.chunkSize
		if limit > 0 {
			size = limit
		} else if offset > 0 {
			size -= offset
		}
		return io.NopCloser(bytes.NewReader(make([]byte, size))), nil
	}
	return r, nil
}
func (index *Stream) setChunk(id int64, data []byte) error {
	err := index.storage.Set(index.key(id), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("storage set: %w", err)
	}
	return nil
}

func (index *Stream) ReadAt(p []byte, off int64) (n int, err error) {
	size := int64(len(p))
	offset := off
	var buff bytes.Buffer
	for {
		chunkID := offset / index.chunkSize
		chunkOffset := offset % index.chunkSize
		legnth := size - int64(buff.Len())
		if legnth+chunkOffset > index.chunkSize {
			legnth = 0
		}
		if index.debug {
			index.logger.Debug("readAt",
				zap.Int64("readat_offset", off),
				zap.Int64("current_offset", offset),
				zap.Int64("buff_size", int64(buff.Len())),
				zap.Int64("size", offset),
				zap.Int64("chunk_id", chunkID),
				zap.Int64("chunk_offset", chunkOffset),
				zap.Int64("length", legnth),
			)
		}
		r, err := index.getChunk(chunkID, chunkOffset, legnth)
		if err != nil {
			return 0, fmt.Errorf("get chunk: %w", err)
		}
		defer r.Close()
		n, err := io.Copy(&buff, r)
		if err != nil {
			return 0, fmt.Errorf("copy data: %w", err)
		}
		err = r.Close()
		if err != nil {
			return 0, fmt.Errorf("close chunk: %w", err)
		}
		offset += n
		if int64(buff.Len()) >= size {
			break
		}
	}
	return io.ReadFull(&buff, p)
}
func (index *Stream) WriteAt(p []byte, off int64) (n int, err error) {
	size := int64(len(p))
	offset := off
	for {
		chunkID := offset / index.chunkSize
		chunkOffset := offset % index.chunkSize
		chunkLimit := size + off - int64(offset)
		if chunkLimit+chunkOffset > index.chunkSize {
			chunkLimit = 0
		}
		r, err := index.getChunk(chunkID, 0, 0)
		if err != nil {
			return 0, fmt.Errorf("get chunk: %w", err)
		}
		defer r.Close()
		data, err := io.ReadAll(r)
		if err != nil {
			return 0, fmt.Errorf("read chunk: %w", err)
		}
		err = r.Close()
		if err != nil {
			return 0, fmt.Errorf("close chunk: %w", err)
		}
		n := copy(data[chunkOffset:], p[offset-off:])
		err = index.setChunk(chunkID, data)
		if err != nil {
			return 0, fmt.Errorf("set chunk: %w", err)
		}
		if index.debug {
			index.logger.Debug("writeAt",
				zap.Int64("chunk_id", chunkID),
				zap.Int64("chunk_offset", chunkOffset),
				zap.Int64("write_at_offset", off),
				zap.Int64("current_offset", offset),
				zap.Int64("size", size),
				zap.Int64("chunk_limit", chunkLimit),
				zap.Int64("write_number", int64(n)),
			)
		}
		if (off+size)/index.chunkSize <= chunkID {
			break
		}
		offset += int64(n)
	}
	return 0, nil
}
