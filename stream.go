package ks

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/myml/ks/storage"
)

type WriteReaderAt interface {
	io.ReaderAt
	io.WriterAt
}

var _ WriteReaderAt = &Stream{}

func NewStream(withStorage func(stream *Stream), withs ...func(stream *Stream)) *Stream {
	stream := &Stream{logger: log.Default(), chunkSize: 1024 * 1024 * 4}
	withStorage(stream)
	for i := range withs {
		withs[i](stream)
	}
	return stream
}

type Stream struct {
	chunkSize int64
	storage   storage.Storage
	logger    *log.Logger
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
			index.logger.Println(
				"readAt",
				"readat_offset", off,
				"current_offset", offset,
				"buff_size", int64(buff.Len()),
				"size", offset,
				"chunk_id", chunkID,
				"chunk_offset", chunkOffset,
				"length", legnth,
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
			index.logger.Println(
				"writeAt",
				"chunk_id", chunkID,
				"chunk_offset", chunkOffset,
				"write_at_offset", off,
				"current_offset", offset,
				"size", size,
				"chunk_limit", chunkLimit,
				"write_number", int64(n),
			)
		}
		if (off+size)/index.chunkSize <= chunkID {
			break
		}
		offset += int64(n)
	}
	return 0, nil
}
