package mem

import (
	"bytes"
	"io"
	"sync"

	"github.com/myml/ks/storage"
)

var _ storage.Storage = &Storage{}

type Storage struct {
	store sync.Map
}

func (storage *Storage) Set(key string, in io.Reader) error {
	data, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	storage.store.Store(key, data)
	return nil
}

func (storage *Storage) Get(key string, offset int64, length int64) (io.ReadCloser, error) {
	if v, ok := storage.store.Load(key); ok {
		data := v.([]byte)
		if offset > 0 {
			data = data[offset:]
		}
		if length > 0 {
			data = data[:length]
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	return nil, nil
}
