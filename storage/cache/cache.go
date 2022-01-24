package cache

import (
	"bytes"
	"io"
	"sync"

	"github.com/myml/kfs-ks/storage"
)

var _ storage.Storage = &Storage{}

type Storage struct {
	RawStorage storage.Storage
	store      sync.Map
}

func (b *Storage) Get(key string, offset int64, length int64) (io.ReadCloser, error) {
	var data []byte
	if v, ok := b.store.Load(key); ok {
		data = v.([]byte)
	} else {
		r, err := b.RawStorage.Get(key, 0, 0)
		if err != nil {
			return nil, err
		}
		if r == nil {
			return nil, nil
		}
		defer r.Close()
		data, err = io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		b.store.Store(key, data)
	}
	if offset > 0 {
		data = data[offset:]
	}
	if length > 0 {
		data = data[:length]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}
func (b *Storage) Set(key string, in io.Reader) error {
	data, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	b.store.Store(key, data)
	return nil
}
func (b *Storage) Flush() error {
	var err error
	b.store.Range(func(k, v interface{}) bool {
		key := k.(string)
		value := v.([]byte)
		err = b.RawStorage.Set(key, bytes.NewReader(value))
		b.store.Delete(key)
		return err == nil
	})
	return err
}
