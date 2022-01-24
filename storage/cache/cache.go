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
	rwlock     sync.RWMutex
	cacheKey   string
	cacheData  []byte
}

func (b *Storage) Get(key string, offset int64, length int64) (io.ReadCloser, error) {
	b.rwlock.RLock()
	ckey, cdata := b.cacheKey, b.cacheData
	b.rwlock.RUnlock()

	if cdata == nil || ckey != key {
		r, err := b.RawStorage.Get(key, 0, 0)
		if err != nil {
			return nil, err
		}
		if r == nil {
			return nil, nil
		}
		defer r.Close()
		cdata, err = io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		b.rwlock.RLock()
		b.cacheKey = key
		b.cacheData = cdata
		b.rwlock.RUnlock()
	}
	if offset > 0 {
		cdata = cdata[offset:]
	}
	if length > 0 {
		cdata = cdata[:length]
	}
	return io.NopCloser(bytes.NewReader(cdata)), nil
}
func (b *Storage) Set(key string, in io.Reader) error {
	data, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	b.rwlock.Lock()
	b.cacheKey = key
	b.cacheData = data
	b.rwlock.Unlock()
	return nil
}
func (b *Storage) Flush() error {
	err := b.RawStorage.Set(b.cacheKey, bytes.NewReader(b.cacheData))
	if err != nil {
		return err
	}
	b.rwlock.Lock()
	b.cacheKey = ""
	b.cacheData = nil
	b.rwlock.Unlock()
	return nil
}
