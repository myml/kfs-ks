package gz

import (
	"compress/gzip"
	"io"

	"github.com/myml/kfs-ks/storage"
)

var _ storage.Storage = &Storage{}

type Storage struct {
	RawStorage storage.Storage
}

func (storage *Storage) Set(key string, in io.Reader) error {
	r, w := io.Pipe()
	gw := gzip.NewWriter(w)
	go func() {
		_, err := io.Copy(gw, in)
		if err != nil {
			w.CloseWithError(err)
		}
		err = gw.Close()
		if err != nil {
			w.CloseWithError(err)
		}
		w.Close()
	}()
	return storage.RawStorage.Set(key, r)
}

func (storage *Storage) Get(key string, offset int64, length int64) (io.ReadCloser, error) {
	r, err := storage.RawStorage.Get(key, 0, 0)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, nil
	}
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	var o struct {
		io.Reader
		io.Closer
	}
	o.Reader = gr
	o.Closer = r

	if offset > 0 {
		_, err = io.CopyN(io.Discard, o, offset)
		if err != nil {
			return nil, err
		}
	}
	if length > 0 {
		o.Reader = io.LimitReader(o.Reader, length)
	}
	return o, nil
}
