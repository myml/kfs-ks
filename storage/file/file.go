package file

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/myml/kfs-ks/storage"
)

var _ storage.Storage = &Storage{}

type Storage struct {
}

func (storage *Storage) Set(key string, in io.Reader) error {
	f, err := os.Create(key)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, in)
	return err
}

func (storage *Storage) Get(key string, offset int64, limit int64) (io.ReadCloser, error) {
	f, err := os.Open(key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("open file: %w", err)
	}
	if offset > 0 {
		_, err = f.Seek(offset, 0)
		if err != nil {
			return nil, fmt.Errorf("seek file: %w", err)
		}
	}
	if limit > 0 {
		o := struct {
			io.Reader
			io.Closer
		}{
			Reader: io.LimitReader(f, limit),
			Closer: f,
		}
		return o, nil
	}
	return f, nil
}
