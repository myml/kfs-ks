package storage

import (
	"io"
)

type Storage interface {
	Get(key string, offset int64, length int64) (io.ReadCloser, error)
	Set(key string, in io.Reader) error
}
