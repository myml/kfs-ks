package blob

import (
	"context"
	"fmt"
	"io"

	"github.com/myml/ks/storage"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"
)

var _ storage.Storage = &Storage{}

type Storage struct {
	gocloud *blob.Bucket
}

func NewStorage(url string) (*Storage, error) {
	bucket, err := blob.OpenBucket(context.Background(), url)
	if err != nil {
		return nil, fmt.Errorf("open bucket: %w", err)
	}
	return &Storage{gocloud: bucket}, nil
}

func (b *Storage) Get(key string, offset int64, length int64) (io.ReadCloser, error) {
	if length == 0 {
		length = -1
	}
	r, err := b.gocloud.NewRangeReader(context.Background(), key, offset, length, nil)
	if gcerrors.Code(err) == gcerrors.NotFound {
		return nil, nil
	}
	return r, nil
}
func (b *Storage) Set(key string, in io.Reader) error {
	w, err := b.gocloud.NewWriter(context.Background(), key, nil)
	if err != nil {
		return fmt.Errorf("new write: %w", err)
	}
	_, err = io.Copy(w, in)
	if err != nil {
		return fmt.Errorf("write data: %w", err)
	}
	err = w.Close()
	if err != nil {
		return fmt.Errorf("write close: %w", err)
	}
	return nil
}
