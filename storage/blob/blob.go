package blob

import (
	"context"
	"fmt"
	"io"

	"github.com/myml/ks/storage"
	"gocloud.dev/blob"

	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"
)

var _ storage.Storage = &Blob{}

type Blob struct {
	gocloud *blob.Bucket
}

func NewBlob(url string) (*Blob, error) {
	bucket, err := blob.OpenBucket(context.Background(), url)
	if err != nil {
		return nil, fmt.Errorf("open bucket: %w", err)
	}
	return &Blob{gocloud: bucket}, nil
}

func (b *Blob) Get(key string, offset int64, length int64) (io.ReadCloser, error) {
	return b.gocloud.NewRangeReader(context.Background(), key, offset, length, nil)
}
func (b *Blob) Set(key string, in io.Reader) error {
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
