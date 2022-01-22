package ks

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"io"
	mrand "math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSteram(t *testing.T) {
	assert := require.New(t)
	stream := NewStream(WithChunkSize(100))
	os.Mkdir("chunks", 0700)

	data := make([]byte, 1024)
	_, err := io.ReadFull(rand.Reader, data)
	assert.NoError(err)
	_, err = stream.WriteAt(data, 0)
	assert.NoError(err)
	getData := make([]byte, 1024)
	_, err = stream.ReadAt(getData, 0)
	assert.NoError(err)
	assert.EqualValues(data, getData)
}

func BenchmarkSteram(b *testing.B) {
	stream := NewStream(WithChunkSize(100))
	os.Mkdir("chunks", 0700)
	data := make([]byte, 999)
	getData := make([]byte, 999)
	io.ReadFull(rand.Reader, data)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		i := int64(mrand.Intn(999))
		stream.WriteAt(data, i)
		stream.ReadAt(getData, i)
		d1, d2 := md5.Sum(data), md5.Sum(getData)
		if !bytes.Equal(d1[:], d2[:]) {
			b.Log("data", data)
			b.Log("getData", getData)
			b.Fail()
		}
	}
}
