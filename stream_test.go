package ks

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"io"
	mrand "math/rand"
	"testing"

	"github.com/myml/kfs-ks/storage/mem"
	"github.com/stretchr/testify/require"
)

func TestSteram(t *testing.T) {
	assert := require.New(t)
	b := &mem.Storage{}
	stream := NewStream(WithStorage(b), WithDebug(true))

	getData := bytes.Repeat([]byte{1}, 1024)
	_, err := stream.ReadAt(getData[24:], 24)
	assert.NoError(err)
	assert.EqualValues(bytes.Repeat([]byte{1}, 24), getData[:24])
	assert.EqualValues(make([]byte, 1000), getData[24:])

	data := make([]byte, 1024)
	_, err = io.ReadFull(rand.Reader, data)
	assert.NoError(err)
	_, err = stream.WriteAt(data, 0)
	assert.NoError(err)
	getData = make([]byte, 1024)
	_, err = stream.ReadAt(getData, 0)
	assert.NoError(err)
	assert.EqualValues(data, getData)

	getData = make([]byte, 1014)
	_, err = stream.ReadAt(getData, 10)
	assert.NoError(err)
	assert.EqualValues(data[10:], getData)

	getData = make([]byte, 1004)
	_, err = stream.ReadAt(getData, 20)
	assert.NoError(err)
	assert.EqualValues(data[20:], getData)
}

func BenchmarkSteram(b *testing.B) {
	s := &mem.Storage{}
	stream := NewStream(WithStorage(s), WithChunkSize(100))
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
