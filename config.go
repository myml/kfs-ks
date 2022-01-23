package ks

import (
	"log"

	"github.com/myml/kfs-ks/storage"
)

func WithLogger(logger *log.Logger) func(stream *Stream) {
	return func(stream *Stream) {
		stream.logger = logger
	}
}
func WithChunkSize(chunkSize int64) func(stream *Stream) {
	return func(stream *Stream) {
		stream.chunkSize = chunkSize
	}
}
func WithStorage(storage storage.Storage) func(stream *Stream) {
	return func(stream *Stream) {
		stream.storage = storage
	}
}
func WithDebug(debug bool) func(stream *Stream) {
	return func(stream *Stream) {
		stream.debug = debug
	}
}
