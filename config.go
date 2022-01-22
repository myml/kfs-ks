package ks

import (
	"github.com/myml/ks/storage"

	"go.uber.org/zap"
)

func WithLogger(logger *zap.Logger) func(stream *Stream) {
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
