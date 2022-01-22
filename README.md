# KeyValue to Stream

转化 KeyValue 接口为一个 Stream 接口，流支持 io.ReaderAt 和 io.WriterAt

## KeyValue 接口

```go
type Storage interface {
	Get(key string, offset int64, limit int64) (io.ReadCloser, error)
	Set(key string, in io.Reader) error
}
```

## Stream 接口

```go
type WriteReaderAt interface {
	io.ReaderAt
	io.WriterAt
}
```

## 例子

```go
import (
	"github.com/myml/ks"
	"github.com/myml/ks/storage/file"
)

stream := ks.NewStream(ks.WithChunkSize(1024*1024*4), ks.WithStorage(file.Storage{}))
...
stream.ReadAt(data, 0)
...
stream.WriteAt(data, 0)
```

## 场景

- 超大文件自动拆分
- 键值存储虚拟块设备

## 路线图

- [ ] 数据加密
- [ ] 数据压缩
- [ ] AWS S3 存储 驱动
- [ ] 存储键分层
