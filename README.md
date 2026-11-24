# go-storage

File storage for Go with local disk and S3-compatible backends. The application
configures the S3 client and controls access to buckets and object keys.

```sh
go get github.com/ahaodev/go-storage
```

```go
import "github.com/ahaodev/go-storage"

disk, err := storage.NewDisk("./files")
s3Store := storage.NewS3(client, false)
```

Both constructors return a `storage.Store`. Pass `-1` as
`UploadRequest.Size` when the stream length is unknown. Disk lists directory
contents recursively. S3 lists objects using a prefix and delimiter.

Run `go test -race ./...` for disk and simulated S3 tests. Set
`SHADMIN_TEST_S3_ENDPOINT` to test against a real S3-compatible server.
