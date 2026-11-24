package storage

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDiskObjectOperations(t *testing.T) {
	store, err := NewDisk(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	req := UploadRequest{
		Bucket: "bucket", Prefix: "nested", ObjectName: "file.txt",
		Reader: strings.NewReader("hello"), Size: 5,
	}
	uploaded, err := store.Upload(ctx, &req)
	if err != nil {
		t.Fatal(err)
	}
	const key = "nested/file.txt"
	if uploaded.Key != key || uploaded.Size != 5 {
		t.Fatalf("unexpected upload result: %+v", uploaded)
	}
	info, err := store.GetObjectStat(ctx, req.Bucket, key)
	if err != nil || info.Size != 5 || info.ETag != uploaded.ETag {
		t.Fatalf("stat = %+v, %v", info, err)
	}
	req.Reader = strings.NewReader("short")
	req.Size = 6
	if _, err := store.Upload(ctx, &req); err == nil {
		t.Fatal("size mismatch accepted")
	}
	reader, err := store.Download(ctx, &DownloadRequest{Bucket: req.Bucket, ObjectName: key})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(body) != "hello" {
		t.Fatalf("download = %q, %v", body, err)
	}
	list, err := store.List(ctx, req.Bucket, "nested/")
	if err != nil || len(list) != 1 || list[0].Key != key {
		t.Fatalf("list = %+v, %v", list, err)
	}
	if err := store.Delete(ctx, req.Bucket, key); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetObjectStat(ctx, req.Bucket, key); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stat after delete = %v", err)
	}
}

func TestDiskBucketConfinement(t *testing.T) {
	dir := t.TempDir()
	store, err := NewDisk(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	bucket := filepath.Join(dir, "bucket")
	if err := os.Mkdir(bucket, 0755); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(bucket, "link")); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"../other/file", "link/file"} {
		_, err := store.Upload(ctx, &UploadRequest{
			Bucket: "bucket", ObjectName: key, Reader: strings.NewReader("test"), Size: 4,
		})
		if err == nil {
			t.Errorf("upload accepted out-of-bucket key %q", key)
		}
	}
	if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
		t.Fatalf("outside directory = %v, %v", entries, err)
	}
}
