package storage

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type diskStore struct {
	basePath string
}

// NewDisk stores objects beneath basePath, in one directory per bucket.
func NewDisk(basePath string) (Store, error) {
	basePath, err := filepath.Abs(basePath)
	if err != nil {
		return nil, fmt.Errorf("resolve storage path: %w", err)
	}
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("create storage path: %w", err)
	}
	return &diskStore{basePath: basePath}, nil
}

func (d *diskStore) openBucket(bucket string, create bool) (*os.Root, error) {
	if bucket == "." || strings.ContainsAny(bucket, "/\\") || !fs.ValidPath(bucket) {
		return nil, fmt.Errorf("invalid bucket name %q", bucket)
	}
	root, err := os.OpenRoot(d.basePath)
	if err != nil {
		return nil, err
	}
	defer root.Close()

	if create {
		if err := root.Mkdir(bucket, 0755); err != nil && !os.IsExist(err) {
			return nil, err
		}
	}
	info, err := root.Lstat(bucket)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("bucket %q must be a directory", bucket)
	}
	return root.OpenRoot(bucket)
}

func diskPath(key string) (string, error) {
	if strings.Contains(key, "\\") || !fs.ValidPath(key) {
		return "", fmt.Errorf("invalid object key %q", key)
	}
	return filepath.Localize(key)
}

func (d *diskStore) Upload(_ context.Context, req *UploadRequest) (*UploadResult, error) {
	objectName := req.ObjectName
	if req.Prefix != "" {
		objectName = strings.TrimSuffix(req.Prefix, "/") + "/" + objectName
	}
	if req.ObjectName == "" || req.ObjectName == "." {
		return nil, fmt.Errorf("invalid object name %q", req.ObjectName)
	}
	filePath, err := diskPath(objectName)
	if err != nil {
		return nil, err
	}
	bucket, err := d.openBucket(req.Bucket, true)
	if err != nil {
		return nil, err
	}
	defer bucket.Close()

	dir := filepath.Dir(filePath)
	if err := bucket.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create object directory: %w", err)
	}
	tempPath := filepath.Join(dir, ".upload-"+rand.Text())
	file, err := bucket.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return nil, fmt.Errorf("create upload: %w", err)
	}
	defer bucket.Remove(tempPath)

	hash := md5.New()
	size, copyErr := io.Copy(io.MultiWriter(file, hash), req.Reader)
	closeErr := file.Close()
	if copyErr != nil {
		return nil, fmt.Errorf("write upload: %w", copyErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close upload: %w", closeErr)
	}
	if req.Size >= 0 && size != req.Size {
		return nil, fmt.Errorf("file size mismatch: expected %d, got %d", req.Size, size)
	}
	if err := bucket.Rename(tempPath, filePath); err != nil {
		return nil, fmt.Errorf("store upload: %w", err)
	}
	return &UploadResult{
		Bucket:     req.Bucket,
		Key:        objectName,
		ObjectName: objectName,
		ETag:       fmt.Sprintf("%x", hash.Sum(nil)),
		Size:       size,
	}, nil
}

func (d *diskStore) Download(_ context.Context, req *DownloadRequest) (io.ReadCloser, error) {
	filePath, err := diskPath(req.ObjectName)
	if err != nil {
		return nil, err
	}
	bucket, err := d.openBucket(req.Bucket, false)
	if err != nil {
		return nil, err
	}
	defer bucket.Close()
	file, err := bucket.Open(filePath)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (d *diskStore) Delete(_ context.Context, bucketName, objectName string) error {
	filePath, err := diskPath(objectName)
	if err != nil {
		return err
	}
	bucket, err := d.openBucket(bucketName, false)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer bucket.Close()
	if err := bucket.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	for dir := filepath.Dir(filePath); dir != "."; dir = filepath.Dir(dir) {
		if err := bucket.Remove(dir); err != nil {
			break
		}
	}
	return nil
}

func (d *diskStore) List(_ context.Context, bucketName, prefix string) ([]FileInfo, error) {
	searchPath := "."
	if prefix != "" {
		searchPath = strings.TrimSuffix(prefix, "/")
		if _, err := diskPath(searchPath); err != nil {
			return nil, err
		}
	}

	bucket, err := d.openBucket(bucketName, false)
	if os.IsNotExist(err) {
		return []FileInfo{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer bucket.Close()

	var files []FileInfo
	err = fs.WalkDir(bucket.FS(), searchPath, func(key string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if key == searchPath || entry.Type()&fs.ModeSymlink != 0 {
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.IsDir() {
			key += "/"
		}
		file := diskFileInfo(key, info)
		if info.Mode().IsRegular() {
			file.ETag, _ = diskHash(bucket, filepath.FromSlash(key))
		}
		files = append(files, file)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}
	return files, nil
}

func (d *diskStore) GetObjectStat(_ context.Context, bucketName, objectName string) (*FileInfo, error) {
	filePath, err := diskPath(strings.TrimSuffix(objectName, "/"))
	if err != nil {
		return nil, err
	}
	bucket, err := d.openBucket(bucketName, false)
	if err != nil {
		return nil, err
	}
	defer bucket.Close()
	info, err := bucket.Stat(filePath)
	if err != nil {
		return nil, err
	}

	file := diskFileInfo(objectName, info)
	file.ContentType = "application/octet-stream"
	if info.Mode().IsRegular() {
		file.ETag, _ = diskHash(bucket, filePath)
		f, err := bucket.Open(filePath)
		if err == nil {
			defer f.Close()
			var buf [512]byte
			n, err := io.ReadFull(f, buf[:])
			if err == nil || err == io.EOF || err == io.ErrUnexpectedEOF {
				file.ContentType = DetectContentType(buf[:n], filepath.Ext(filePath))
			}
		}
	}
	return &file, nil
}

func diskFileInfo(key string, info fs.FileInfo) FileInfo {
	fileType := GetFileType(key)
	if info.IsDir() {
		fileType = "folder"
	}
	return FileInfo{
		Key:          key,
		Name:         path.Base(key),
		LastModified: info.ModTime(),
		Size:         info.Size(),
		Type:         fileType,
		Path:         key,
		IsFolder:     info.IsDir(),
	}
}

func diskHash(bucket *os.Root, name string) (string, error) {
	file, err := bucket.Open(name)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
