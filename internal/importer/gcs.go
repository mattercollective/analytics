package importer

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// GCSClient wraps the Google Cloud Storage client for listing and reading files.
type GCSClient struct {
	client *storage.Client
}

func NewGCSClient(ctx context.Context) (*GCSClient, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create GCS client: %w", err)
	}
	return &GCSClient{client: client}, nil
}

func (g *GCSClient) Close() error {
	return g.client.Close()
}

// ListFiles returns all object paths under the given bucket/prefix.
func (g *GCSClient) ListFiles(ctx context.Context, bucket, prefix string) ([]string, error) {
	it := g.client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})

	var paths []string
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}
		// Skip directory markers
		if attrs.Size > 0 {
			paths = append(paths, attrs.Name)
		}
	}
	return paths, nil
}

// ReadFile returns a reader for a file in GCS. Caller must close the reader.
// Automatically decompresses .gz files.
func (g *GCSClient) ReadFile(ctx context.Context, bucket, path string) (io.ReadCloser, error) {
	obj := g.client.Bucket(bucket).Object(path)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	if strings.HasSuffix(path, ".gz") {
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			reader.Close()
			return nil, fmt.Errorf("decompress %s: %w", path, err)
		}
		return &gzReadCloser{gz: gzReader, underlying: reader}, nil
	}

	return reader, nil
}

// FileExt returns the extension of a GCS path (handles .csv.zip, .csv.gz, etc.)
func FileExt(path string) string {
	base := filepath.Base(path)
	if strings.HasSuffix(base, ".csv.zip") {
		return ".csv.zip"
	}
	if strings.HasSuffix(base, ".csv.gz") {
		return ".csv.gz"
	}
	return filepath.Ext(base)
}

type gzReadCloser struct {
	gz         *gzip.Reader
	underlying io.ReadCloser
}

func (r *gzReadCloser) Read(p []byte) (int, error) { return r.gz.Read(p) }
func (r *gzReadCloser) Close() error {
	r.gz.Close()
	return r.underlying.Close()
}
