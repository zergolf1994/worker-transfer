package transfer

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// unzip extracts a zip file into destDir, checking ctx between entries so
// an admin cancel aborts mid-extract.
func unzip(ctx context.Context, zipPath, destDir string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return err
	}

	for _, f := range r.File {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		name := filepath.Join(destDir, f.Name)
		if !strings.HasPrefix(filepath.Clean(name), filepath.Clean(destDir)+string(os.PathSeparator)) {
			return fmt.Errorf("invalid zip path: %s", f.Name)
		}
		if f.FileInfo().IsDir() {
			os.MkdirAll(name, 0755)
			continue
		}
		if err := os.MkdirAll(filepath.Dir(name), 0755); err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		out, err := os.Create(name)
		if err != nil {
			rc.Close()
			return err
		}
		_, err = io.Copy(out, rc)
		out.Close()
		rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func zipDir(ctx context.Context, sourceDir, zipPath string) error {
	out, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	zw := zip.NewWriter(out)
	closeWithError := func(err error) error {
		_ = zw.Close()
		_ = out.Close()
		return err
	}
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		return closeWithError(err)
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return closeWithError(ctx.Err())
		}
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(sourceDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return closeWithError(err)
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return closeWithError(err)
		}
		header.Name = entry.Name()
		header.Method = zip.Deflate
		writer, err := zw.CreateHeader(header)
		if err != nil {
			return closeWithError(err)
		}
		in, err := os.Open(path)
		if err != nil {
			return closeWithError(err)
		}
		_, copyErr := io.Copy(writer, in)
		closeErr := in.Close()
		if copyErr != nil {
			return closeWithError(copyErr)
		}
		if closeErr != nil {
			return closeWithError(closeErr)
		}
	}
	if err := zw.Close(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
