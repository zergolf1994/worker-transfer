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
