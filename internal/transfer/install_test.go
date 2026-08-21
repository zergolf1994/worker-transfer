package transfer

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirectoryFilesSizeBeforeInstallDir(t *testing.T) {
	source := filepath.Join(t.TempDir(), "sprite")
	if err := os.MkdirAll(filepath.Join(source, "ignored"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"sprite.vtt":   "12345",
		"sprite-1.jpg": "1234567",
	} {
		if err := os.WriteFile(filepath.Join(source, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	size, err := directoryFilesSize(source)
	if err != nil {
		t.Fatal(err)
	}
	if size != 12 {
		t.Fatalf("directoryFilesSize() = %d, want 12", size)
	}

	storagePath := t.TempDir()
	if err := installDir(storagePath, "file-1", "sprite", source); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(storagePath, "file-1", "sprite", "sprite.vtt")); err != nil {
		t.Fatalf("installed sprite.vtt: %v", err)
	}

	afterMove, err := directoryFilesSize(source)
	if err != nil {
		t.Fatal(err)
	}
	if afterMove != 0 {
		t.Fatalf("source size after installDir() = %d, want 0", afterMove)
	}
}
