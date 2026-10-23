package multistream

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func localSpec(t *testing.T, size uint64) LocalTempSpec {
	t.Helper()
	dir := t.TempDir()
	return LocalTempSpec{FinalPath: filepath.Join(dir, "final.bin"), TempPath: filepath.Join(dir, ".temp.bin"), Size: size}
}

func TestFreshLocalTempAndNoClobberPublish(t *testing.T) {
	spec := localSpec(t, 8192)
	f, err := PrepareFreshLocalTemp(spec)
	if err != nil {
		t.Fatal(err)
	}
	payload := patternBytes(spec.Size)
	if _, err := f.WriteAt(payload, 0); err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(1_700_000_000, 123456789)
	if err := SetLocalTempMeta(f, spec, 0o640, mtime); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := PublishLocalFinal(spec, mtime); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(spec.FinalPath)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("published content mismatch: %v", err)
	}
	fi, _ := os.Lstat(spec.FinalPath)
	if fi.Mode().Perm() != 0o640 || !fi.ModTime().Equal(mtime) {
		t.Fatalf("published metadata = %v %v", fi.Mode().Perm(), fi.ModTime())
	}
	published, err := ProbeLocalPublishState(spec)
	if err != nil || !published {
		t.Fatalf("ProbeLocalPublishState = %v, %v", published, err)
	}
	if err := PublishLocalFinal(spec, mtime); !errors.Is(err, ErrLocalFinalExists) {
		t.Fatalf("second publish = %v", err)
	}
}

func TestFreshLocalTempRefusesExistingObjects(t *testing.T) {
	t.Run("final", func(t *testing.T) {
		spec := localSpec(t, 4)
		if err := os.WriteFile(spec.FinalPath, []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := PrepareFreshLocalTemp(spec); !errors.Is(err, ErrLocalFinalExists) {
			t.Fatalf("error = %v", err)
		}
		got, _ := os.ReadFile(spec.FinalPath)
		if string(got) != "keep" {
			t.Fatal("existing final changed")
		}
	})
	t.Run("temp", func(t *testing.T) {
		spec := localSpec(t, 4)
		if err := os.WriteFile(spec.TempPath, []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := PrepareFreshLocalTemp(spec); !errors.Is(err, ErrLocalTempExists) {
			t.Fatalf("error = %v", err)
		}
		got, _ := os.ReadFile(spec.TempPath)
		if string(got) != "keep" {
			t.Fatal("existing temp changed")
		}
	})
}

func TestResumeLocalTempValidation(t *testing.T) {
	spec := localSpec(t, 1024)
	if _, err := OpenResumeLocalTemp(spec); !errors.Is(err, ErrLocalTempMissing) {
		t.Fatalf("missing = %v", err)
	}
	if err := os.WriteFile(spec.TempPath, make([]byte, spec.Size), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := OpenResumeLocalTemp(spec)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if err := os.Truncate(spec.TempPath, int64(spec.Size-1)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenResumeLocalTemp(spec); !errors.Is(err, ErrLocalTempMismatch) {
		t.Fatalf("mismatch = %v", err)
	}
}

func TestProbeLocalPublishConflict(t *testing.T) {
	spec := localSpec(t, 4)
	if err := os.WriteFile(spec.TempPath, []byte("temp"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(spec.FinalPath, []byte("else"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ProbeLocalPublishState(spec); !errors.Is(err, ErrPublishConflict) {
		t.Fatalf("conflict = %v", err)
	}
}

func TestLocalTempSpecValidation(t *testing.T) {
	for _, spec := range []LocalTempSpec{{}, {FinalPath: "relative", TempPath: "/tmp/x"}, {FinalPath: "/tmp/x", TempPath: "/else/y"}, {FinalPath: "/tmp/x", TempPath: "/tmp/x"}} {
		if _, err := PrepareFreshLocalTemp(spec); !errors.Is(err, ErrInvalidLocalTemp) {
			t.Fatalf("spec %+v error = %v", spec, err)
		}
	}
}
