package daemon

import (
	"container/heap"
	"context"
	"errors"
	"os"
	"path/filepath"
	"unicode/utf8"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

// manifestConfig bounds traversal memory and stream size. The defaults are
// production values; tests lower them to exercise the limits.
type manifestConfig struct {
	MaxEntries         int
	MaxBytes           int
	MaxFrontierEntries int
	MaxFrontierBytes   int
	MaxDirEntries      int
}

func defaultManifestConfig() manifestConfig {
	return manifestConfig{
		MaxEntries:         5_000_000,
		MaxBytes:           1 << 30,
		MaxFrontierEntries: 1_000_000,
		MaxFrontierBytes:   256 << 20,
		MaxDirEntries:      1_000_000,
	}
}

// precheckManifest validates the root before any response bytes are written
// and applies each mode's missing-path rule.
func precheckManifest(mode protocol.ManifestMode, root string) error {
	info, err := os.Stat(root)
	switch mode {
	case protocol.ModeDestTree:
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return protocol.NewError(protocol.CodeNotFound, "destination directory %q does not exist", root)
			}
			return osError("stat destination", err)
		}
		if !info.IsDir() {
			// Matching the existing `if [ -d ] ... else exit 42` behavior:
			// an authoritative empty destination.
			return protocol.NewError(protocol.CodeNotFound, "destination %q is not a directory", root)
		}
	case protocol.ModeSourceTree:
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return protocol.NewError(protocol.CodeNotFound, "source %q does not exist", root)
			}
			return osError("stat source", err)
		}
		if !info.IsDir() {
			return protocol.NewError(protocol.CodeNotDir, "source %q is not a directory", root)
		}
	case protocol.ModeSourceFile:
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return protocol.NewError(protocol.CodeNotFound, "source file %q does not exist", root)
			}
			return osError("stat source file", err)
		}
		if info.IsDir() {
			return protocol.NewError(protocol.CodeNotDir, "source file %q is a directory", root)
		}
	default:
		return protocol.NewError(protocol.CodeInvalidArgument, "unknown manifest mode %q", mode)
	}
	return nil
}

// osError maps an OS error to a protocol error.
func osError(op string, err error) error {
	switch {
	case errors.Is(err, os.ErrNotExist):
		return protocol.NewError(protocol.CodeNotFound, "%s: %v", op, err)
	case errors.Is(err, os.ErrPermission):
		return protocol.NewError(protocol.CodePermission, "%s: %v", op, err)
	default:
		return protocol.NewError(protocol.CodeInternal, "%s: %v", op, err)
	}
}

// manifestWalker emits entries in global path byte order using a heap merge:
// a directory's children replace its marker, so memory is bounded by the
// frontier rather than the whole tree.
type manifestWalker struct {
	ctx  context.Context
	mode protocol.ManifestMode
	cfg  manifestConfig
	h    walkHeap
	emit func(protocol.ManifestEntryRecord) error

	entries    int
	bytes      int
	frontierBs int
}

type walkKind int

const (
	walkFile walkKind = iota
	walkDir
)

type walkItem struct {
	key  string // heap order: rel for files, rel+"/" for directories
	rel  string // slash-relative path without a trailing separator
	abs  string
	kind walkKind
	info os.FileInfo
}

type walkHeap []walkItem

func (h walkHeap) Len() int           { return len(h) }
func (h walkHeap) Less(i, j int) bool { return h[i].key < h[j].key }
func (h walkHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *walkHeap) Push(x any)        { *h = append(*h, x.(walkItem)) }
func (h *walkHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// walkTree walks root in the mode's rules and calls emit for each entry.
func walkTree(ctx context.Context, mode protocol.ManifestMode, root string, cfg manifestConfig, emit func(protocol.ManifestEntryRecord) error) (int, error) {
	if emit == nil {
		return 0, protocol.NewError(protocol.CodeInternal, "nil emit")
	}
	if mode == protocol.ModeSourceFile {
		info, err := os.Lstat(root)
		if err != nil {
			return 0, osError("lstat source file", err)
		}
		name := filepath.Base(root)
		if len(name) > protocol.MaxPathBytes {
			return 0, protocol.NewError(protocol.CodeTooLarge, "file name exceeds %d bytes", protocol.MaxPathBytes)
		}
		if err := protocol.ValidateRelPath(name); err != nil {
			return 0, err
		}
		rec, err := entryRecord(name, info)
		if err != nil {
			return 0, err
		}
		if err := emit(rec); err != nil {
			return 0, err
		}
		return 1, nil
	}

	w := &manifestWalker{ctx: ctx, mode: mode, cfg: cfg, emit: emit}
	if cfg.MaxEntries <= 0 {
		cfg = defaultManifestConfig()
		w.cfg = cfg
	}
	if err := w.pushDir(root, ""); err != nil {
		return 0, err
	}
	for w.h.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		it := heap.Pop(&w.h).(walkItem)
		w.frontierBs -= frontierCost(it.key)
		if it.kind == walkDir {
			if err := w.pushDir(it.abs, it.rel); err != nil {
				return 0, err
			}
			continue
		}
		shouldEmit := true
		if mode == protocol.ModeDestTree {
			shouldEmit = it.info.Mode().IsRegular()
		}
		if !shouldEmit {
			continue
		}
		rec, err := entryRecord(it.rel, it.info)
		if err != nil {
			return 0, err
		}
		w.entries++
		w.bytes += len(rec.Path) + 48
		if w.entries > w.cfg.MaxEntries || w.bytes > w.cfg.MaxBytes {
			return w.entries, protocol.NewError(protocol.CodeManifestTooLarge, "manifest exceeds the configured limits")
		}
		if err := w.emit(rec); err != nil {
			return w.entries, err
		}
	}
	return w.entries, nil
}

// pushDir lists one directory and pushes its entries onto the frontier.
func (w *manifestWalker) pushDir(abs, rel string) error {
	entries, err := os.ReadDir(abs)
	if err != nil {
		return osError("read directory", err)
	}
	if len(entries) > w.cfg.MaxDirEntries {
		return protocol.NewError(protocol.CodeManifestTooLarge, "directory has too many entries")
	}
	for _, de := range entries {
		name := de.Name()
		childRel := name
		if rel != "" {
			childRel = rel + "/" + name
		}
		if len(childRel) > protocol.MaxPathBytes {
			return protocol.NewError(protocol.CodeTooLarge, "path exceeds %d bytes", protocol.MaxPathBytes)
		}
		if !utf8.ValidString(childRel) {
			return protocol.NewError(protocol.CodeUnsupportedPathEncoding, "path is not valid UTF-8")
		}
		if err := protocol.ValidateRelPath(childRel); err != nil {
			return err
		}
		info, err := de.Info()
		if err != nil {
			return osError("stat entry", err)
		}
		item := walkItem{info: info, rel: childRel, abs: filepath.Join(abs, name)}
		if info.IsDir() {
			item.kind = walkDir
			item.key = childRel + "/"
		} else {
			item.kind = walkFile
			item.key = childRel
		}
		w.h.Push(item)
		w.frontierBs += frontierCost(item.key)
		if w.h.Len() > w.cfg.MaxFrontierEntries || w.frontierBs > w.cfg.MaxFrontierBytes {
			return protocol.NewError(protocol.CodeManifestTooLarge, "manifest frontier exceeds the configured limits")
		}
	}
	heap.Init(&w.h)
	return nil
}

func frontierCost(rel string) int { return len(rel) + 48 }

// entryRecord converts a file identity into a wire entry.
func entryRecord(rel string, info os.FileInfo) (protocol.ManifestEntryRecord, error) {
	if info == nil {
		return protocol.ManifestEntryRecord{}, protocol.NewError(protocol.CodeInternal, "entry %q has no info", rel)
	}
	kind := protocol.KindSpecial
	switch {
	case info.Mode().IsRegular():
		kind = protocol.KindFile
	case info.Mode()&os.ModeSymlink != 0:
		kind = protocol.KindSymlink
	}
	return protocol.ManifestEntryRecord{
		Type:    protocol.RecordTypeEntry,
		Path:    rel,
		Kind:    kind,
		Size:    info.Size(),
		MtimeNS: info.ModTime().UnixNano(),
		Perm:    uint32(info.Mode().Perm()),
	}, nil
}
