// Package discovery streams local trees and parses remote source or
// destination manifests for bidirectional planning and revalidation.
//
// Local discovery (Walk, Stream) enumerates the source tree without ever
// following symbolic links: every entry beneath the root is inspected with
// Lstat, so a symlink — even one pointing at a directory — is reported as a
// symlink and never descended into. Regular files are reported with a
// slash-separated path relative to the root plus size and modification
// time; symlinks and special files (devices, FIFOs, sockets, ...) are
// reported with their kind so the planner can mark them unsupported;
// per-entry scan failures are reported as error entries while the walk
// continues past them.
//
// Entries stream through a callback (Walk) or a bounded channel (Stream);
// the walker never buffers the whole tree, reading directories in fixed
// batches.
//
// The explicitly named root is the single exception to the no-follow rule:
// it is resolved with Stat, so a source root that is itself a symlink to a
// directory still works (matching the source validation contract). Nothing
// beneath the root is ever followed.
package discovery

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Kind classifies a discovered entry.
type Kind int

// Entry kinds. Only KindFile entries are transferable; the planner marks
// the rest unsupported.
const (
	// KindFile is a regular file.
	KindFile Kind = iota
	// KindSymlink is a symbolic link. The link target is never inspected.
	KindSymlink
	// KindSpecial is anything that is neither a regular file nor a
	// symlink nor a directory: devices, FIFOs, sockets, and the like.
	KindSpecial
)

// String renders the kind for diagnostics.
func (k Kind) String() string {
	switch k {
	case KindFile:
		return "file"
	case KindSymlink:
		return "symlink"
	case KindSpecial:
		return "special"
	}
	return fmt.Sprintf("kind(%d)", int(k))
}

// Entry is one discovered item. Path is always slash-separated and relative
// to the walk root. Size and ModTime are meaningful only for KindFile.
// Err is set when scanning this entry failed; the entry then carries no
// metadata beyond the path at which the scan failed.
type Entry struct {
	// Path is the slash-separated path relative to the walk root.
	Path string
	// Size is the file size in bytes (KindFile only).
	Size int64
	// ModTime is the file modification time (KindFile only).
	ModTime time.Time
	// Kind classifies the entry.
	Kind Kind
	// Err reports a per-entry scan failure; the walk continues past it.
	Err error
}

// readBatch bounds how many directory entries are loaded at once while
// walking, keeping per-directory memory independent of directory size.
const readBatch = 128

// ValidateRelPath rejects paths that cannot safely name an entry relative
// to a tree root: empty paths, absolute paths, parent-directory or current
// -directory elements, empty elements (doubled or trailing slashes), and
// NUL bytes. Both local discovery and remote manifest parsing apply it, so
// a path can never escape the source or destination root.
func ValidateRelPath(path string) error {
	if path == "" {
		return errors.New("path is empty")
	}
	if strings.ContainsRune(path, 0) {
		return fmt.Errorf("path %q contains a NUL byte", path)
	}
	if strings.HasPrefix(path, "/") {
		return fmt.Errorf("path %q is absolute; want relative", path)
	}
	for _, elem := range strings.Split(path, "/") {
		switch elem {
		case "..":
			return fmt.Errorf("path %q contains a parent-directory element", path)
		case ".":
			return fmt.Errorf("path %q contains a current-directory element", path)
		case "":
			return fmt.Errorf("path %q contains an empty element", path)
		}
	}
	return nil
}

// Walk streams every entry of the tree rooted at root to emit in directory
// order, recursively. It returns immediately when emit returns an error,
// propagating that error; per-entry scan failures are delivered as error
// entries instead, and the walk continues past them.
//
// The root operand itself is resolved with Stat (a symlinked source root is
// followed once, like rsync's source argument); everything beneath it is
// inspected with Lstat and symlinks are never followed.
func Walk(root string, emit func(Entry) error) error {
	if emit == nil {
		return errors.New("discovery: emit callback is nil")
	}
	if root == "" {
		return errors.New("discovery: root path is empty")
	}
	if strings.ContainsRune(root, 0) {
		return fmt.Errorf("discovery: root path %q contains a NUL byte", root)
	}
	info, err := os.Stat(root) // anchor only; see package doc comment
	if err != nil {
		return fmt.Errorf("discovery: stat root %q: %w", root, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("discovery: root %q is not a directory", root)
	}
	return walkDir(root, "", emit)
}

// walkDir streams the direct children of root/rel and then recurses into
// each subdirectory. Subdirectory traversal is deferred until after the
// directory handle is closed, so open file descriptors never scale with
// tree depth.
func walkDir(root, rel string, emit func(Entry) error) error {
	dirAbs := root
	if rel != "" {
		dirAbs = filepath.Join(root, filepath.FromSlash(rel))
	}
	f, err := os.Open(dirAbs)
	if err != nil {
		if rel == "" {
			return fmt.Errorf("discovery: open root %q: %w", root, err)
		}
		return emit(Entry{Path: rel, Err: fmt.Errorf("open directory: %w", err)})
	}

	var subdirs []string
	var scanErr error
	for scanErr == nil {
		var list []os.DirEntry
		list, scanErr = f.ReadDir(readBatch)
		for _, de := range list {
			childRel := de.Name()
			if rel != "" {
				childRel = rel + "/" + de.Name()
			}
			if err := scanEntry(dirAbs, childRel, de, &subdirs, emit); err != nil {
				_ = f.Close()
				return err
			}
		}
	}
	if errors.Is(scanErr, io.EOF) {
		scanErr = nil
	}
	if cerr := f.Close(); cerr != nil && scanErr == nil {
		scanErr = fmt.Errorf("close directory: %w", cerr)
	}
	if scanErr != nil {
		if err := emit(Entry{Path: rel, Err: fmt.Errorf("read directory: %w", scanErr)}); err != nil {
			return err
		}
	}
	for _, name := range subdirs {
		childRel := name
		if rel != "" {
			childRel = rel + "/" + name
		}
		if err := walkDir(root, childRel, emit); err != nil {
			return err
		}
	}
	return nil
}

// scanEntry inspects one directory entry with Lstat and emits it. Directory
// names are queued in subdirs for later recursion instead of being emitted.
// It returns the emit error, if any, which aborts the whole walk.
func scanEntry(dirAbs, childRel string, de os.DirEntry, subdirs *[]string, emit func(Entry) error) error {
	if err := ValidateRelPath(childRel); err != nil {
		// A name read from a real directory cannot produce an escaping
		// relative path; report defensively instead of emitting it.
		return emit(Entry{Path: childRel, Err: fmt.Errorf("invalid entry path: %w", err)})
	}
	fi, err := os.Lstat(filepath.Join(dirAbs, de.Name()))
	if err != nil {
		return emit(Entry{Path: childRel, Err: fmt.Errorf("lstat: %w", err)})
	}
	switch {
	case fi.Mode().IsRegular():
		return emit(Entry{
			Path:    childRel,
			Size:    fi.Size(),
			ModTime: fi.ModTime(),
			Kind:    KindFile,
		})
	case fi.IsDir():
		*subdirs = append(*subdirs, de.Name())
		return nil
	case fi.Mode()&os.ModeSymlink != 0:
		return emit(Entry{Path: childRel, Kind: KindSymlink})
	default:
		return emit(Entry{Path: childRel, Kind: KindSpecial})
	}
}

// Stream walks root in a background goroutine and returns its entries on a
// channel with capacity buf, so at most buf entries are ever buffered. The
// entry channel is closed when the walk ends; the error channel then
// delivers the walk result (nil on success) and closes.
//
// Callers must drain the entry channel to completion; to stop a scan early,
// use Walk and return an error from the emit callback.
func Stream(root string, buf int) (<-chan Entry, <-chan error) {
	if buf < 1 {
		buf = 1
	}
	entries := make(chan Entry, buf)
	errc := make(chan error, 1)
	go func() {
		defer close(entries)
		defer close(errc)
		errc <- Walk(root, func(e Entry) error {
			entries <- e
			return nil
		})
	}()
	return entries, errc
}
