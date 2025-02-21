package connectors

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FileMeta struct {
	Path     string
	Size     int64
	Modified time.Time
	IsDir    bool
}

type DiscoveryOptions struct {
	Recursive      bool
	MinSize        int64
	MaxSize        int64
	ModifiedAfter  time.Time
	ModifiedBefore time.Time
	Progress       func(path string, d fs.DirEntry, err error) error
}

func DiscoverFiles(root string, ext string, options DiscoveryOptions) ([]FileMeta, error) {
	if root == "" {
		return nil, fmt.Errorf("root directory cannot be empty")
	}

	stat, err := os.Stat(root)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("directory does not exist: %s", root)
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("path is not a directory: %s", root)
	}

	ext = strings.TrimPrefix(ext, ".")
	if ext == "" {
		return nil, fmt.Errorf("file extension cannot be empty")
	}

	var files []FileMeta
	walkFunc := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %s: %w", path, err)
		}

		if d.IsDir() && path != root && !options.Recursive {
			return filepath.SkipDir
		}

		if !d.IsDir() && strings.EqualFold(filepath.Ext(path), "."+ext) {
			info, err := d.Info()
			if err != nil {
				return fmt.Errorf("error getting file info for %s: %w", path, err)
			}

			if options.MinSize > 0 && info.Size() < options.MinSize {
				return nil
			}
			if options.MaxSize > 0 && info.Size() > options.MaxSize {
				return nil
			}
			if !options.ModifiedAfter.IsZero() && info.ModTime().Before(options.ModifiedAfter) {
				return nil
			}
			if !options.ModifiedBefore.IsZero() && info.ModTime().After(options.ModifiedBefore) {
				return nil
			}

			files = append(files, FileMeta{
				Path:     path,
				Size:     info.Size(),
				Modified: info.ModTime(),
				IsDir:    d.IsDir(),
			})
		}

		return nil
	}

	if options.Progress != nil {
		err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err := options.Progress(path, d, err); err != nil {
				return err
			}
			return walkFunc(path, d, err)
		})
	} else {
		err = filepath.WalkDir(root, walkFunc)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no matching files found in %s", root)
	}

	return files, nil
}
