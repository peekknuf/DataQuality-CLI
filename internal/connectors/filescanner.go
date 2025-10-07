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

func DiscoverFiles(root string, ext string, options DiscoveryOptions) ([]FileMeta, int, error) {
	if err := validateDiscoveryParams(root, ext); err != nil {
		return nil, 0, err
	}

	ext = strings.TrimPrefix(ext, ".")
	var files []FileMeta
	var fileCount int

	walkFunc := createWalkFunc(ext, options, &files, &fileCount, root)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if options.Progress != nil {
			options.Progress(path, d, walkErr)
		}
		return walkFunc(path, d, walkErr)
	})
	if err != nil {
		return nil, 0, fmt.Errorf("error walking directory: %w", err)
	}

	if len(files) == 0 {
		return files, 0, fmt.Errorf("no matching files found in %s", root)
	}

	return files, fileCount, nil
}

func validateDiscoveryParams(root string, ext string) error {
	if root == "" {
		return fmt.Errorf("root directory cannot be empty")
	}

	stat, err := os.Stat(root)
	if os.IsNotExist(err) {
		return fmt.Errorf("directory does not exist: %s", root)
	}
	if !stat.IsDir() {
		return fmt.Errorf("path is not a directory: %s", root)
	}

	ext = strings.TrimPrefix(ext, ".")
	if ext == "" {
		return fmt.Errorf("file extension cannot be empty")
	}

	return nil
}

func createWalkFunc(ext string, options DiscoveryOptions, files *[]FileMeta, fileCount *int, root string) func(path string, d fs.DirEntry, err error) error {
	return func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %s: %w", path, err)
		}

		if d.IsDir() && path != root && !options.Recursive {
			return filepath.SkipDir
		}

		if !d.IsDir() && strings.EqualFold(filepath.Ext(path), "."+ext) {
			return processMatchingFile(path, d, options, files, fileCount)
		}

		return nil
	}
}

func processMatchingFile(path string, d fs.DirEntry, options DiscoveryOptions, files *[]FileMeta, fileCount *int) error {
	info, err := d.Info()
	if err != nil {
		return fmt.Errorf("error getting file info for %s: %w", path, err)
	}

	if !matchesFileCriteria(info, options) {
		return nil
	}

	*files = append(*files, FileMeta{
		Path:     path,
		Size:     info.Size(),
		Modified: info.ModTime(),
		IsDir:    d.IsDir(),
	})
	*fileCount++

	return nil
}

func matchesFileCriteria(info fs.FileInfo, options DiscoveryOptions) bool {
	if options.MinSize > 0 && info.Size() < options.MinSize {
		return false
	}
	if options.MaxSize > 0 && info.Size() > options.MaxSize {
		return false
	}
	if !options.ModifiedAfter.IsZero() && info.ModTime().Before(options.ModifiedAfter) {
		return false
	}
	if !options.ModifiedBefore.IsZero() && info.ModTime().After(options.ModifiedBefore) {
		return false
	}
	return true
}
