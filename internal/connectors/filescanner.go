package connectors

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
)

type FileMeta struct {
	Path     string
	Size     int64
	Modified int64
}

func DiscoverFiles(root string, ext string) ([]FileMeta, error) {
	var files []FileMeta

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && strings.EqualFold(filepath.Ext(path), "."+ext) {
			info, err := d.Info()
			if err != nil {
				return err
			}

			files = append(files, FileMeta{
				Path:     path,
				Size:     info.Size(),
				Modified: info.ModTime().Unix(),
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan directory: %w", err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no %s files found in %s", ext, root)
	}

	return files, nil
}
