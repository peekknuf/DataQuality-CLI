package io

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"syscall"
)

// MMapReader provides memory-mapped file I/O for efficient large file processing
type MMapReader struct {
	file       *os.File
	data       []byte
	size       int64
	offset     int64
	chunkSize  int64
	isMapped   bool
	useMmap    bool
}

// MMapConfig contains configuration for memory-mapped reading
type MMapConfig struct {
	ChunkSize    int64  // Size of chunks to map
	MaxMapSize   int64  // Maximum size to memory map
	UseMmap      bool   // Whether to use memory mapping
	FallbackSize int64  // Size for regular I/O when mmap is disabled
}

// DefaultMMapConfig returns a default configuration
func DefaultMMapConfig() MMapConfig {
	return MMapConfig{
		ChunkSize:    64 * 1024 * 1024, // 64MB chunks
		MaxMapSize:   512 * 1024 * 1024, // 512MB max map size
		UseMmap:      true,
		FallbackSize: 1024 * 1024, // 1MB for regular I/O
	}
}

// NewMMapReader creates a new memory-mapped file reader
func NewMMapReader(filePath string, config MMapConfig) (*MMapReader, error) {
	if config.ChunkSize == 0 {
		config.ChunkSize = DefaultMMapConfig().ChunkSize
	}
	if config.MaxMapSize == 0 {
		config.MaxMapSize = DefaultMMapConfig().MaxMapSize
	}
	if config.FallbackSize == 0 {
		config.FallbackSize = DefaultMMapConfig().FallbackSize
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	size := fileInfo.Size()
	
	// Determine if we should use memory mapping
	useMmap := config.UseMmap && size <= config.MaxMapSize && size > 0
	
	reader := &MMapReader{
		file:      file,
		size:      size,
		chunkSize: config.ChunkSize,
		isMapped:  false,
		useMmap:   useMmap,
		offset:    0,
	}

	// Try to memory map the file if enabled
	if useMmap {
		if err := reader.mmapFile(); err != nil {
			// Fall back to regular I/O if memory mapping fails
			reader.useMmap = false
			fmt.Printf("Warning: Memory mapping failed, falling back to regular I/O: %v\n", err)
		}
	}

	return reader, nil
}

// mmapFile memory maps the entire file
func (r *MMapReader) mmapFile() error {
	if r.size <= 0 {
		return fmt.Errorf("invalid file size: %d", r.size)
	}

	// Memory map the file
	data, err := syscall.Mmap(int(r.file.Fd()), 0, int(r.size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("memory mapping failed: %w", err)
	}

	r.data = data
	r.isMapped = true
	return nil
}

// ReadChunk reads the next chunk of data from the file
func (r *MMapReader) ReadChunk() ([]byte, error) {
	if r.offset >= r.size {
		return nil, nil // EOF
	}

	if r.useMmap && r.isMapped {
		return r.readMappedChunk()
	}

	return r.readRegularChunk()
}

// readMappedChunk reads a chunk from memory-mapped data
func (r *MMapReader) readMappedChunk() ([]byte, error) {
	if r.offset >= r.size {
		return nil, nil // EOF
	}

	// Calculate chunk boundaries
	start := r.offset
	end := start + r.chunkSize
	if end > r.size {
		end = r.size
	}

	// Extract chunk from mapped data
	chunk := r.data[start:end]
	r.offset = end

	return chunk, nil
}

// readRegularChunk reads a chunk using regular I/O
func (r *MMapReader) readRegularChunk() ([]byte, error) {
	if r.offset >= r.size {
		return nil, nil // EOF
	}

	// Calculate chunk size
	chunkSize := r.chunkSize
	remaining := r.size - r.offset
	if remaining < chunkSize {
		chunkSize = remaining
	}

	// Allocate buffer for chunk
	chunk := make([]byte, chunkSize)

	// Seek to offset
	_, err := r.file.Seek(r.offset, 0)
	if err != nil {
		return nil, fmt.Errorf("seek failed: %w", err)
	}

	// Read chunk
	n, err := r.file.Read(chunk)
	if err != nil {
		return nil, fmt.Errorf("read failed: %w", err)
	}

	r.offset += int64(n)
	return chunk[:n], nil
}

// Seek moves the read offset to the specified position
func (r *MMapReader) Seek(offset int64) error {
	if offset < 0 || offset > r.size {
		return fmt.Errorf("invalid offset: %d", offset)
	}

	r.offset = offset
	return nil
}

// Size returns the total size of the file
func (r *MMapReader) Size() int64 {
	return r.size
}

// Offset returns the current read offset
func (r *MMapReader) Offset() int64 {
	return r.offset
}

// IsMapped returns true if the file is memory mapped
func (r *MMapReader) IsMapped() bool {
	return r.isMapped
}

// Close closes the reader and unmaps the file if necessary
func (r *MMapReader) Close() error {
	var err error

	// Unmap file if memory mapped
	if r.isMapped && r.data != nil {
		if unmapErr := syscall.Munmap(r.data); unmapErr != nil {
			err = fmt.Errorf("unmap failed: %w", unmapErr)
		}
		r.data = nil
		r.isMapped = false
	}

	// Close file
	if r.file != nil {
		if closeErr := r.file.Close(); closeErr != nil {
			if err != nil {
				err = fmt.Errorf("%v; close failed: %w", err, closeErr)
			} else {
				err = fmt.Errorf("close failed: %w", closeErr)
			}
		}
		r.file = nil
	}

	return err
}

// MMapPool provides a pool of memory-mapped readers for reuse
type MMapPool struct {
	pool    sync.Pool
	config  MMapConfig
	maxSize int
}

// NewMMapPool creates a new pool of memory-mapped readers
func NewMMapPool(config MMapConfig, poolSize int) *MMapPool {
	if poolSize <= 0 {
		poolSize = runtime.NumCPU()
	}

	return &MMapPool{
		config:  config,
		maxSize: poolSize,
		pool: sync.Pool{
			New: func() interface{} {
				return make([]*MMapReader, 0, poolSize)
			},
		},
	}
}

// Get retrieves a reader from the pool or creates a new one
func (p *MMapPool) Get(filePath string) (*MMapReader, error) {
	return NewMMapReader(filePath, p.config)
}

// Put returns a reader to the pool for reuse
func (p *MMapPool) Put(reader *MMapReader) {
	if reader == nil {
		return
	}

	// Reset reader state
	reader.offset = 0

	// Close and unmap to free resources
	reader.Close()
}

// ChunkedReader provides chunked reading with automatic memory management
type ChunkedReader struct {
	reader    *MMapReader
	chunkSize int
	buffer    []byte
	pos       int
	eof       bool
}

// NewChunkedReader creates a new chunked reader
func NewChunkedReader(reader *MMapReader, chunkSize int) *ChunkedReader {
	if chunkSize <= 0 {
		chunkSize = 64 * 1024 // 64KB default
	}

	return &ChunkedReader{
		reader:    reader,
		chunkSize: chunkSize,
		buffer:    make([]byte, 0, chunkSize),
		pos:       0,
		eof:       false,
	}
}

// Read reads data into the provided buffer
func (r *ChunkedReader) Read(p []byte) (int, error) {
	if r.eof {
		return 0, nil // EOF
	}

	// If buffer is empty, read next chunk
	if r.pos >= len(r.buffer) {
		chunk, err := r.reader.ReadChunk()
		if err != nil {
			return 0, err
		}

		if chunk == nil {
			r.eof = true
			return 0, nil // EOF
		}

		// Replace buffer with new chunk
		r.buffer = chunk
		r.pos = 0
	}

	// Copy from buffer to provided slice
	n := copy(p, r.buffer[r.pos:])
	r.pos += n

	return n, nil
}

// ReadByte reads a single byte
func (r *ChunkedReader) ReadByte() (byte, error) {
	if r.eof {
		return 0, nil // EOF
	}

	// If buffer is empty, read next chunk
	if r.pos >= len(r.buffer) {
		chunk, err := r.reader.ReadChunk()
		if err != nil {
			return 0, err
		}

		if chunk == nil {
			r.eof = true
			return 0, nil // EOF
		}

		// Replace buffer with new chunk
		r.buffer = chunk
		r.pos = 0
	}

	b := r.buffer[r.pos]
	r.pos++
	return b, nil
}

// Close closes the chunked reader
func (r *ChunkedReader) Close() error {
	if r.reader != nil {
		return r.reader.Close()
	}
	return nil
}

// BufferedFileReader provides a buffered file reader with automatic chunk management
type BufferedFileReader struct {
	file     *os.File
	buffer   []byte
	pos      int
	size     int
	eof      bool
	fileSize int64
}

// NewBufferedFileReader creates a new buffered file reader
func NewBufferedFileReader(filePath string, bufferSize int) (*BufferedFileReader, error) {
	if bufferSize <= 0 {
		bufferSize = 1024 * 1024 // 1MB default
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	return &BufferedFileReader{
		file:     file,
		buffer:   make([]byte, bufferSize),
		pos:      0,
		size:     0,
		eof:      false,
		fileSize: fileInfo.Size(),
	}, nil
}

// Read reads data into the provided buffer
func (r *BufferedFileReader) Read(p []byte) (int, error) {
	if r.eof {
		return 0, nil // EOF
	}

	// If buffer is empty, read next chunk
	if r.pos >= r.size {
		n, err := r.file.Read(r.buffer)
		if err != nil {
			return 0, err
		}

		if n == 0 {
			r.eof = true
			return 0, nil // EOF
		}

		r.size = n
		r.pos = 0
	}

	// Copy from buffer to provided slice
	n := copy(p, r.buffer[r.pos:])
	r.pos += n

	return n, nil
}

// Close closes the buffered file reader
func (r *BufferedFileReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// Size returns the total file size
func (r *BufferedFileReader) Size() int64 {
	return r.fileSize
}

// IsEOF returns true if the reader has reached the end of the file
func (r *BufferedFileReader) IsEOF() bool {
	return r.eof
}