package parser

import (
	"errors"
	"fmt"
	"unicode/utf8"
	"unsafe"
)

// ParseState represents the current state of the CSV parser
type ParseState int

const (
	StateField ParseState = iota
	StateQuote
	StateQuoteEscape
	StateDelimiter
	StateNewline
	StateEOF
)

// ParserConfig contains configuration options for the CSV parser
type ParserConfig struct {
	Delimiter    rune  // Field delimiter (comma, semicolon, tab)
	Quote        rune  // Quote character
	Escape       rune  // Escape character
	ChunkSize    int   // Size of processing chunks
	BufferSize   int   // Size of internal buffer
	TrimSpace    bool  // Whether to trim leading/trailing whitespace
	Headers      bool  // Whether first row contains headers
	MaxFieldSize int   // Maximum field size to prevent memory exhaustion
}

// DefaultParserConfig returns a default configuration for the CSV parser
func DefaultParserConfig() ParserConfig {
	return ParserConfig{
		Delimiter:    ',',
		Quote:        '"',
		Escape:       '"',
		ChunkSize:    64 * 1024, // 64KB chunks
		BufferSize:   1024 * 1024, // 1MB buffer
		TrimSpace:    true,
		Headers:      true,
		MaxFieldSize: 10 * 1024 * 1024, // 10MB max field size
	}
}

// CSVParser is a high-performance, zero-allocation CSV parser
type CSVParser struct {
	config     ParserConfig
	buffer     []byte          // Reusable buffer
	data       []byte          // Current data being parsed
	pos        int             // Current position in data
	state      ParseState      // Current parsing state
	fieldStart int             // Start position of current field
	fieldEnd   int             // End position of current field
	record     []string        // Current record being built
	headers    []string        // Column headers
	lineNum    int             // Current line number
	fieldNum   int             // Current field number
}

// NewCSVParser creates a new high-performance CSV parser
func NewCSVParser(config ParserConfig) *CSVParser {
	if config.ChunkSize == 0 {
		config.ChunkSize = DefaultParserConfig().ChunkSize
	}
	if config.BufferSize == 0 {
		config.BufferSize = DefaultParserConfig().BufferSize
	}
	if config.MaxFieldSize == 0 {
		config.MaxFieldSize = DefaultParserConfig().MaxFieldSize
	}

	return &CSVParser{
		config:  config,
		buffer:  make([]byte, 0, config.BufferSize),
		record:  make([]string, 0, 64), // Pre-allocate for typical row sizes
		headers: make([]string, 0, 64),
	}
}

// Parse initializes parsing with new data
func (p *CSVParser) Parse(data []byte) error {
	if len(data) == 0 {
		return errors.New("empty data")
	}

	p.data = data
	p.pos = 0
	p.state = StateField
	p.fieldStart = 0
	p.lineNum = 1
	p.fieldNum = 0

	// Parse headers if configured
	if p.config.Headers {
		if err := p.parseHeaders(); err != nil {
			return fmt.Errorf("failed to parse headers: %w", err)
		}
	}

	return nil
}

// parseHeaders extracts the header row from the CSV data
func (p *CSVParser) parseHeaders() error {
	p.record = p.record[:0] // Clear but preserve capacity

	for p.pos < len(p.data) {
		if err := p.parseField(); err != nil {
			return err
		}

		// Extract field value
		field := p.extractField()
		p.record = append(p.record, field)

		// Check for end of record
		if p.state == StateNewline || p.state == StateEOF {
			break
		}
	}

	// Copy headers to dedicated slice
	p.headers = make([]string, len(p.record))
	copy(p.headers, p.record)
	p.record = p.record[:0] // Reset for data records

	return nil
}

// NextRecord advances to the next record in the CSV data
func (p *CSVParser) NextRecord() ([]string, error) {
	if p.pos >= len(p.data) {
		return nil, nil // EOF
	}

	p.record = p.record[:0] // Clear but preserve capacity
	p.fieldNum = 0

	for p.pos < len(p.data) {
		if err := p.parseField(); err != nil {
			return nil, err
		}

		// Extract field value
		field := p.extractField()
		p.record = append(p.record, field)
		p.fieldNum++

		// Check for end of record
		if p.state == StateNewline || p.pos >= len(p.data) {
			p.lineNum++
			break
		}
	}

	return p.record, nil
}

// parseField parses a single field from the current position
func (p *CSVParser) parseField() error {
	p.fieldStart = p.pos
	inQuotes := false

	for p.pos < len(p.data) {
		char := p.data[p.pos]

		switch p.state {
		case StateField:
			if char == byte(p.config.Quote) {
				if p.fieldStart == p.pos {
					// Quote at field start
					inQuotes = true
					p.fieldStart++
					p.state = StateQuote
				} else {
					// Quote in middle of field (invalid)
					return fmt.Errorf("unexpected quote at line %d, field %d", p.lineNum, p.fieldNum)
				}
			} else if char == byte(p.config.Delimiter) {
				// Field delimiter
				p.fieldEnd = p.pos
				p.state = StateDelimiter
				p.pos++
				return nil
			} else if char == '\n' || char == '\r' {
				// End of record
				p.fieldEnd = p.pos
				p.state = StateNewline
				p.pos++
				// Handle \r\n
				if char == '\r' && p.pos < len(p.data) && p.data[p.pos] == '\n' {
					p.pos++
				}
				return nil
			}
			// Regular character, continue

		case StateQuote:
			if char == byte(p.config.Escape) {
				// Escape character
				p.state = StateQuoteEscape
				p.pos++
			} else if char == byte(p.config.Quote) {
				// End of quoted field
				inQuotes = false
				p.fieldEnd = p.pos
				p.state = StateField
				p.pos++
				// Skip whitespace after quote if configured
				if p.config.TrimSpace {
					for p.pos < len(p.data) && (p.data[p.pos] == ' ' || p.data[p.pos] == '\t') {
						p.pos++
					}
				}
				// Check for delimiter or newline
				if p.pos < len(p.data) {
					nextChar := p.data[p.pos]
					if nextChar == byte(p.config.Delimiter) {
						p.state = StateDelimiter
						p.pos++
						return nil
					} else if nextChar == '\n' || nextChar == '\r' {
						p.state = StateNewline
						p.pos++
						if nextChar == '\r' && p.pos < len(p.data) && p.data[p.pos] == '\n' {
							p.pos++
						}
						return nil
					}
				}
				return nil
			}
			// Regular character in quoted field, continue

		case StateQuoteEscape:
			// Escaped character, treat as literal
			p.state = StateQuote
			p.pos++
		}

		// Check field size limit
		if p.pos-p.fieldStart > p.config.MaxFieldSize {
			return fmt.Errorf("field size exceeds maximum at line %d, field %d", p.lineNum, p.fieldNum)
		}

		p.pos++
	}

	// End of data
	if inQuotes {
		return fmt.Errorf("unterminated quoted field at line %d, field %d", p.lineNum, p.fieldNum)
	}

	p.fieldEnd = p.pos
	p.state = StateEOF
	return nil
}

// extractField extracts the current field value with zero allocations
func (p *CSVParser) extractField() string {
	if p.fieldStart >= p.fieldEnd {
		return ""
	}

	field := p.data[p.fieldStart:p.fieldEnd]

	// Trim whitespace if configured
	if p.config.TrimSpace {
		// Trim leading whitespace
		for len(field) > 0 && (field[0] == ' ' || field[0] == '\t') {
			field = field[1:]
		}
		// Trim trailing whitespace
		for len(field) > 0 && (field[len(field)-1] == ' ' || field[len(field)-1] == '\t') {
			field = field[:len(field)-1]
		}
	}

	// Convert to string without allocation using unsafe
	return *(*string)(unsafe.Pointer(&field))
}

// Headers returns the parsed headers
func (p *CSVParser) Headers() []string {
	return p.headers
}

// LineNum returns the current line number
func (p *CSVParser) LineNum() int {
	return p.lineNum
}

// FieldNum returns the current field number in the current record
func (p *CSVParser) FieldNum() int {
	return p.fieldNum
}

// Reset resets the parser state for reuse
func (p *CSVParser) Reset() {
	p.data = nil
	p.pos = 0
	p.state = StateField
	p.fieldStart = 0
	p.fieldEnd = 0
	p.lineNum = 1
	p.fieldNum = 0
	p.record = p.record[:0]
}

// SetConfig updates the parser configuration
func (p *CSVParser) SetConfig(config ParserConfig) {
	p.config = config
}

// Stats returns parsing statistics
func (p *CSVParser) Stats() ParserStats {
	return ParserStats{
		LinesParsed:  p.lineNum,
		FieldsParsed: p.fieldNum,
		BytesRead:    p.pos,
	}
}

// ParserStats contains statistics about the parsing process
type ParserStats struct {
	LinesParsed  int
	FieldsParsed int
	BytesRead    int
}

// IsValidDelimiter checks if a rune is a valid CSV delimiter
func IsValidDelimiter(delim rune) bool {
	return delim == ',' || delim == ';' || delim == '\t' || delim == '|'
}

// DetectDelimiter attempts to detect the delimiter in CSV data
func DetectDelimiter(data []byte, sampleSize int) rune {
	if sampleSize <= 0 || sampleSize > len(data) {
		sampleSize = len(data)
	}
	
	sample := data[:sampleSize]
	
	// Count potential delimiters in first few lines
	delimCounts := map[rune]int{
		',': 0,
		';': 0,
		'\t': 0,
		'|': 0,
	}
	
	lines := 0
	for i := 0; i < len(sample) && lines < 5; i++ {
		if sample[i] == '\n' || sample[i] == '\r' {
			lines++
		}
		
		for delim := range delimCounts {
			if sample[i] == byte(delim) {
				delimCounts[delim]++
			}
		}
	}
	
	// Find most frequent delimiter
	maxCount := 0
	bestDelim := ','
	for delim, count := range delimCounts {
		if count > maxCount {
			maxCount = count
			bestDelim = delim
		}
	}
	
	return bestDelim
}

// ValidateUTF8 checks if data is valid UTF-8
func ValidateUTF8(data []byte) bool {
	return utf8.Valid(data)
}