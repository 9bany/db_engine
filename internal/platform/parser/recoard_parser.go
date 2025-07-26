package parser

import (
	"errors"
	"fmt"
	"io"
	"os"

	parserio "github.com/9bany/db/internal/platform/parser/io"
	"github.com/9bany/db/internal/platform/types"
)

type RawRecord struct {
	Size     uint32
	FullSize uint32
	Values   map[string]interface{}
}

func NewRawRecord(size uint32, record map[string]interface{}) *RawRecord {
	return &RawRecord{
		Size:     size,
		FullSize: size,
		Values:   record,
	}
}

func NewRecordParser(file *os.File, columns []string) *RecordParser {
	return &RecordParser{
		file:    file,
		columns: columns,
		Value:   nil,
		Reader:  parserio.NewReader(file),
	}
}

type RecordParser struct {
	file    *os.File
	columns []string
	Value   *RawRecord
	Reader  *parserio.Reader
}

func (r *RecordParser) Parse() error {
	read := parserio.NewReader(r.file)
	t, err := read.ReadByte()
	if err != nil {
		return fmt.Errorf("RecordParser.Parse: %w", err)
	}
	if t != types.TypeRecord {
		return fmt.Errorf("RecordParser.Parse: expected TypeRecord, got %d", t)
	}

	record := make(map[string]interface{})

	lenRecord, err := read.ReadUint32()

	if err != nil {
		return fmt.Errorf("RecordParser.Parse: %w", err)
	}
	for i := 0; i < len(r.columns); i++ {
		tlvParser := NewTLVParser(read)
		value, err := tlvParser.Parse()
		if errors.Is(err, io.EOF) {
			r.Value = NewRawRecord(lenRecord, record)
		}
		if err != nil {
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}
		record[r.columns[i]] = value
	}

	r.Value = NewRawRecord(
		lenRecord,
		record,
	)
	return nil
}
