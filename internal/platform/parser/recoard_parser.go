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
		FullSize: size + types.LenMeta,
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
		if err == io.EOF {
			return io.EOF
		}
		return fmt.Errorf("RecordParser.Parse: %w", err)
	}

	if t != types.TypeRecord && t != types.TypeDeletedRecord {
		return fmt.Errorf("RecordParser.Parse: expected TypeRecord, got %d", t)
	}

	if t == types.TypeDeletedRecord {
		if _, err := r.file.Seek(-1*types.LenByte, io.SeekCurrent); err != nil {
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}

		err = r.skipDeletedRecords()
		if err != nil {
			if err == io.EOF {
				return err
			}
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}
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

func (r *RecordParser) skipDeletedRecords() error {
	for {
		t, err := r.Reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return err
			}
			return fmt.Errorf("RecordParser.Parse: %w", err)
		}
		if t == types.TypeDeletedRecord {
			l, err := r.Reader.ReadUint32()
			if err != nil {
				return fmt.Errorf("RecordParser.Parse: %w", err)
			}
			if _, err = r.file.Seek(int64(l), io.SeekCurrent); err != nil {
				return fmt.Errorf("RecordParser.Parse: %w", err)
			}
		}
		if t == types.TypeRecord {
			return nil
		}
	}
}
