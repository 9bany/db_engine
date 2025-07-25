package io

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	parserio "github.com/9bany/db/internal/platform/parser/io"
	"github.com/9bany/db/internal/platform/types"
)

type ColumnDefinitionReader struct {
	reader *parserio.Reader
}

func NewColumnDefinitionReader(reader *parserio.Reader) *ColumnDefinitionReader {
	return &ColumnDefinitionReader{
		reader: reader,
	}
}

func (r *ColumnDefinitionReader) Read(b []byte) (int, error) {
	buf := bytes.Buffer{}
	dataType, err := r.reader.ReadByte()
	if err != nil {
		if err == io.EOF {
			return buf.Len(), io.EOF
		}
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: data type: %w", err)
	}

	if dataType != types.TypeColumnDefinition {
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: expected data type %d, got %d", types.TypeColumnDefinition, dataType)
	}
	buf.WriteByte(dataType)

	dataLength, err := r.reader.ReadUint32()
	if err != nil {
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: data length: %w", err)
	}

	if err := binary.Write(&buf, binary.LittleEndian, dataLength); err != nil {
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: write data length: %w", err)
	}

	col := make([]byte, dataLength)
	if _, err := r.reader.Read(col); err != nil {
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: read column definition: %w", err)
	}

	buf.Write(col)
	copy(b, buf.Bytes())

	return buf.Len(), nil

}
