package table

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/9bany/db/internal/platform/parser/encoding"
	parserio "github.com/9bany/db/internal/platform/parser/io"
	"github.com/9bany/db/internal/platform/types"
	"github.com/9bany/db/internal/table/column"
	columnio "github.com/9bany/db/internal/table/column/io"
)

var FileExtension string = ".bin"

type Tables []Table

type Columns map[string]*column.Column

func fileNameWithoutExt(f *os.File) string {
	return strings.TrimSuffix(filepath.Base(f.Name()), filepath.Ext(f.Name()))
}

type Table struct {
	Name             string
	file             *os.File
	columnNames      []string
	columns          Columns
	columnsDefReader *columnio.ColumnDefinitionReader
}

func NewTable(f *os.File, r *parserio.Reader, columnDefReader *columnio.ColumnDefinitionReader) (*Table, error) {
	return &Table{
		Name:             fileNameWithoutExt(f),
		file:             f,
		columnsDefReader: columnDefReader,
		columns:          make(Columns),
		columnNames:      make([]string, 0),
	}, nil
}

func NewTableWithColumns(f *os.File, columns Columns, columnNames []string) (*Table, error) {
	if len(columns) == 0 {
		return nil, NewCannotCreateTableError(nil, "table must have at least one column")
	}

	for _, col := range columns {
		if len(col.Name) == 0 {
			return nil, NewCannotCreateTableError(nil, "column name cannot be empty")
		}
	}

	return &Table{
		Name:        f.Name(),
		file:        f,
		columnNames: columnNames,
		columns:     columns,
	}, nil
}

func (t *Table) validateColumns(record map[string]interface{}) error {
	for colName, col := range t.columns {
		if _, ok := record[colName]; !ok {
			return fmt.Errorf("Table.validateColumns: column %s is missing in the record", colName)
		}
		if err := col.ValidateValue(record[colName]); err != nil {
			return fmt.Errorf("Table.validateColumns: %w", err)
		}
	}
	return nil
}

func (t *Table) WriteColumnDefinitions(w io.Writer) error {
	for _, col := range t.columnNames {
		b, err := t.columns[col].MarshalBinary()
		if err != nil {
			return fmt.Errorf("Table.WriteColumnDefinitions: %w", err)
		}
		writer := columnio.NewColumnDefinitionWriter(w)
		if n, err := writer.Write(b); n < len(b) || err != nil {
			return fmt.Errorf("Table.WriteColumnDefinitions: %w", err)
		}
	}
	return nil
}

func (t *Table) ReadColumnDefinitions() error {
	if _, err := t.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("Table.ReadColumnDefinitions: %w", err)
	}
	for {
		buf := make([]byte, 0, 1024)
		n, err := t.columnsDefReader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("Table.ReadColumnDefinitions: %w", err)
		}
		col := column.Column{}
		err = col.UnmarshalBinary(buf[:n])
		if err != nil {
			return fmt.Errorf("Table.ReadColumnDefinitions: %w", err)
		}
		colName := string(col.Name[:])
		t.columns[colName] = &col
		t.columnNames = append(t.columnNames, colName)
	}
	return nil
}

func (t *Table) Insert(record map[string]interface{}) (int, error) {
	if _, err := t.file.Seek(0, io.SeekEnd); err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	if err := t.validateColumns(record); err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}

	var sizeOfRecord uint32
	for _, colName := range t.columnNames {
		value, ok := record[colName]
		if !ok {
			return 0, fmt.Errorf("Table.Insert: missing value for column %s", colName)
		}

		tlvMarshaler := encoding.NewTLVMarshaler(value)
		length, err := tlvMarshaler.TLVLength()
		if err != nil {
			return 0, fmt.Errorf("Table.Insert: %w", err)
		}
		sizeOfRecord += length
	}

	buf := bytes.Buffer{}
	byteMarshaler := encoding.NewValueMarshaler(types.TypeRecord)
	typeBuf, err := byteMarshaler.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	buf.Write(typeBuf)

	sizeMarshaler := encoding.NewValueMarshaler(sizeOfRecord)
	sizeBuf, err := sizeMarshaler.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	buf.Write(sizeBuf)

	for _, colName := range t.columnNames {
		value, ok := record[colName]
		if !ok {
			return 0, fmt.Errorf("Table.Insert: missing value for column %s", colName)
		}

		tlvMarshaler := encoding.NewTLVMarshaler(value)
		valueBuf, err := tlvMarshaler.MarshalBinary()
		if err != nil {
			return 0, fmt.Errorf("Table.Insert: %w", err)
		}
		buf.Write(valueBuf)
	}

	n, err := t.file.Write(buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("Table.Insert: %w", err)
	}
	if n != buf.Len() {
		return 0, fmt.Errorf("Table.Insert: expected to write %d bytes, but wrote %d", buf.Len(), n)
	}
	return n, nil
}
