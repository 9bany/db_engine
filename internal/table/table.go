package table

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	parserio "github.com/9bany/db/internal/platform/parser/io"
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
