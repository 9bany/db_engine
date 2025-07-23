package table

import (
	"io"
	"os"

	"github.com/9bany/db/internal/table/column"
)

var FileExtension string = ".bin"

type Tables []Table

type Columns map[string]*column.Column

type Table struct {
	Name        string
	file        *os.File
	columnNames []string
	columns     Columns
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

type ColumnDefinitionWriter struct {
	w io.Writer
}

