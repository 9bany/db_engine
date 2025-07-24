package table

import (
	"fmt"
	"io"
	"os"

	"github.com/9bany/db/internal/table/column"
	columnio "github.com/9bany/db/internal/table/column/io"
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

func (t *Table) WriteColumnDefinitions(w io.Writer) error {
	for _, col := range t.columnNames {
		b, err := t.columns[col].MarshalBinary()
		if err != nil {
			return fmt.Errorf("Table.WriteColumnDefinitions: %w", err)
		}
		fmt.Println(b)
		writer := columnio.NewColumnDefinitionWriter(w)
		if n, err := writer.Write(b); n < len(b) || err != nil {
			return fmt.Errorf("Table.WriteColumnDefinitions: %w", err)
		}
	}
	return nil
}
