package table

import (
	"os"

	"github.com/9bany/db/internal/table/column"
)

type Tables []Table

type Columns map[string]*column.Column

type Table struct {
	Name        string
	file        *os.File
	columnNames []string
	columns     Columns
}
