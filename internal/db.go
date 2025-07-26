package internal

import (
	"fmt"
	"os"
	"path/filepath"

	parserio "github.com/9bany/db/internal/platform/parser/io"
	"github.com/9bany/db/internal/table"
	columnio "github.com/9bany/db/internal/table/column/io"
)

const (
	BaseDir = "./data"
)

func path(name string) string {
	return filepath.Join(BaseDir, name)
}

func exists(name string) bool {
	_, err := os.Stat(path(name))
	return !os.IsNotExist(err)
}

type Tables map[string]*table.Table

type Database struct {
	name   string
	path   string
	Tables Tables
}

func CreateDatabase(name string) (*Database, error) {
	if exists(name) {
		return nil, NewDatabaseAlreadyExistsError(name)
	}
	if err := os.MkdirAll(path(name), 0644); err != nil {
		return nil, fmt.Errorf("CreateDatabase: %w", err)
	}
	return &Database{
		name:   name,
		path:   path(name),
		Tables: make(Tables),
	}, nil
}

func NewDatabase(name string) (*Database, error) {
	if !exists(name) {
		return nil, NewDatabaseDoesNotExistError(name)
	}
	db := &Database{
		name: name,
		path: path(name),
	}

	table, err := db.readTables()
	if err != nil {
		return nil, fmt.Errorf("NewDatabase: %w", err)
	}
	db.Tables = table
	return db, nil
}

func (db *Database) CreateTable(name string,
	columnNames []string,
	columns table.Columns) (*table.Table, error) {

	path := filepath.Join(path(db.name), name+table.FileExtension)

	if _, err := os.Open(path); err == nil {
		return nil, NewTableAlreadyExistsError(name)
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, NewCannotCreateTableError(err, name)
	}

	t, err := table.NewTableWithColumns(f, columns, columnNames)
	if err != nil {
		return nil, NewCannotCreateTableError(err, name)
	}

	err = t.WriteColumnDefinitions(f)
	if err != nil {
		return nil, NewCannotCreateTableError(err, name)
	}
	db.Tables[name] = t
	return t, nil

}

func (db *Database) readTables() (Tables, error) {
	entries, err := os.ReadDir(db.path)
	if err != nil {
		return nil, fmt.Errorf("readTables: %w", err)
	}
	tables := make([]*table.Table, 0)
	for _, e := range entries {
		if _, err := e.Info(); err != nil {
			return nil, fmt.Errorf("readTables: %w", err)
		}

		f, err := os.OpenFile(filepath.Join(db.path, e.Name()), os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			return nil, fmt.Errorf("readTables: %w", err)
		}

		r := parserio.NewReader(f)
		columnDefReader := columnio.NewColumnDefinitionReader(r)

		t, err := table.NewTable(f, r, columnDefReader)
		if err != nil {
			return nil, fmt.Errorf("readTables: %w", err)
		}

		err = t.ReadColumnDefinitions()
		if err != nil {
			return nil, fmt.Errorf("readTables: %w", err)
		}
		tables = append(tables, t)
	}

	tablesMap := make(Tables)
	for _, v := range tables {
		tablesMap[v.Name] = v
	}

	return tablesMap, nil
}
