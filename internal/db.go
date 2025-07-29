package internal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/9bany/db/internal/platform/parser"
	parserio "github.com/9bany/db/internal/platform/parser/io"
	"github.com/9bany/db/internal/table"
	columnio "github.com/9bany/db/internal/table/column/io"
	"github.com/9bany/db/internal/table/wal"
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

	if err := os.MkdirAll(path(name), 0777); err != nil {
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

	recParser := parser.NewRecordParser(f, columnNames)

	t, err := table.NewTableWithColumns(f, columns, columnNames)
	if err != nil {
		return nil, NewCannotCreateTableError(err, name)
	}

	err = t.WriteColumnDefinitions(f)
	if err != nil {
		return nil, NewCannotCreateTableError(err, name)
	}
	db.Tables[name] = t

	err = t.SetRecordParser(recParser)
	if err != nil {
		return nil, NewCannotCreateTableError(err, name)
	}

	return t, nil

}

func (db *Database) readTables() (Tables, error) {
	entries, err := os.ReadDir(db.path)
	if err != nil {
		return nil, fmt.Errorf("Database.readTables: %w", err)
	}
	tables := make([]*table.Table, 0)
	for _, e := range entries {
		if _, err := e.Info(); err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		f, err := os.OpenFile(filepath.Join(db.path, e.Name()), os.O_RDWR, 0777)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		r := parserio.NewReader(f)
		columnDefReader := columnio.NewColumnDefinitionReader(r)

		tableName, err := table.GetTableName(f)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		writeAheadLog, err := wal.NewWal(db.path, tableName)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		t, err := table.NewTable(f, r, columnDefReader, writeAheadLog)
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}

		err = t.ReadColumnDefinitions()
		if err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}
		if err = t.SetRecordParser(parser.NewRecordParser(f, t.ColumnNames())); err != nil {
			return nil, fmt.Errorf("Database.readTables: %w", err)
		}
		tables = append(tables, t)
	}

	tablesMap := make(Tables)
	for _, v := range tables {
		tablesMap[v.Name] = v
	}

	return tablesMap, nil
}
