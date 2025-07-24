package internal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/9bany/db/internal/table"
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
