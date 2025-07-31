// filepath: /Users/eban.y/work/github/db_engine/internal/db_test.go
package internal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDatabase_Success(t *testing.T) {
	// Create a temporary directory to simulate the database
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "testdb")
	err := os.Mkdir(dbPath, 0777)
	assert.Nil(t, err)

	// Create a dummy table file
	tableFile := filepath.Join(dbPath, "table1.bin")
	_, err = os.Create(tableFile)
	assert.Nil(t, err)

	// Call NewDatabase
	db, err := NewDatabase("testdb")
	assert.Nil(t, err)
	assert.NotNil(t, db)
	assert.Equal(t, "testdb", db.name)
	assert.Equal(t, dbPath, db.path)
	assert.Contains(t, db.Tables, "table1")
}

func TestNewDatabase_DatabaseDoesNotExist(t *testing.T) {
	// Call NewDatabase with a non-existent database
	_, err := NewDatabase("nonexistentdb")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "database nonexistentdb does not exist")
}
