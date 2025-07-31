package main

import (
	"log"

	"github.com/9bany/db/internal"
	"github.com/9bany/db/internal/platform/types"
	"github.com/9bany/db/internal/table"
	"github.com/9bany/db/internal/table/column"
)

func createDb(dbName string) {
	db, err := internal.CreateDatabase(dbName)
	if err != nil {
		log.Fatalf("Error creating database: %v", err)
	}

	idColumn := column.NewColumn("id", types.TypeInt32, column.ColumnOptions{Nullable: true})
	usernameColum := column.NewColumn("username", types.TypeString, column.ColumnOptions{Nullable: true})

	_, err = db.CreateTable("tb_user", []string{
		"id", "username",
	}, table.Columns{
		"id":       idColumn,
		"username": usernameColum,
	})
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}
	log.Println("Table 'tb_user' created successfully")
}

func readDb(db string) *internal.Database {
	db1, err := internal.NewDatabase(db)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	return db1
}

func main() {
	dbName := "my_db"
	// createDb(dbName)
	db1 := readDb(dbName)

	result, err := db1.Tables["tb_user"].Select(map[string]interface{}{
		"id": int32(2),
	})
	if err != nil {
		log.Fatal("create:", err)
	}
	log.Println(result)
}
