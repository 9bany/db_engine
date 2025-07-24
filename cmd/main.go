package main

import (
	"log"

	"github.com/9bany/db/internal"
	"github.com/9bany/db/internal/platform/types"
	"github.com/9bany/db/internal/table"
	"github.com/9bany/db/internal/table/column"
)

func main() {
	db, err := internal.CreateDatabase("my_database")
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
	// You can now use db.Tables["tb_user"] to access the created table
	// For example, you can write column definitions to the table file:
	_, ok := db.Tables["tb_user"]
	if !ok {
		log.Fatalf("Table 'tb_user' not found in database")
	}
	log.Println("Table 'tb_user' exists in database")
}
