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

func readDb(db string) {
	db1, err := internal.NewDatabase(db)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	t1 := db1.Tables["tb_user"]

	log.Println(t1)
}

func main() {
	dbName := "my_db"
	// createDb(dbName)
	db1, err := internal.NewDatabase(dbName)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}

	result, err := db1.Tables["tb_user"].Select(map[string]interface{}{
		"username": "chaniuxinhgai",
	})

	// result, err := db1.Tables["tb_user"].Update(map[string]interface{}{
	// 	"username": "chanchan",
	// }, map[string]interface{}{
	// 	"username": "chaniuxinhgai",
	// })
	if err != nil {
		log.Fatal(err)
	}
	log.Println(result)
}
