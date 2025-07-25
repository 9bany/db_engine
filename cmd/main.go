package main

import (
	"log"

	"github.com/9bany/db/internal"
)

func main() {
	// db, err := internal.CreateDatabase("my_database")
	// if err != nil {
	// 	log.Fatalf("Error creating database: %v", err)
	// }

	// idColumn := column.NewColumn("id", types.TypeInt32, column.ColumnOptions{Nullable: true})
	// usernameColum := column.NewColumn("username", types.TypeString, column.ColumnOptions{Nullable: true})

	// _, err = db.CreateTable("tb_user", []string{
	// 	"id", "username",
	// }, table.Columns{
	// 	"id":       idColumn,
	// 	"username": usernameColum,
	// })
	// if err != nil {
	// 	log.Fatalf("Error creating table: %v", err)
	// }
	// log.Println("Table 'tb_user' created successfully")

	db1, err := internal.NewDatabase("my_database")
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	t1 := db1.Tables["tb_user"]

	log.Println(t1)
}
