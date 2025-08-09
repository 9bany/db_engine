package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/9bany/db/internal"
	"github.com/9bany/db/internal/platform/types"
	"github.com/9bany/db/internal/table"
	"github.com/9bany/db/internal/table/column"
	"github.com/spf13/cobra"
)

var (
	Database string
)

func dropDb(dbName string) error {
	return internal.DropDatabase(dbName)
}

func createDb(dbName string) error {
	_, err := internal.CreateDatabase(dbName)
	if err != nil {
		return err
	}
	return nil
}

func fakeTbUser(dbName string) error {
	db, err := internal.NewDatabase(dbName)
	if err != nil {
		return err
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
		return err
	}
	return nil
}

func fakeInsertTbUser(dbName string) error {
	db, err := internal.NewDatabase(dbName)
	if err != nil {
		return err
	}
	_, err = db.Tables["tb_user"].Insert(map[string]interface{}{
		"id": int32(1),
		"username": "bany",
	})
	if err != nil {
		return err
	}
	return nil
}

func init() {

	createDbCmd.PersistentFlags().StringVarP(&Database, "database_name", "d", "", "Database name")
	databaseCmd.AddCommand(createDbCmd)

	dropDbCmd.PersistentFlags().StringVarP(&Database, "database_name", "d", "", "Database name")
	databaseCmd.AddCommand(dropDbCmd)

	fakeTbCmd.PersistentFlags().StringVarP(&Database, "database_name", "d", "", "Database name")
	databaseCmd.AddCommand(fakeTbCmd)

	fakeInsertTbCmd.PersistentFlags().StringVarP(&Database, "database_name", "d", "", "Database name")
	databaseCmd.AddCommand(fakeInsertTbCmd)

	rootCmd.AddCommand(databaseCmd)
}

var databaseCmd = &cobra.Command{
	Use:   "database",
	Short: "Database commands",
	Long:  ``,
}

var createDbCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a database",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		if len(Database) == 0 {
			os.Exit(0)
		}
		if err := createDb(Database); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Created database: %s\n", Database)
	},
}

var dropDbCmd = &cobra.Command{
	Use:   "drop",
	Short: "Drop a database",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		if len(Database) == 0 {
			os.Exit(0)
		}
		err := dropDb(Database)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Droped database: %s\n", Database)
	},
}

var fakeTbCmd = &cobra.Command{
	Use:   "fake",
	Short: "Fake a table in database",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		if len(Database) == 0 {
			os.Exit(0)
		}
		if err := fakeTbUser(Database); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Created tb_user in database: %s\n", Database)
	},
}

var fakeInsertTbCmd = &cobra.Command{
	Use:   "fake-insert",
	Short: "Fake a table in database",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		if len(Database) == 0 {
			os.Exit(0)
		}
		if err := fakeInsertTbUser(Database); err != nil {
			log.Fatal(err)
		}
		fmt.Println("Inserted into tb_user")
	},
}
