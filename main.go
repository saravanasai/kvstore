package main

import "fmt"

func main() {
	dbFile := "test.db"

	database, err := NewDatabase(dbFile)
	if err != nil {
		fmt.Println("Failed to open a database file")
	}

	value, err := database.Get("user_1")
	if err != nil {
		fmt.Println("Failed to get value:", err)
	} else {
		fmt.Println("Retrieved value:", value)
	}

}
