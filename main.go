package main

import "fmt"

func main() {
	dbFile := "test.db"

	db, err := NewDatabase(dbFile)
	if err != nil {
		fmt.Println("Failed to create database:", err)
		return
	}

	err = db.Put("user_1", "hello")
	if err != nil {
		fmt.Println("Failed to insert user_1:", err)
	}
	err = db.Put("user_2", "world")
	if err != nil {
		fmt.Println("Failed to insert user_2:", err)
	}

	// Example: Retrieve value for user_1
	value, err := db.Get("user_1")
	if err != nil {
		fmt.Println("Failed to get user_1:", err)
	} else {
		fmt.Println("Retrieved value for user_1:", value)
	}
}
