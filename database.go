package main

import (
	"fmt"
)

type Database struct {
	pageManager *PageManager
	disk        *Disk
}

func NewDatabase(filePath string) (*Database, error) {
	disk, err := NewDisk(filePath)
	if err != nil {
		fmt.Println("Error:" + err.Error())
	}

	pageManager := NewPageManager(disk)
	pageManager.LoadMetaPage()

	return &Database{
		pageManager: pageManager,
		disk:        disk,
	}, nil
}

func (db *Database) Put(key string, value string) error {
	return db.pageManager.InsertRecord(key, value)
}

func (db *Database) Get(key string) (string, error) {
	return db.pageManager.FindRecord(key)
}
