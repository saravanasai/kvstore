package main

import (
	"fmt"
)

type Database struct {
	pageManager *PageManager
	buffer      *SimpleBuffer
	disk        *Disk
}

func NewDatabase(filePath string) (*Database, error) {
	disk, err := NewDisk(filePath)
	if err != nil {
		fmt.Println("Error:" + err.Error())
	}

	pageManager := NewPageManager(disk)
	pageManager.LoadMetaPage()
	buffer := NewSimpleBuffer(4, pageManager) // Buffer size 4 for example

	return &Database{
		pageManager: pageManager,
		buffer:      buffer,
		disk:        disk,
	}, nil
}

func (db *Database) Put(key string, value string) error {
	return db.buffer.InsertRecord(key, value)
}

func (db *Database) Get(key string) (string, error) {
	// Search in buffer first
	db.buffer.mu.Lock()
	defer db.buffer.mu.Unlock()
	for _, page := range db.buffer.pages {
		value, found := page.ReadRecord(key)
		if found {
			return value, nil
		}
	}
	// If not found in buffer, search on disk
	return db.pageManager.FindRecord(key)
}
