package main

import (
	"fmt"
	"os"
)

type Disk struct {
	FilePath string
	File     *os.File
}

func NewDisk(filepath string) (*Disk, error) {

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		fmt.Println("Database file path cannot be opened")
		return nil, err
	}

	return &Disk{
		FilePath: filepath,
		File:     file,
	}, nil
}

func (disk *Disk) Read(offset int, len int) ([]byte, error) {

	buf := make([]byte, len)

	_, err := disk.File.ReadAt(buf, int64(offset))
	return buf, err

}

func (disk *Disk) Write(offset int, data []byte) (int, error) {

	_, err := disk.File.WriteAt(data, int64(offset))

	if err != nil {
		return 0, err
	}

	return 1, err
}

func (disk *Disk) Close() error {
	return disk.File.Close()
}
