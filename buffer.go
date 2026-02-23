package main

import (
	"errors"
	"sync"
)

type SimpleBuffer struct {
	pages    map[uint64]*Page
	capacity int
	pm       *PageManager
	mu       sync.Mutex
}

func NewSimpleBuffer(capacity int, pm *PageManager) *SimpleBuffer {
	return &SimpleBuffer{
		pages:    make(map[uint64]*Page),
		capacity: capacity,
		pm:       pm,
	}
}

// Get page from buffer or load from disk
func (sb *SimpleBuffer) GetPage(pageId uint64) (*Page, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	if page, ok := sb.pages[pageId]; ok {
		return page, nil
	}

	// If buffer is full, return error (no eviction for simplicity)
	if len(sb.pages) >= sb.capacity {
		return nil, errors.New("buffer full, cannot load more pages")
	}

	page, err := sb.pm.LoadPage(pageId)
	if err != nil {
		return nil, err
	}
	sb.pages[pageId] = page
	return page, nil
}

// Insert record using buffer
func (sb *SimpleBuffer) InsertRecord(key, value string) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	recordSize := KeySize + ValueSize + len(key) + len(value)
	var pageWithSpace *Page
	var pageId uint64

	// Find page with space in buffer
	for pid, page := range sb.pages {
		if page.HasSpace(recordSize) {
			pageWithSpace = page
			pageId = pid
			break
		}
	}

	// If not found, load from disk or create new
	if pageWithSpace == nil {
		pageWithSpace, err := sb.pm.findPageWithSpace(recordSize)
		if err != nil {
			pageWithSpace = sb.pm.CreatePage()
			pageId = pageWithSpace.PageId
			// If buffer is full, return error
			if len(sb.pages) >= sb.capacity {
				return errors.New("buffer full, cannot add new page")
			}
			sb.pages[pageId] = pageWithSpace
		} else {
			pageId = pageWithSpace.PageId
			if len(sb.pages) >= sb.capacity {
				return errors.New("buffer full, cannot load more pages")
			}
			sb.pages[pageId] = pageWithSpace
		}
	}

	err := pageWithSpace.WriteRecord(key, value)
	if err != nil {
		return err
	}
	return nil // Don't write to disk immediately
}

// Flush all pages in buffer to disk
func (sb *SimpleBuffer) Flush() error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	for _, page := range sb.pages {
		err := sb.pm.writePageToDisk(page)
		if err != nil {
			return err
		}
	}
	return nil
}
