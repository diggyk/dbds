package util

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// DBList manages a list of data elements, storing them in memory or on disk.
type DBList[T any] struct {
	memoryData    []T
	diskPath      string
	maxInMemory   int
	mutex         sync.RWMutex
	totalCount    int
	sortedIndexes []int
	isSorted      bool
}

// NewDBList creates a new DBList with a given path for disk storage and maximum in-memory length.
func NewDBList[T any](path string, maxInMemory int) *DBList[T] {
	return &DBList[T]{
		memoryData:    make([]T, 0, maxInMemory),
		diskPath:      path,
		maxInMemory:   maxInMemory,
		totalCount:    0,
		sortedIndexes: make([]int, 0, maxInMemory),
		isSorted:      true,
	}
}

// Add appends an item to the DBList, managing memory and disk storage automatically.
func (d *DBList[T]) Add(item T) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if len(d.memoryData) < d.maxInMemory {
		d.memoryData = append(d.memoryData, item)
	} else {
		filePath, err := d.filePathForIndex(d.totalCount, true)
		if err != nil {
			return err
		}

		data, err := json.Marshal(item)
		if err != nil {
			return err
		}

		file, err := os.Create(filePath)
		if err != nil {
			return err
		}
		defer file.Close()

		if _, err = file.Write(data); err != nil {
			return err
		}
	}

	d.sortedIndexes = append(d.sortedIndexes, d.totalCount)
	d.totalCount++
	d.isSorted = false

	return nil
}

// Adds appends multiple items to the DBList at once.
func (d *DBList[T]) Adds(items []T) error {
	for _, item := range items {
		if err := d.Add(item); err != nil {
			return err
		}
	}
	return nil
}

// Size returns the total number of elements in the DBList.
func (d *DBList[T]) Size() int {
	return d.totalCount
}

// Get retrieves an item by sorted index.
func (d *DBList[T]) Get(index int) (T, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if index >= len(d.sortedIndexes) {
		var zero T
		return zero, fmt.Errorf("index out of range")
	}

	index = d.sortedIndexes[index]
	return d.getFromStorage(index)
}

// getFromStorage gets the item at the given index, either from memory or disk.
func (d *DBList[T]) getFromStorage(index int) (T, error) {
	if index < len(d.memoryData) {
		return d.memoryData[index], nil
	} else {
		return d.retrieveFromDisk(index)
	}
}

func (d *DBList[T]) retrieveFromDisk(index int) (T, error) {
	var item T

	filePath, err := d.filePathForIndex(index, false)
	if err != nil {
		return item, err
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return item, fmt.Errorf("failed to read from disk: %w", err)
	}

	err = json.Unmarshal(data, &item)
	if err != nil {
		return item, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	return item, nil
}

// Iterator returns a channel that iterates over all elements, both in memory and on disk.
func (d *DBList[T]) Iterator(ctx context.Context) <-chan T {
	ch := make(chan T)

	go func() {
		defer close(ch)

		for i := 0; i < d.totalCount; i++ {
			if ctx.Err() != nil {
				// Exit if the context has been cancelled or timed out
				return
			}

			item, err := d.Get(i)
			if err != nil {
				slog.Error(fmt.Sprintf("DBList failed to load index %d", i))
				continue
			}

			select {
			case ch <- item:
			case <-ctx.Done():
				// Exit if context is cancelled
				return
			}
		}
	}()

	return ch
}

// Sort will rebuild the sorted index based on the provided compare function
func (d *DBList[T]) Sort(compare func(a, b T) bool) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.isSorted {
		return
	}

	sort.SliceStable(d.sortedIndexes, func(i, j int) bool {
		itemA, _ := d.getFromStorage(d.sortedIndexes[i])
		itemB, _ := d.getFromStorage(d.sortedIndexes[j])
		return compare(itemA, itemB)
	})

	d.isSorted = true
}

// filePathForIndex generates the file path for a given index and ensures the path exists if required.
func (d *DBList[T]) filePathForIndex(index int, create bool) (string, error) {
	filePath := filepath.Join(d.diskPath, fmt.Sprintf("%d.json", index))

	if create {
		// Ensure the directory exists
		dirPath := filepath.Dir(filePath)
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return "", fmt.Errorf("failed to create directory: %w", err)
		}
	}

	return filePath, nil
}
