package util

import (
	"reflect"
	"sync"
	"testing"
)

type Item struct {
	ID int
}

// TestDBList_Add tests the Add method for both memory and disk storage.
func TestDBList_Add(t *testing.T) {
	tempDir := t.TempDir()
	list := NewDBList[Item](tempDir, 2) // maxInMemory set to 2

	items := []Item{{ID: 1}, {ID: 2}, {ID: 3}}
	for _, item := range items {
		if err := list.Add(item); err != nil {
			t.Errorf("Failed to add item: %v", err)
		}
	}

	if got := len(list.memoryData); got != 2 {
		t.Errorf("Expected 2 items in memory, got %d", got)
	}

	// Check if third item is on disk
	if _, err := list.filePathForIndex(2, false); err != nil {
		t.Errorf("Expected file for index 2 to exist on disk, but got error: %v", err)
	}
}

// TestDBList_Adds tests the Adds method for adding multiple items.
func TestDBList_Adds(t *testing.T) {
	tempDir := t.TempDir()
	list := NewDBList[Item](tempDir, 3)

	items := []Item{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: 5}}
	if err := list.Adds(items); err != nil {
		t.Errorf("Failed to add items: %v", err)
	}

	if got := list.Size(); got != 5 {
		t.Errorf("Expected size to be 5, got %d", got)
	}
}

// TestDBList_Get tests retrieving items from memory and disk.
func TestDBList_Get(t *testing.T) {
	tempDir := t.TempDir()
	list := NewDBList[Item](tempDir, 1)

	items := []Item{{ID: 1}, {ID: 2}}
	list.Adds(items)

	// Retrieve first item (should be from memory)
	if item, err := list.Get(0); err != nil || !reflect.DeepEqual(item, items[0]) {
		t.Errorf("Failed to retrieve item from memory: expected %v, got %v, err %v", items[0], item, err)
	}

	// Retrieve second item (should be from disk)
	if item, err := list.Get(1); err != nil || !reflect.DeepEqual(item, items[1]) {
		t.Errorf("Failed to retrieve item from disk: expected %v, got %v, err %v", items[1], item, err)
	}
}

// TestDBList_Sort tests the sorting functionality.
func TestDBList_Sort(t *testing.T) {
	list := NewDBList[Item]("", 10) // Assuming all in memory for simplicity

	items := []Item{{ID: 3}, {ID: 1}, {ID: 2}}
	list.Adds(items)
	list.Sort(func(a, b Item) bool { return a.ID < b.ID })

	if sortedItem, _ := list.Get(0); sortedItem.ID != 1 {
		t.Errorf("Expected first item to have ID 1, got %d", sortedItem.ID)
	}
}
func TestDBList_Concurrency(t *testing.T) {
	tempDir := t.TempDir()
	list := NewDBList[Item](tempDir, 10)

	var wg sync.WaitGroup
	addItem := func(id int) {
		defer wg.Done()
		if err := list.Add(Item{ID: id}); err != nil {
			t.Errorf("Failed to add item: %v", err)
		}
	}

	wg.Add(100)
	for i := 0; i < 100; i++ {
		go addItem(i)
	}
	wg.Wait()

	if got := list.Size(); got != 100 {
		t.Errorf("Expected size to be 100, got %d", got)
	}
}
