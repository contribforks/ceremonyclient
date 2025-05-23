package store

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"source.quilibrium.com/quilibrium/monorepo/node/config"
)

func TestRocksDBBasicOperations(t *testing.T) {
	testDir := t.TempDir()

	dbConfig := &config.DBConfig{
		Path: testDir,
	}

	db := NewRocksDB(dbConfig)
	defer db.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	err := db.Set(key, value)
	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	gotValue, closer, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	defer closer.Close()

	if !bytes.Equal(gotValue, value) {
		t.Fatalf("Expected %v, got %v", value, gotValue)
	}

	err = db.Delete(key)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	_, _, err = db.Get(key)
	if err == nil || !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("Expected ErrNotFound, got %v", err)
	}

	batch := db.NewBatch(false)

	batch.Set([]byte("batch-key-1"), []byte("batch-value-1"))
	batch.Set([]byte("batch-key-2"), []byte("batch-value-2"))

	err = batch.Commit()
	if err != nil {
		t.Fatalf("Failed to commit batch: %v", err)
	}

	gotValue1, closer1, err := db.Get([]byte("batch-key-1"))
	if err != nil {
		t.Fatalf("Failed to get batch value 1: %v", err)
	}
	defer closer1.Close()

	if !bytes.Equal(gotValue1, []byte("batch-value-1")) {
		t.Fatalf("Expected batch-value-1, got %v", gotValue1)
	}

	iter, err := db.NewIter([]byte("batch-key-"), []byte("batch-key-z"))
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	if count != 2 {
		t.Fatalf("Expected 2 keys, got %d", count)
	}
}

func TestRocksDBDeleteRange(t *testing.T) {
	testDir := t.TempDir()

	dbConfig := &config.DBConfig{
		Path: testDir,
	}

	db := NewRocksDB(dbConfig)
	defer db.Close()

	for i := 0; i < 10; i++ {
		key := []byte(string(rune('a' + i)))
		value := []byte(string(rune('A' + i)))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to set value: %v", err)
		}
	}

	err := db.DeleteRange([]byte("c"), []byte("h"))
	if err != nil {
		t.Fatalf("Failed to delete range: %v", err)
	}

	for i := 0; i < 10; i++ {
		key := []byte(string(rune('a' + i)))
		_, _, err := db.Get(key)

		if i >= 2 && i <= 6 { // 'c' to 'g'
			if err == nil || !errors.Is(err, pebble.ErrNotFound) {
				t.Fatalf("Expected key %s to be deleted", key)
			}
		} else {
			if err != nil {
				t.Fatalf("Expected key %s to exist, got error: %v", key, err)
			}
		}
	}
}

func TestRocksDBTransaction(t *testing.T) {
	testDir := t.TempDir()

	dbConfig := &config.DBConfig{
		Path: testDir,
	}

	db := NewRocksDB(dbConfig)
	defer db.Close()

	txn := db.NewBatch(false)

	txn.Set([]byte("txn-key-1"), []byte("txn-value-1"))
	txn.Set([]byte("txn-key-2"), []byte("txn-value-2"))

	// Verify data is not visible before commit
	_, _, err := db.Get([]byte("txn-key-1"))
	if err == nil || !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("Expected ErrNotFound before commit")
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify data is visible after commit
	val1, closer1, err := db.Get([]byte("txn-key-1"))
	if err != nil {
		t.Fatalf("Failed to get value after commit: %v", err)
	}
	defer closer1.Close()

	if !bytes.Equal(val1, []byte("txn-value-1")) {
		t.Fatalf("Expected txn-value-1, got %v", val1)
	}

	txn2 := db.NewBatch(false)
	txn2.Set([]byte("txn-key-3"), []byte("txn-value-3"))
	err = txn2.Abort()
	if err != nil {
		t.Fatalf("Failed to abort transaction: %v", err)
	}

	// Verify aborted transaction data is not visible
	_, _, err = db.Get([]byte("txn-key-3"))
	if err == nil || !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("Expected ErrNotFound after abort")
	}
}
