package store

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"source.quilibrium.com/quilibrium/monorepo/node/config"
)

const (
	smallValueSize  = 64
	mediumValueSize = 1024
	largeValueSize  = 65536
	numKeys         = 10000
)

func setupTestDBs(b *testing.B) (*RocksDB, *PebbleDB, func()) {
	rocksDir := b.TempDir()
	pebbleDir := b.TempDir()

	rocksConfig := &config.DBConfig{Path: rocksDir}
	pebbleConfig := &config.DBConfig{Path: pebbleDir}

	rocksDB := NewRocksDB(rocksConfig)
	pebbleDB := NewPebbleDB(pebbleConfig)

	cleanup := func() {
		rocksDB.Close()
		pebbleDB.Close()
	}

	return rocksDB, pebbleDB, cleanup
}

var gen = rand.NewChaCha8([32]byte{})

func generateRandomBytes(size int) []byte {
	data := make([]byte, size)
	gen.Read(data)
	return data
}

func populateDB(db KVDB, count int, valueSize int) {
	batch := db.NewBatch(false)
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := generateRandomBytes(valueSize)
		batch.Set(key, value)
	}
	batch.Commit()
}

func BenchmarkSequentialSet(b *testing.B) {
	for _, size := range []int{smallValueSize, mediumValueSize, largeValueSize} {
		b.Run(fmt.Sprintf("RocksDB-%dB", size), func(b *testing.B) {
			rocksDB, _, cleanup := setupTestDBs(b)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("key-%d", i))
				value := generateRandomBytes(size)
				rocksDB.Set(key, value)
			}
		})

		b.Run(fmt.Sprintf("PebbleDB-%dB", size), func(b *testing.B) {
			_, pebbleDB, cleanup := setupTestDBs(b)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("key-%d", i))
				value := generateRandomBytes(size)
				pebbleDB.Set(key, value)
			}
		})
	}
}

func BenchmarkSequentialGet(b *testing.B) {
	for _, size := range []int{smallValueSize, mediumValueSize, largeValueSize} {
		rocksDB, _, cleanup1 := setupTestDBs(b)
		defer cleanup1()

		populateDB(rocksDB, numKeys, size)

		b.Run(fmt.Sprintf("RocksDB-%dB", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("key-%d", i%numKeys))
				val, closer, _ := rocksDB.Get(key)
				if closer != nil {
					closer.Close()
				}
				_ = val
			}
		})

		_, pebbleDB, cleanup2 := setupTestDBs(b)
		defer cleanup2()

		populateDB(pebbleDB, numKeys, size)

		b.Run(fmt.Sprintf("PebbleDB-%dB", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("key-%d", i%numKeys))
				val, closer, _ := pebbleDB.Get(key)
				if closer != nil {
					closer.Close()
				}
				_ = val
			}
		})
	}
}

func BenchmarkSequentialDelete(b *testing.B) {
	b.Run("RocksDB", func(b *testing.B) {
		rocksDB, _, cleanup := setupTestDBs(b)
		defer cleanup()

		populateDB(rocksDB, numKeys, smallValueSize)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%d", i))
			rocksDB.Delete(key)
		}
	})

	b.Run("PebbleDB", func(b *testing.B) {
		_, pebbleDB, cleanup := setupTestDBs(b)
		defer cleanup()

		populateDB(pebbleDB, numKeys, smallValueSize)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%d", i))
			pebbleDB.Delete(key)
		}
	})
}

func BenchmarkBatchWrites(b *testing.B) {
	batchSizes := []int{10, 100, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("RocksDB-BatchSize%d", batchSize), func(b *testing.B) {
			rocksDB, _, cleanup := setupTestDBs(b)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := rocksDB.NewBatch(false)
				for j := 0; j < batchSize; j++ {
					key := []byte(fmt.Sprintf("key-%d-%d", i, j))
					value := generateRandomBytes(smallValueSize)
					batch.Set(key, value)
				}
				batch.Commit()
			}
		})

		b.Run(fmt.Sprintf("PebbleDB-BatchSize%d", batchSize), func(b *testing.B) {
			_, pebbleDB, cleanup := setupTestDBs(b)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := pebbleDB.NewBatch(false)
				for j := 0; j < batchSize; j++ {
					key := []byte(fmt.Sprintf("key-%d-%d", i, j))
					value := generateRandomBytes(smallValueSize)
					batch.Set(key, value)
				}
				batch.Commit()
			}
		})
	}
}

func BenchmarkParallelSet(b *testing.B) {
	for _, goroutines := range []int{2, 4, 8, 16} {
		b.Run(fmt.Sprintf("RocksDB-%dGoroutines", goroutines), func(b *testing.B) {
			rocksDB, _, cleanup := setupTestDBs(b)
			defer cleanup()

			b.ResetTimer()
			b.SetParallelism(goroutines)
			b.RunParallel(func(pb *testing.PB) {
				counter := 0
				for pb.Next() {
					key := []byte(fmt.Sprintf("key-%d", counter))
					value := generateRandomBytes(smallValueSize)
					rocksDB.Set(key, value)
					counter++
				}
			})
		})

		b.Run(fmt.Sprintf("PebbleDB-%dGoroutines", goroutines), func(b *testing.B) {
			_, pebbleDB, cleanup := setupTestDBs(b)
			defer cleanup()

			b.ResetTimer()
			b.SetParallelism(goroutines)
			b.RunParallel(func(pb *testing.PB) {
				counter := 0
				for pb.Next() {
					key := []byte(fmt.Sprintf("key-%d", counter))
					value := generateRandomBytes(smallValueSize)
					pebbleDB.Set(key, value)
					counter++
				}
			})
		})
	}
}

func BenchmarkParallelGet(b *testing.B) {
	for _, goroutines := range []int{2, 4, 8, 16} {
		rocksDB, _, cleanup1 := setupTestDBs(b)
		defer cleanup1()

		populateDB(rocksDB, numKeys, smallValueSize)

		b.Run(fmt.Sprintf("RocksDB-%dGoroutines", goroutines), func(b *testing.B) {
			b.SetParallelism(goroutines)
			b.RunParallel(func(pb *testing.PB) {
				counter := 0
				for pb.Next() {
					key := []byte(fmt.Sprintf("key-%d", counter%numKeys))
					val, closer, _ := rocksDB.Get(key)
					if closer != nil {
						closer.Close()
					}
					_ = val
					counter++
				}
			})
		})

		_, pebbleDB, cleanup2 := setupTestDBs(b)
		defer cleanup2()

		populateDB(pebbleDB, numKeys, smallValueSize)

		b.Run(fmt.Sprintf("PebbleDB-%dGoroutines", goroutines), func(b *testing.B) {
			b.SetParallelism(goroutines)
			b.RunParallel(func(pb *testing.PB) {
				counter := 0
				for pb.Next() {
					key := []byte(fmt.Sprintf("key-%d", counter%numKeys))
					val, closer, _ := pebbleDB.Get(key)
					if closer != nil {
						closer.Close()
					}
					_ = val
					counter++
				}
			})
		})
	}
}

func BenchmarkMixedReadWrite(b *testing.B) {
	readRatios := []float64{0.25, 0.5, 0.75}

	for _, readRatio := range readRatios {
		rocksDB, _, cleanup1 := setupTestDBs(b)
		defer cleanup1()

		populateDB(rocksDB, numKeys, smallValueSize)

		b.Run(fmt.Sprintf("RocksDB-ReadRatio%.2f-%d", readRatio, numKeys), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				counter := 0
				for pb.Next() {
					if rand.Float64() < readRatio {
						key := []byte(fmt.Sprintf("key-%d", rand.IntN(numKeys)))
						val, closer, _ := rocksDB.Get(key)
						if closer != nil {
							closer.Close()
						}
						_ = val
					} else {
						key := []byte(fmt.Sprintf("key-%d", counter%numKeys))
						value := generateRandomBytes(smallValueSize)
						rocksDB.Set(key, value)
					}
					counter++
				}
			})
		})

		_, pebbleDB, cleanup2 := setupTestDBs(b)
		defer cleanup2()

		populateDB(pebbleDB, numKeys, smallValueSize)

		b.Run(fmt.Sprintf("PebbleDB-ReadRatio%.2f-%d", readRatio, numKeys), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				counter := 0
				for pb.Next() {
					if rand.Float64() < readRatio {
						key := []byte(fmt.Sprintf("key-%d", rand.IntN(numKeys)))
						val, closer, _ := pebbleDB.Get(key)
						if closer != nil {
							closer.Close()
						}
						_ = val
					} else {
						key := []byte(fmt.Sprintf("key-%d", counter%numKeys))
						value := generateRandomBytes(smallValueSize)
						pebbleDB.Set(key, value)
					}
					counter++
				}
			})
		})
	}
}

func BenchmarkIteration(b *testing.B) {
	keyCounts := []int{100, 1000, 10000}

	for _, keyCount := range keyCounts {
		rocksDB, _, cleanup1 := setupTestDBs(b)
		defer cleanup1()

		rocksBatch := rocksDB.NewBatch(false)
		for i := 0; i < keyCount; i++ {
			key := []byte(fmt.Sprintf("key-%06d", i))
			value := generateRandomBytes(smallValueSize)
			rocksBatch.Set(key, value)
		}
		rocksBatch.Commit()

		b.Run(fmt.Sprintf("RocksDB-%dKeys", keyCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				iter, _ := rocksDB.NewIter([]byte("key-"), []byte("key-z"))
				count := 0
				for iter.First(); iter.Valid(); iter.Next() {
					_ = iter.Key()
					_ = iter.Value()
					count++
				}
				iter.Close()
			}
		})

		_, pebbleDB, cleanup2 := setupTestDBs(b)
		defer cleanup2()

		pebbleBatch := pebbleDB.NewBatch(false)
		for i := 0; i < keyCount; i++ {
			key := []byte(fmt.Sprintf("key-%06d", i))
			value := generateRandomBytes(smallValueSize)
			pebbleBatch.Set(key, value)
		}
		pebbleBatch.Commit()

		b.Run(fmt.Sprintf("PebbleDB-%dKeys", keyCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				iter, _ := pebbleDB.NewIter([]byte("key-"), []byte("key-z"))
				count := 0
				for iter.First(); iter.Valid(); iter.Next() {
					_ = iter.Key()
					_ = iter.Value()
					count++
				}
				iter.Close()
			}
		})
	}
}
