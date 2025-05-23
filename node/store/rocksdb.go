package store

import (
	"bytes"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/linxGnu/grocksdb"
	"github.com/pkg/errors"
	"source.quilibrium.com/quilibrium/monorepo/node/config"
)

type RocksDB struct {
	db *grocksdb.DB
	ro *grocksdb.ReadOptions
	wo *grocksdb.WriteOptions
}

type RocksDBCloser struct {
	slice *grocksdb.Slice
}

func (c *RocksDBCloser) Close() error {
	if c.slice != nil {
		c.slice.Free()
	}
	return nil
}

func NewRocksDB(config *config.DBConfig) *RocksDB {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	db, err := grocksdb.OpenDb(opts, config.Path)
	if err != nil {
		panic(err)
	}

	ro := grocksdb.NewDefaultReadOptions()
	wo := grocksdb.NewDefaultWriteOptions()
	wo.SetSync(true)

	return &RocksDB{
		db: db,
		ro: ro,
		wo: wo,
	}
}

func (r *RocksDB) Get(key []byte) ([]byte, io.Closer, error) {
	slice, err := r.db.Get(r.ro, key)
	if err != nil {
		return nil, nil, errors.Wrap(err, "rocksdb get")
	}

	if slice.Data() == nil {
		slice.Free()
		return nil, nil, pebble.ErrNotFound
	}

	// Copy data since slice will be freed when closer is closed
	data := make([]byte, len(slice.Data()))
	copy(data, slice.Data())

	return data, &RocksDBCloser{slice: slice}, nil
}

func (r *RocksDB) Set(key, value []byte) error {
	return r.db.Put(r.wo, key, value)
}

func (r *RocksDB) Delete(key []byte) error {
	return r.db.Delete(r.wo, key)
}

func (r *RocksDB) NewBatch(indexed bool) Transaction {
	batch := grocksdb.NewWriteBatch()
	return &RocksDBTransaction{
		db:    r.db,
		batch: batch,
		ro:    r.ro,
		wo:    r.wo,
	}
}

func (r *RocksDB) NewIter(lowerBound []byte, upperBound []byte) (Iterator, error) {
	iterOpts := grocksdb.NewDefaultReadOptions()
	if lowerBound != nil {
		iterOpts.SetIterateLowerBound(lowerBound)
	}
	if upperBound != nil {
		iterOpts.SetIterateUpperBound(upperBound)
	}

	iter := r.db.NewIterator(iterOpts)
	return &RocksDBIterator{
		iter:     iter,
		iterOpts: iterOpts,
	}, nil
}

func (r *RocksDB) Compact(start, end []byte, parallelize bool) error {
	r.db.CompactRange(grocksdb.Range{
		Start: start,
		Limit: end,
	})
	return nil
}

func (r *RocksDB) CompactAll() error {
	r.db.CompactRange(grocksdb.Range{})
	return nil
}

func (r *RocksDB) Close() error {
	r.ro.Destroy()
	r.wo.Destroy()
	r.db.Close()
	return nil
}

func (r *RocksDB) DeleteRange(start, end []byte) error {
	batch := grocksdb.NewWriteBatch()
	batch.DeleteRange(start, end)
	err := r.db.Write(r.wo, batch)
	batch.Destroy()
	return err
}

// Ensure RocksDB implements KVDB
var _ KVDB = (*RocksDB)(nil)

type RocksDBTransaction struct {
	db    *grocksdb.DB
	batch *grocksdb.WriteBatch
	ro    *grocksdb.ReadOptions
	wo    *grocksdb.WriteOptions
}

func (t *RocksDBTransaction) Get(key []byte) ([]byte, io.Closer, error) {
	// this is probably not used and adding proper support is hard (see comment below)
	panic("not supported")
	//// For transactions, we need to check the batch first, then fall back to the DB
	//// This is a simplified implementation - in practice, you'd need to check
	//// if the key exists in the batch first
	//slice, err := t.db.Get(t.ro, key)
	//if err != nil {
	//	return nil, nil, errors.Wrap(err, "rocksdb transaction get")
	//}

	//if slice.Data() == nil {
	//	slice.Free()
	//	return nil, nil, pebble.ErrNotFound
	//}

	//data := make([]byte, len(slice.Data()))
	//copy(data, slice.Data())

	//return data, &RocksDBCloser{slice: slice}, nil
}

func (t *RocksDBTransaction) Set(key []byte, value []byte) error {
	t.batch.Put(key, value)
	return nil
}

func (t *RocksDBTransaction) Commit() error {
	err := t.db.Write(t.wo, t.batch)
	t.batch.Destroy()
	return err
}

func (t *RocksDBTransaction) Delete(key []byte) error {
	t.batch.Delete(key)
	return nil
}

func (t *RocksDBTransaction) Abort() error {
	t.batch.Destroy()
	return nil
}

func (t *RocksDBTransaction) NewIter(lowerBound []byte, upperBound []byte) (Iterator, error) {
	// This is a simplified implementation, really reading from uncommited data requires another approach
	// This will read from the database, not counting batch values. Probably a good idea to panic here instead
	iterOpts := grocksdb.NewDefaultReadOptions()
	if lowerBound != nil {
		iterOpts.SetIterateLowerBound(lowerBound)
	}
	if upperBound != nil {
		iterOpts.SetIterateUpperBound(upperBound)
	}

	iter := t.db.NewIterator(iterOpts)
	return &RocksDBIterator{
		iter:     iter,
		iterOpts: iterOpts,
	}, nil
}

func (t *RocksDBTransaction) DeleteRange(lowerBound []byte, upperBound []byte) error {
	t.batch.DeleteRange(lowerBound, upperBound)
	return nil
}

// Ensure RocksDBTransaction implements Transaction
var _ Transaction = (*RocksDBTransaction)(nil)

type RocksDBIterator struct {
	iter     *grocksdb.Iterator
	iterOpts *grocksdb.ReadOptions
}

func (i *RocksDBIterator) Key() []byte {
	if !i.Valid() {
		return nil
	}

	key := i.iter.Key()
	keyData := make([]byte, len(key.Data()))
	copy(keyData, key.Data())
	key.Free()
	return keyData
}

func (i *RocksDBIterator) First() bool {
	i.iter.SeekToFirst()
	return i.Valid()
}

func (i *RocksDBIterator) Next() bool {
	if !i.Valid() {
		return false
	}
	i.iter.Next()
	return i.Valid()
}

func (i *RocksDBIterator) Prev() bool {
	if !i.Valid() {
		return false
	}
	i.iter.Prev()
	return i.Valid()
}

func (i *RocksDBIterator) Valid() bool {
	return i.iter.Valid()
}

func (i *RocksDBIterator) Value() []byte {
	if !i.Valid() {
		return nil
	}

	value := i.iter.Value()
	valueData := make([]byte, len(value.Data()))
	copy(valueData, value.Data())
	value.Free()
	return valueData
}

func (i *RocksDBIterator) Close() error {
	i.iter.Close()
	i.iterOpts.Destroy()
	return nil
}

func (i *RocksDBIterator) SeekLT(key []byte) bool {
	i.iter.Seek(key)
	if !i.Valid() {
		i.iter.SeekToLast()
		return i.Valid()
	}

	if bytes.Equal(i.Key(), key) {
		i.iter.Prev()
	}

	return i.Valid()
}

func (i *RocksDBIterator) Last() bool {
	i.iter.SeekToLast()
	return i.Valid()
}

// Ensure RocksDBIterator implements Iterator
var _ Iterator = (*RocksDBIterator)(nil)
