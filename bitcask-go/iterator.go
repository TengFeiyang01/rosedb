package bitcask_go

import (
	"bitcask-go/index"
	"bytes"
)

// Iterator 迭代器
type Iterator struct {
	indexIter index.Iterator // 索引迭代器
	db        *DB
	options   IteratorOptions
}

func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		options:   opts,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek 根据传入的 key 查找第一个大于(或小于)等于的目标key，从这个key开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
}

// Next 跳转到下一个key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Valid 当前遍历的位置的
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key 当前遍历位置的 Key 数据
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 当前遍历位置的 Value 数据
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

// Close 关闭迭代器，释放相应资源
func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
