package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	defer iterator.Close()
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestIterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	defer iterator.Close()
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	value, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestKey(10), value)
}

func TestIterator_Multi_Values(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-3")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("Alter"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("Alex"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("Bob"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("Candy"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("David"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("Expert"), utils.RandomValue(10))
	assert.Nil(t, err)

	// 正向迭代
	iter1 := db.NewIterator(DefaultIteratorOptions)
	defer iter1.Close()
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		val, err := iter1.Value()
		assert.Nil(t, err)
		t.Log("key = ", string(iter1.Key()), "value = ", string(val))
	}

	iter1.Rewind()
	for iter1.Seek([]byte("Bo")); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
		t.Log("key = ", string(iter1.Key()))
	}

	// 反向迭代
	iterOpts1 := DefaultIteratorOptions
	iterOpts1.Reverse = true
	iter2 := db.NewIterator(iterOpts1)
	defer iter2.Close()
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		val, err := iter2.Value()
		assert.Nil(t, err)
		t.Log("key = ", string(iter2.Key()), "value = ", string(val))
	}

	iter2.Rewind()
	for iter2.Seek([]byte("Dr")); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
		t.Log("key = ", string(iter2.Key()))
	}

	// 指定了 prefix = "Alt" 只会打印 Alter
	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Prefix = []byte("Alt")
	iter3 := db.NewIterator(iterOpts2)
	defer iter3.Close()
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
		t.Log("key = ", string(iter3.Key()))
	}
}
