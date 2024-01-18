package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	res1 := art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 11})
	assert.Nil(t, res1)
	res2 := art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 11})
	assert.NotNil(t, res2)
	assert.Equal(t, res2, &data.LogRecordPos{Fid: 1, Offset: 11})

	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 22})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 33})
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 11})
	pos1 := art.Get([]byte("key-1"))
	assert.NotNil(t, pos1)

	// 获取不存在的数据
	pos2 := art.Get([]byte(""))
	assert.Nil(t, pos2)
	t.Log(pos2)

	// Put 已有的数据 更改其对应的 val
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 22})
	pos1 = art.Get([]byte("key-1"))
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 22}, pos1)
	assert.NotNil(t, pos1)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()

	// 删除不存在的数据
	res1, ok1 := art.Delete([]byte("not exist"))
	assert.Nil(t, res1)
	assert.False(t, ok1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 11})
	res2, ok2 := art.Delete([]byte("key-1"))
	assert.NotNil(t, res2)
	assert.True(t, ok2)
	assert.Equal(t, uint32(1), res2.Fid)
	assert.Equal(t, int64(11), res2.Offset)
	pos := art.Get([]byte("key-1"))
	assert.Nil(t, pos)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()
	assert.Equal(t, 0, art.Size())
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 1})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 2})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 3})
	size := art.Size()
	assert.Equal(t, 3, size)
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	art.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 1})
	art.Put([]byte("adse"), &data.LogRecordPos{Fid: 1, Offset: 2})
	art.Put([]byte("bbde"), &data.LogRecordPos{Fid: 1, Offset: 3})
	art.Put([]byte("bade"), &data.LogRecordPos{Fid: 1, Offset: 4})

	iter := art.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
