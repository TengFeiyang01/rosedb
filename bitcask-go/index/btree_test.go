package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)

	// 重复 Put 得到的是旧值
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.NotNil(t, res3)
	assert.Equal(t, res3, &data.LogRecordPos{Fid: 1, Offset: 2})
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.NotNil(t, res3)
	assert.Equal(t, res3, &data.LogRecordPos{Fid: 1, Offset: 2})

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)
	res2, ok2 := bt.Delete(nil)
	assert.NotNil(t, res2)
	assert.True(t, ok2)
	assert.Equal(t, uint32(1), res2.Fid)
	assert.Equal(t, int64(100), res2.Offset)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.Nil(t, res3)

	res4, ok4 := bt.Delete([]byte("aaa"))
	assert.NotNil(t, res4)
	assert.True(t, ok4)
	assert.Equal(t, uint32(22), res4.Fid)
	assert.Equal(t, int64(33), res4.Offset)
}

func TestBTree_Iterator(t *testing.T) {
	bt := NewBTree()
	// 1.BTree 为空的情况
	iter1 := bt.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// 2.BTree 有数据的情况
	bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 31, Offset: 31})
	iter2 := bt.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 3.BTree 有多条数据的输出
	bt.Put([]byte("acee"), &data.LogRecordPos{Fid: 31, Offset: 31})
	bt.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 33, Offset: 3221})
	bt.Put([]byte("ccde"), &data.LogRecordPos{Fid: 34, Offset: 33})
	bt.Put([]byte("eede"), &data.LogRecordPos{Fid: 34, Offset: 33})
	iter3 := bt.Iterator(false)
	assert.Equal(t, true, iter3.Valid())
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		//t.Log("key=", string(iter3.Key()))
	}

	iter4 := bt.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		//t.Log("key=", string(iter4.Key()))
	}

	// 4.测试 seek
	iter5 := bt.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		t.Log(string(iter5.Key()))
	}

	// 5.反向遍历的 seek
	iter6 := bt.Iterator(true)
	for iter6.Seek([]byte("ccf")); iter6.Valid(); iter6.Next() {
		t.Log(string(iter6.Key()))
	}

}
