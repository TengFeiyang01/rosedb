package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join("../tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path, _ := os.CreateTemp("../tmp", "a.data")
	fio, err := NewFileIOManager(path.Name())

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("bitcask kv"))
	assert.Equal(t, 10, n)
	t.Log(n, err)

	n, err = fio.Write([]byte("storage"))
	assert.Equal(t, 7, n)
	t.Log(n, err)
}

func TestFileIO_Read(t *testing.T) {
	path, _ := os.CreateTemp("../tmp", "a.data")
	fio, err := NewFileIOManager(path.Name())

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b := make([]byte, 5)
	n, err := fio.Read(b, 0)
	t.Log(b, n)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	t.Log(b2, n)
	assert.Equal(t, []byte("key-b"), b2)
}

func TestFileIO_Sync(t *testing.T) {
	path, _ := os.CreateTemp("../tmp", "a.data")
	fio, err := NewFileIOManager(path.Name())

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path, _ := os.CreateTemp("../tmp", "a.data")
	fio, err := NewFileIOManager(path.Name())

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
