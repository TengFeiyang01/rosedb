加上这一章节的 LogRecord 编码和解码，基本上读写的整个流程的逻辑就是完整的了，我们包含 Put、Get、Delete 方法的 bitcask KV 存储引擎就基本上能够工作起来了。

前面也已经提到过，LogRecord 就是实际写到数据文件中的 key、value 数据，还包含一些其他的头部元数据信息。

在 bitcask 论文中，其实也对其有基本的描述：

![](Pasted%20image%2020230529171721.png)

而我们在实际的设计当中，基本参考了这个格式，但是会略有不同，如下图是我们的 bitcask 存储引擎的文件中存储的日志记录的格式：

![](Pasted%20image%2020230529171851.png)

# LogRecord 编码
编码实际上就是将用户传入的 Key、Value 数据转化为最终写入到数据文件中的一条日志记录，Go 语言中可以使用系统自带的 binary 包，Rust 则可以使用较为常用的 Bytes 包中的 BytesMut 结构，同时我们使用 prost 库其中的几个方法来辅助我们完成编码。

首先是 EncodeLogRecord 编码方法，其功能是将传入的 LogRecord 结构体转换为符合我们日志记录格式的字节数组。

我们首先会将 header 部分的几个字段写入到对应的字节数组中，header 的这几个字段的占据的空间如下：
- crc 是 uint32 类型的，占 4 个字节
- Type 定义为 byte 类型，只需要 1 个字节
- keySize 和 valueSize 是变长的，每一个的最大值是 5

Header 部分编码之后，我们需要将 key 和 value 的数据拷贝到字节数组中，因为传入的 key 和 value 就是字节数组类型的，所以不用对其进行编码。

最后是对数据做 crc 校验，然后也需要将这个值存储到磁盘中，方便获取的时候进行比较，判断数据的有效性。

# LogRecord 解码
从数据文件中读取日志记录 LogRecord 时，首先会按照固定大小读取 header 部分的字节数，然后对其进行解码，主要是根据编码时的对应长度获取 crc 校验值、Type、key size 和 value size。

然后再根据 key size 和 value size 读出实际的 key/value 数据。

最后需要校验读取出的 crc 值是否和 LogRecord 对应的 crc 值是否相等，如果不相等的话则说明这条数据存在错误，那么需要返回对应的错误信息。
# 补充内容—存储引擎基础功能单元测试
测试 case 如下：

```text
Put
1.正常 Put 一条数据
2.重复 Put key 相同的数据
3.key 为空
4.value 为空
5.写到数据文件进行了转换
6.重启后再 Put 数据

Get
1.正常读取一条数据
2.读取一个不存在的 key
3.值被重复 Put 后在读取
4.值被删除后再 Get
5.转换为了旧的数据文件，从旧的数据文件上获取 value
6.重启后，前面写入的数据都能拿到

Delete
1.正常删除一个存在的 key
2.删除一个不存在的 key
3.删除一个空的 key
4.值被删除之后重新 Put
5.重启之后，再进行校验
```

Rust 代码，测试过程中发现 bug 一个：

>Pop 是取出列表的最后一个元素，这个循环中应该取出旧的数据文件，而不是把新的数据文件取出来
>因此翻转此列表，将活跃文件放到了列表第一个位置

```rust
// 翻转数据文件列表，最新的在第一个，最旧的在最后
data_files.reverse(); // 新加上这一行

// 将旧的数据文件保存到 older_files 中
let mut older_files = HashMap::new();
if data_files.len() > 1 {
    for _ in 0..=data_files.len() - 2 {
        let file = data_files.pop().unwrap();
        older_files.insert(file.get_file_id(), file);
    }
}
```

## Go 测试代码
db_test.go 内容如下：

```go
package bitcask_go

import (
   "bitcask-go/utils"
   "github.com/stretchr/testify/assert"
   "os"
   "testing"
)

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
   if db != nil {
      if db.activeFile != nil {
         _ = db.activeFile.Close() // todo 实现了 Close 方法之后，这里使用 Close 方法替代
      }
      err := os.RemoveAll(db.options.DirPath)
      if err != nil {
         panic(err)
      }
   }
}

func TestOpen(t *testing.T) {
   opts := DefaultOptions
   dir, _ := os.MkdirTemp("", "bitcask-go")
   opts.DirPath = dir
   db, err := Open(opts)
   defer destroyDB(db)
   assert.Nil(t, err)
   assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
   opts := DefaultOptions
   dir, _ := os.MkdirTemp("", "bitcask-go-put")
   opts.DirPath = dir
   opts.DataFileSize = 64 * 1024 * 1024
   db, err := Open(opts)
   defer destroyDB(db)
   assert.Nil(t, err)
   assert.NotNil(t, db)

   // 1.正常 Put 一条数据
   err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
   assert.Nil(t, err)
   val1, err := db.Get(utils.GetTestKey(1))
   assert.Nil(t, err)
   assert.NotNil(t, val1)

   // 2.重复 Put key 相同的数据
   err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
   assert.Nil(t, err)
   val2, err := db.Get(utils.GetTestKey(1))
   assert.Nil(t, err)
   assert.NotNil(t, val2)

   // 3.key 为空
   err = db.Put(nil, utils.RandomValue(24))
   assert.Equal(t, ErrKeyIsEmpty, err)

   // 4.value 为空
   err = db.Put(utils.GetTestKey(22), nil)
   assert.Nil(t, err)
   val3, err := db.Get(utils.GetTestKey(22))
   assert.Equal(t, 0, len(val3))
   assert.Nil(t, err)

   // 5.写到数据文件进行了转换
   for i := 0; i < 1000000; i++ {
      err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
      assert.Nil(t, err)
   }
   assert.Equal(t, 2, len(db.olderFiles))

   // 6.重启后再 Put 数据
   //db.Close() // todo 实现 Close 方法后这里用 Close() 替代
   err = db.activeFile.Close()
   assert.Nil(t, err)

   // 重启数据库
   db2, err := Open(opts)
   assert.Nil(t, err)
   assert.NotNil(t, db2)
   val4 := utils.RandomValue(128)
   err = db2.Put(utils.GetTestKey(55), val4)
   assert.Nil(t, err)
   val5, err := db2.Get(utils.GetTestKey(55))
   assert.Nil(t, err)
   assert.Equal(t, val4, val5)
}

func TestDB_Get(t *testing.T) {
   opts := DefaultOptions
   dir, _ := os.MkdirTemp("", "bitcask-go-get")
   opts.DirPath = dir
   opts.DataFileSize = 64 * 1024 * 1024
   db, err := Open(opts)
   defer destroyDB(db)
   assert.Nil(t, err)
   assert.NotNil(t, db)

   // 1.正常读取一条数据
   err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
   assert.Nil(t, err)
   val1, err := db.Get(utils.GetTestKey(11))
   assert.Nil(t, err)
   assert.NotNil(t, val1)

   // 2.读取一个不存在的 key
   val2, err := db.Get([]byte("some key unknown"))
   assert.Nil(t, val2)
   assert.Equal(t, ErrKeyNotFound, err)

   // 3.值被重复 Put 后在读取
   err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
   assert.Nil(t, err)
   err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
   val3, err := db.Get(utils.GetTestKey(22))
   assert.Nil(t, err)
   assert.NotNil(t, val3)

   // 4.值被删除后再 Get
   err = db.Put(utils.GetTestKey(33), utils.RandomValue(24))
   assert.Nil(t, err)
   err = db.Delete(utils.GetTestKey(33))
   assert.Nil(t, err)
   val4, err := db.Get(utils.GetTestKey(33))
   assert.Equal(t, 0, len(val4))
   assert.Equal(t, ErrKeyNotFound, err)

   // 5.转换为了旧的数据文件，从旧的数据文件上获取 value
   for i := 100; i < 1000000; i++ {
      err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
      assert.Nil(t, err)
   }
   assert.Equal(t, 2, len(db.olderFiles))
   val5, err := db.Get(utils.GetTestKey(101))
   assert.Nil(t, err)
   assert.NotNil(t, val5)

   // 6.重启后，前面写入的数据都能拿到
   //db.Close() // todo 实现 Close 方法后这里用 Close() 替代
   err = db.activeFile.Close()
   assert.Nil(t, err)

   // 重启数据库
   db2, err := Open(opts)
   val6, err := db2.Get(utils.GetTestKey(11))
   assert.Nil(t, err)
   assert.NotNil(t, val6)
   assert.Equal(t, val1, val6)

   val7, err := db2.Get(utils.GetTestKey(22))
   assert.Nil(t, err)
   assert.NotNil(t, val7)
   assert.Equal(t, val3, val7)

   val8, err := db.Get(utils.GetTestKey(33))
   assert.Equal(t, 0, len(val8))
   assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
   opts := DefaultOptions
   dir, _ := os.MkdirTemp("", "bitcask-go-delete")
   opts.DirPath = dir
   opts.DataFileSize = 64 * 1024 * 1024
   db, err := Open(opts)
   defer destroyDB(db)
   assert.Nil(t, err)
   assert.NotNil(t, db)

   // 1.正常删除一个存在的 key
   err = db.Put(utils.GetTestKey(11), utils.RandomValue(128))
   assert.Nil(t, err)
   err = db.Delete(utils.GetTestKey(11))
   assert.Nil(t, err)
   _, err = db.Get(utils.GetTestKey(11))
   assert.Equal(t, ErrKeyNotFound, err)

   // 2.删除一个不存在的 key
   err = db.Delete([]byte("unknown key"))
   assert.Nil(t, err)

   // 3.删除一个空的 key
   err = db.Delete(nil)
   assert.Equal(t, ErrKeyIsEmpty, err)

   // 4.值被删除之后重新 Put
   err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
   assert.Nil(t, err)
   err = db.Delete(utils.GetTestKey(22))
   assert.Nil(t, err)

   err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
   assert.Nil(t, err)
   val1, err := db.Get(utils.GetTestKey(22))
   assert.NotNil(t, val1)
   assert.Nil(t, err)

   // 5.重启之后，再进行校验
   //db.Close() // todo 实现 Close 方法后这里用 Close() 替代
   err = db.activeFile.Close()
   assert.Nil(t, err)

   // 重启数据库
   db2, err := Open(opts)
   _, err = db2.Get(utils.GetTestKey(11))
   assert.Equal(t, ErrKeyNotFound, err)

   val2, err := db2.Get(utils.GetTestKey(22))
   assert.Nil(t, err)
   assert.Equal(t, val1, val2)
}
```

## Rust 测试代码
**==and_kv.rs 文件中的内容做一下修改（视频中遗漏了）==**：
- 增加 `#[allow(dead_code)]` 防止编译告警
- get_test_value 方法增加一点长度，不然测不到文件写满转换的 case
修改后的文件内容如下：

```rust
use bytes::Bytes;

#[allow(dead_code)]
pub fn get_test_key(i: usize) -> Bytes {
    Bytes::from(std::format!("bitcask-rs-key-{:09}", i))
}

#[allow(dead_code)]
pub fn get_test_value(i: usize) -> Bytes {
    Bytes::from(std::format!(
        "bitcask-rs-value-value-value-value-value-value-value-value-value-{:09}",
        i
    ))
}

#[test]
fn test_get_test_key_value() {
    for i in 0..=10 {
        assert!(get_test_key(i).len() > 0)
    }

    for i in 0..=10 {
        assert!(get_test_value(i).len() > 0)
    }
}
```

在 lib.rs 中加上如下内容：

```rust
#[cfg(test)]
mod db_tests;
```

db_test.rs 内容如下：

```rust
use bytes::Bytes;
use std::path::PathBuf;

use crate::{
    db::Engine,
    errors::Errors,
    options::Options,
    util::rand_kv::{get_test_key, get_test_value},
};

#[test]
fn test_engine_put() {
    let mut opts = Options::default();
    opts.dir_path = PathBuf::from("/tmp/bitcask-rs-put");
    opts.data_file_size = 64 * 1024 * 1024;
    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    // 1.正常 Put 一条数据
    let res1 = engine.put(get_test_key(11), get_test_value(11));
    assert!(res1.is_ok());
    let res2 = engine.get(get_test_key(11));
    assert!(res2.is_ok());
    assert!(res2.unwrap().len() > 0);

    // 2.重复 Put key 相同的数据
    let res3 = engine.put(get_test_key(22), get_test_value(22));
    assert!(res3.is_ok());
    let res4 = engine.put(get_test_key(22), Bytes::from("a new value"));
    assert!(res4.is_ok());
    let res5 = engine.get(get_test_key(22));
    assert!(res5.is_ok());
    assert_eq!(res5.unwrap(), Bytes::from("a new value"));

    // 3.key 为空
    let res6 = engine.put(Bytes::new(), get_test_value(123));
    assert_eq!(Errors::KeyIsEmpty, res6.err().unwrap());

    // 4.value 为空
    let res7 = engine.put(get_test_key(33), Bytes::new());
    assert!(res7.is_ok());
    let res8 = engine.get(get_test_key(33));
    assert_eq!(0, res8.ok().unwrap().len());

    // 5.写到数据文件进行了转换
    for i in 0..=1000000 {
        let res = engine.put(get_test_key(i), get_test_value(i));
        assert!(res.is_ok());
    }

    // 6.重启后再 Put 数据
    // 先关闭原数据库 todo
    let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
    let res9 = engine2.put(get_test_key(55), get_test_value(55));
    assert!(res9.is_ok());

    let res10 = engine2.get(get_test_key(55));
    assert_eq!(res10.unwrap(), get_test_value(55));

    // 删除测试的文件夹
    std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
}

#[test]
fn test_engine_get() {
    let mut opts = Options::default();
    opts.dir_path = PathBuf::from("/tmp/bitcask-rs-get");
    opts.data_file_size = 64 * 1024 * 1024;
    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    // 1.正常读取一条数据
    let res1 = engine.put(get_test_key(111), get_test_value(111));
    assert!(res1.is_ok());
    let res2 = engine.get(get_test_key(111));
    assert!(res2.is_ok());
    assert!(res2.unwrap().len() > 0);

    // 2.读取一个不存在的 key
    let res3 = engine.get(Bytes::from("not existed key"));
    assert_eq!(Errors::KeyNotFound, res3.err().unwrap());

    // 3.值被重复 Put 后在读取
    let res4 = engine.put(get_test_key(222), get_test_value(222));
    assert!(res4.is_ok());
    let res5 = engine.put(get_test_key(222), Bytes::from("a new value"));
    assert!(res5.is_ok());
    let res6 = engine.get(get_test_key(222));
    assert_eq!(Bytes::from("a new value"), res6.unwrap());

    // 4.值被删除后再 Get
    let res7 = engine.put(get_test_key(333), get_test_value(333));
    assert!(res7.is_ok());
    let res8 = engine.delete(get_test_key(333));
    assert!(res8.is_ok());
    let res9 = engine.get(get_test_key(333));
    assert_eq!(Errors::KeyNotFound, res9.err().unwrap());

    // 5.转换为了旧的数据文件，从旧的数据文件上获取 value
    for i in 500..=1000000 {
        let res = engine.put(get_test_key(i), get_test_value(i));
        assert!(res.is_ok());
    }
    let res10 = engine.get(get_test_key(505));
    assert_eq!(get_test_value(505), res10.unwrap());

    // 6.重启后，前面写入的数据都能拿到
    // 先关闭原数据库 todo
    let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
    let res11 = engine2.get(get_test_key(111));
    assert_eq!(get_test_value(111), res11.unwrap());
    let res12 = engine2.get(get_test_key(222));
    assert_eq!(Bytes::from("a new value"), res12.unwrap());
    let res13 = engine2.get(get_test_key(333));
    assert_eq!(Errors::KeyNotFound, res13.err().unwrap());

    // 删除测试的文件夹
    std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
}

#[test]
fn test_engine_delete() {
    let mut opts = Options::default();
    opts.dir_path = PathBuf::from("/tmp/bitcask-rs-delete");
    opts.data_file_size = 64 * 1024 * 1024;
    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    // 1.正常删除一个存在的 key
    let res1 = engine.put(get_test_key(111), get_test_value(111));
    assert!(res1.is_ok());
    let res2 = engine.delete(get_test_key(111));
    assert!(res2.is_ok());
    let res3 = engine.get(get_test_key(111));
    assert_eq!(Errors::KeyNotFound, res3.err().unwrap());

    // 2.删除一个不存在的 key
    let res4 = engine.delete(Bytes::from("not-existed-key"));
    assert!(res4.is_ok());

    // 3.删除一个空的 key
    let res5 = engine.delete(Bytes::new());
    assert_eq!(Errors::KeyIsEmpty, res5.err().unwrap());

    // 4.值被删除之后重新 Put
    let res6 = engine.put(get_test_key(222), get_test_value(222));
    assert!(res6.is_ok());
    let res7 = engine.delete(get_test_key(222));
    assert!(res7.is_ok());
    let res8 = engine.put(get_test_key(222), Bytes::from("a new value"));
    assert!(res8.is_ok());
    let res9 = engine.get(get_test_key(222));
    assert_eq!(Bytes::from("a new value"), res9.unwrap());

    // 5.重启后再 Put 数据
    // 先关闭原数据库 todo
    let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
    let res10 = engine2.get(get_test_key(111));
    assert_eq!(Errors::KeyNotFound, res10.err().unwrap());
    let res11 = engine.get(get_test_key(222));
    assert_eq!(Bytes::from("a new value"), res11.unwrap());

    // 删除测试的文件夹
    std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
}
```