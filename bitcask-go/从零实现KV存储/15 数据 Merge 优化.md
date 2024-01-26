前面已经完成了数据 Merge 的基本流程，我们提供了一个叫 Merge 的方法，让用户可以自己调用，但是在基础功能之上还存在一些可以优化的地方，这一节就来看一下。
# 统计失效数据量
基础的 merge 流程是挨个遍历数据文件进行回收清理，但如果我们的存储引擎中，无效的数据本身就很少（或者没有无效的数据），那么全量的遍历整个数据文件，然后依次重写有效数据的操作代价较高，可能会导致严重的磁盘空间和带宽浪费。

所以我们可以在存储引擎运行的过程当中统计有多少数据量是失效的，这样会得到一个实时的失效数据总量，再让用户决定是否进行 merge 操作。

那么应该如何统计失效的数据量呢？我们可以在内存索引中维护一个值，记录每条数据在磁盘上的大小，Delete 数据的时候，可以得到旧的值，这个旧的值就是磁盘上失效的数据。

Put 存数据的时候，如果判断到有旧的数据存在，那么也同样累加这个值，这样我们就能够从 Put/Delete 数据的流程中，得到失效数据的累计值。

这里需要改动我们之前的索引数据结构的返回值，put 的时候，将之前的旧值返回出来，delete 的时候，将值返回出来，然后使用的时候，我们拿到这个值如果不为空的话，就增加累计值。

我们可以提供一个配置项，只有当失效的数据占比到达了某个比例之后，才进行 Merge 操作。

这里我们可以提供一个 Stat 的方法，返回存储引擎的一些统计信息，包含目前失效的数据量，当然我们可以加上其他的属性，比如数据库中 Key 的数量、数据文件的个数、占据磁盘总空间等。

```go
type Stat struct {
   KeyNum          uint  // key 的数量
   DataFileNum     uint  // 数据文件的个数
   ReclaimableSize int64 // 磁盘可回收的空间，字节为单位
   DiskSize        int64 // 所占磁盘空间的大小
}
```

```rust
// 数据库统计信息
#[derive(Debug)]
pub struct Stat {
    /// 数据文件的个数
    pub data_file_num: usize,
    /// 数据库中 key 的总量
    pub key_num: usize,
    /// 可以 Merge 的数据量
    pub reclaim_size: usize,
    /// 占据的磁盘空间大小
    pub disk_size: u64,
}
```

# 磁盘空间判断
merge 的时候，我们开启了一个临时目录，并且将有效的数据全部存放到这个临时目录中，但是设想这样一个极端情况：如果原本的数据目录本身就很大了，并且数据库中失效的数据很少（或者根本没有失效数据），那么在 merge 完成后，磁盘上有可能存在两倍于原始数据容量的数据，这有可能会导致磁盘空间被写满。

所以我们可以在 merge 之前，加上一个判断，查看当前数据目录所在磁盘，是否有足够的空间容纳 Merge 后的数据，避免由于数据量太大导致磁盘空间被写满。

具体的做法也很简单，我们添加一个获取目录所占容量的方法，然后再获取到所在磁盘的剩余容量，如果merge 后的数据量超过了磁盘的剩余容量，那么直接返回一个磁盘空间不足的错误信息。

**Go**

```go
// DirSize 获取一个目录的大小
func DirSize(dirPath string) (int64, error) {
   var size int64
   err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
      if err != nil {
         return err
      }
      if !info.IsDir() {
         size += info.Size()
      }
      return nil
   })
   return size, err
}

// AvailableDiskSize 获取磁盘剩余可用空间大小
func AvailableDiskSize() (uint64, error) {
   wd, err := syscall.Getwd()
   if err != nil {
      return 0, err
   }
   var stat syscall.Statfs_t
   if err = syscall.Statfs(wd, &stat); err != nil {
      return 0, err
   }
   return stat.Bavail * uint64(stat.Bsize), nil
}
```

**Rust**

```rust
pub fn available_disk_size() -> u64 {
    if let Ok(free) = fs2::available_space(PathBuf::from("/")) {
        return free;
    }
    0
}

pub fn dir_disk_size(dir_path: PathBuf) -> u64 {
    if let Ok(size) = fs_extra::dir::get_size(dir_path) {
        return size;
    }
    0
}
```

# Merge 测试
测试 case 如下

```test
1.没有任何数据的情况下进行 Merge
2.全部都是有效数据的情况下 Merge
3.有失效的数据，和被重复 Put 的数据
4.数据库中全部都是失效的数据
5.Merge 的过程中有新的写入和删除

bug 修复：
flock 文件不应该拷贝过去(Go)
没有数据的话直接返回，不用 Merge(Rust)
如果 merge 后的文件为空，则不拷贝到原数据目录(Rust)
```

测试代码：

## Go

```go
package bitcask_go

import (
   "bitcask-go/utils"
   "github.com/stretchr/testify/assert"
   "os"
   "sync"
   "testing"
)

// 没有任何数据的情况下进行 merge
func TestDB_Merge(t *testing.T) {
   opts := DefaultOptions
   dir, _ := os.MkdirTemp("", "bitcask-go-merge-1")
   opts.DirPath = dir
   db, err := Open(opts)
   defer destroyDB(db)
   assert.Nil(t, err)
   assert.NotNil(t, db)

   err = db.Merge()
   assert.Nil(t, err)
}

// 全部都是有效的数据
func TestDB_Merge2(t *testing.T) {
   opts := DefaultOptions
   dir, _ := os.MkdirTemp("", "bitcask-go-merge-2")
   opts.DataFileSize = 32 * 1024 * 1024
   opts.DataFileMergeRatio = 0
   opts.DirPath = dir
   db, err := Open(opts)
   defer destroyDB(db)
   assert.Nil(t, err)
   assert.NotNil(t, db)

   for i := 0; i < 50000; i++ {
      err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
      assert.Nil(t, err)
   }

   err = db.Merge()
   assert.Nil(t, err)

   // 重启校验
   err = db.Close()
   assert.Nil(t, err)

   db2, err := Open(opts)
   defer func() {
      _ = db2.Close()
   }()
   assert.Nil(t, err)
   keys := db2.ListKeys()
   assert.Equal(t, 50000, len(keys))

   for i := 0; i < 50000; i++ {
      val, err := db2.Get(utils.GetTestKey(i))
      assert.Nil(t, err)
      assert.NotNil(t, val)
   }
}

// 有失效的数据，和被重复 Put 的数据
func TestDB_Merge3(t *testing.T) {
   opts := DefaultOptions
   dir, _ := os.MkdirTemp("", "bitcask-go-merge-3")
   opts.DataFileSize = 32 * 1024 * 1024
   opts.DataFileMergeRatio = 0
   opts.DirPath = dir
   db, err := Open(opts)
   defer destroyDB(db)
   assert.Nil(t, err)
   assert.NotNil(t, db)

   for i := 0; i < 50000; i++ {
      err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
      assert.Nil(t, err)
   }
   for i := 0; i < 10000; i++ {
      err := db.Delete(utils.GetTestKey(i))
      assert.Nil(t, err)
   }
   for i := 40000; i < 50000; i++ {
      err := db.Put(utils.GetTestKey(i), []byte("new value in merge"))
      assert.Nil(t, err)
   }

   err = db.Merge()
   assert.Nil(t, err)

   // 重启校验
   err = db.Close()
   assert.Nil(t, err)

   db2, err := Open(opts)
   defer func() {
      _ = db2.Close()
   }()
   assert.Nil(t, err)
   keys := db2.ListKeys()
   assert.Equal(t, 40000, len(keys))

   for i := 0; i < 10000; i++ {
      _, err := db2.Get(utils.GetTestKey(i))
      assert.Equal(t, ErrKeyNotFound, err)
   }
   for i := 40000; i < 50000; i++ {
      val, err := db2.Get(utils.GetTestKey(i))
      assert.Nil(t, err)
      assert.Equal(t, []byte("new value in merge"), val)
   }
}

// 全部是无效的数据
func TestDB_Merge4(t *testing.T) {
   opts := DefaultOptions
   dir, _ := os.MkdirTemp("", "bitcask-go-merge-4")
   opts.DataFileSize = 32 * 1024 * 1024
   opts.DataFileMergeRatio = 0
   opts.DirPath = dir
   db, err := Open(opts)
   defer destroyDB(db)
   assert.Nil(t, err)
   assert.NotNil(t, db)

   for i := 0; i < 50000; i++ {
      err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
      assert.Nil(t, err)
   }
   for i := 0; i < 50000; i++ {
      err := db.Delete(utils.GetTestKey(i))
      assert.Nil(t, err)
   }

   err = db.Merge()
   assert.Nil(t, err)

   // 重启校验
   err = db.Close()
   assert.Nil(t, err)

   db2, err := Open(opts)
   defer func() {
      _ = db2.Close()
   }()
   assert.Nil(t, err)
   keys := db2.ListKeys()
   assert.Equal(t, 0, len(keys))
}

// Merge 的过程中有新的数据写入或删除
func TestDB_Merge5(t *testing.T) {
   opts := DefaultOptions
   dir, _ := os.MkdirTemp("", "bitcask-go-merge-5")
   opts.DataFileSize = 32 * 1024 * 1024
   opts.DataFileMergeRatio = 0
   opts.DirPath = dir
   db, err := Open(opts)
   defer destroyDB(db)
   assert.Nil(t, err)
   assert.NotNil(t, db)

   for i := 0; i < 50000; i++ {
      err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
      assert.Nil(t, err)
   }

   wg := new(sync.WaitGroup)
   wg.Add(1)
   go func() {
      defer wg.Done()
      for i := 0; i < 50000; i++ {
         err := db.Delete(utils.GetTestKey(i))
         assert.Nil(t, err)
      }
      for i := 60000; i < 70000; i++ {
         err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
         assert.Nil(t, err)
      }
   }()
   err = db.Merge()
   assert.Nil(t, err)
   wg.Wait()

   //重启校验
   err = db.Close()
   assert.Nil(t, err)

   db2, err := Open(opts)
   defer func() {
      _ = db2.Close()
   }()
   assert.Nil(t, err)
   keys := db2.ListKeys()
   assert.Equal(t, 10000, len(keys))

   for i := 60000; i < 70000; i++ {
      val, err := db2.Get(utils.GetTestKey(i))
      assert.Nil(t, err)
      assert.NotNil(t, val)
   }
}
```

## Rust

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::rand_kv::{get_test_key, get_test_value};
    use bytes::Bytes;
    use std::{sync::Arc, thread};

    #[test]
    fn test_merge_1() {
        // 没有任何数据的情况下进行 Merge
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-merge-1");
        opts.data_file_size = 32 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let res1 = engine.merge();
        assert!(res1.is_ok());

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_merge_2() {
        // 全部都是有效数据的情况
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-merge-2");
        opts.data_file_size = 32 * 1024 * 1024;
        opts.data_file_merge_ratio = 0 as f32;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        for i in 0..50000 {
            let put_res = engine.put(get_test_key(i), get_test_value(i));
            assert!(put_res.is_ok());
        }

        let res1 = engine.merge();
        assert!(res1.is_ok());

        // 重启校验
        std::mem::drop(engine);

        let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
        let keys = engine2.list_keys().unwrap();
        assert_eq!(keys.len(), 50000);

        for i in 0..50000 {
            let get_res = engine2.get(get_test_key(i));
            assert!(get_res.ok().unwrap().len() > 0);
        }

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_merge_3() {
        // 部分有效数据，和被删除数据的情况
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-merge-3");
        opts.data_file_size = 32 * 1024 * 1024;
        opts.data_file_merge_ratio = 0 as f32;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        for i in 0..50000 {
            let put_res = engine.put(get_test_key(i), get_test_value(i));
            assert!(put_res.is_ok());
        }
        for i in 0..10000 {
            let put_res = engine.put(get_test_key(i), Bytes::from("new value in merge"));
            assert!(put_res.is_ok());
        }
        for i in 40000..50000 {
            let del_res = engine.delete(get_test_key(i));
            assert!(del_res.is_ok());
        }

        let res1 = engine.merge();
        assert!(res1.is_ok());

        // 重启校验
        std::mem::drop(engine);

        let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
        let keys = engine2.list_keys().unwrap();
        assert_eq!(keys.len(), 40000);

        for i in 0..10000 {
            let get_res = engine2.get(get_test_key(i));
            assert_eq!(Bytes::from("new value in merge"), get_res.ok().unwrap());
        }

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_merge_4() {
        // 全部都是无效数据的情况
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-merge-4");
        opts.data_file_size = 32 * 1024 * 1024;
        opts.data_file_merge_ratio = 0 as f32;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        for i in 0..50000 {
            let put_res = engine.put(get_test_key(i), get_test_value(i));
            assert!(put_res.is_ok());
            let del_res = engine.delete(get_test_key(i));
            assert!(del_res.is_ok());
        }

        let res1 = engine.merge();
        assert!(res1.is_ok());

        // 重启校验
        std::mem::drop(engine);

        let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
        let keys = engine2.list_keys().unwrap();
        assert_eq!(keys.len(), 0);

        for i in 0..50000 {
            let get_res = engine2.get(get_test_key(i));
            assert_eq!(Errors::KeyNotFound, get_res.err().unwrap());
        }

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_merge_5() {
        // Merge 的过程中有新的写入和删除
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-merge-5");
        opts.data_file_size = 32 * 1024 * 1024;
        opts.data_file_merge_ratio = 0 as f32;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        for i in 0..50000 {
            let put_res = engine.put(get_test_key(i), get_test_value(i));
            assert!(put_res.is_ok());
        }
        for i in 0..10000 {
            let put_res = engine.put(get_test_key(i), Bytes::from("new value in merge"));
            assert!(put_res.is_ok());
        }
        for i in 40000..50000 {
            let del_res = engine.delete(get_test_key(i));
            assert!(del_res.is_ok());
        }

        let eng = Arc::new(engine);

        let mut handles = vec![];
        let eng1 = eng.clone();
        let handle1 = thread::spawn(move || {
            for i in 60000..100000 {
                let put_res = eng1.put(get_test_key(i), get_test_value(i));
                assert!(put_res.is_ok());
            }
        });
        handles.push(handle1);

        let eng2 = eng.clone();
        let handle2 = thread::spawn(move || {
            let merge_res = eng2.merge();
            assert!(merge_res.is_ok());
        });
        handles.push(handle2);

        for handle in handles {
            handle.join().unwrap();
        }

        // 重启校验
        std::mem::drop(eng);
        let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
        let keys = engine2.list_keys().unwrap();
        assert_eq!(keys.len(), 80000);

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }
}
```