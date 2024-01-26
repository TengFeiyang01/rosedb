前面的 Redis 数据结构设计中，List 结构的编码设计如下：

![](Pasted%20image%2020230529201112.png)


**元数据**

```sql
        +----------+------------+-----------+-----------+-----------+-----------+
key =>  |   type   |  expire    |  version  |  size     |  head     |  tail     |
        | (1byte)  | (Ebyte)    |  (8byte)  | (Sbyte)   | (8byte)   | (8byte)   |
        +----------+------------+-----------+-----------+-----------+-----------+
```

List 结构的元数据部分和 Hash、Set 比较类似，只是多了两个字段 head 和 tail。

List 数据结构可以看做是一个队列，可以在队列的头尾进行 Push、Pop 操作，因此我们可以使用一个标识来表示头尾，在初始情况下，`head = tail = U64_MAX / 2`。

![](Pasted%20image%2020230529201209.png)

**数据部分**

```sql
                     +---------------+
key|version|index => |     value     |
                     +---------------+
```

index 会根据 head 或者 tail 的值来确定，当在左边 Push 的时候，index 的值是 `head - 1`，当在右边 push 的时候，index 的值就是 `tail`。

# LPUSH
先查找元数据，如果不存在则初始化。

```go
type listInternalKey struct {
   key     []byte
   version int64
   index   uint64
}
```

```rust
pub(crate) struct ListInternalKey {
    pub(crate) key: Vec<u8>,
    pub(crate) version: u128,
    pub(crate) index: u64,
}
```

构造数据部分的 key，其中 index 的值就是 `meta.head - 1`，调用存储引擎的接口存储数据，并且更新元数据。

# RPUSH
和 LPush 基本类似，只是 index 的值是 `meta.tail`。

# LPOP
先查找元数据，如果元数据不存在或者 key 下面没有任何数据，那么直接返回。
否则构造数据部分的 key，并且调用存储引擎的接口获取值。然后需要更新元数据，元数据的 size 需要递减，然后 `meta.head += 1`。

# RPOP
和 LPop 基本类似，只是更新元数据的时候，是将元数据的 tail 递减，`meta.tail -= 1`。
