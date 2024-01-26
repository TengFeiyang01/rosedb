在前面的 Redis 数据结构总体设计中，我们设计的 ZSet 数据结构的编码如下：
![](Pasted%20image%2020230605145315.png)

**元数据**

```sql
        +----------+------------+-----------+-----------+
key =>  |   type   |  expire    |  version  |  size     |
        | (1byte)  | (Ebyte)    |  (8byte)  | (Sbyte)   |
        +----------+------------+-----------+-----------+
```

和 Hash、Set 一致。

**数据部分**

```sql
                      +---------------+
key|version|member => |     score     |   (1)
                      +---------------+

                                         +---------------+
key|version|score|member|member size  => |      null     |   (2)
                                         +---------------+
```

分为两部分存储，第一部分主要是通过 key + member 获取到对应的 score，第二部分将 member 按照 score 分值进行排列，主要是为了能够按照 score 的顺序获取 member。

# ZAdd

首先根据 key 获取元数据，如果没拿到的话则初始化新的元数据结构体。

然后再获取这个 member 是否已经存在了，如果存在，并且其 score 和用户设置的 score 是一样的话，那么不需要做任何操作。

否则的话需要更新这个值，构造数据部分的两个 key，调用存储引擎的接口写入数据，并且更新元数据。

# ZScore

首先根据 key 获取元数据，如果没获取到的话，则说明 key 不存在则直接返回。

否则根据元数据的信息构造数据部分的 key：`key|version|member`，调用存储引擎的接口获取 score 的值。