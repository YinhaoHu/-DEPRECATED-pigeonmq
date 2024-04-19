# 存储层规定

---

## 概述

Bookie Ensemble对外提供可靠的、易扩展、负载均衡、高性能的健壮分布式AOF存储服务。保证一个段在所有的相关bookie中的数据是最终一致的。

Bookie Ensemble提供的保证如下：

* 一旦一个数据项在一个段中追加成功，那么最终会在所有的复制段的该位置追加相同的数据项。

* 一旦一个append请求被返回成功，那么有MinimumReplicaNumber个bookie拥有该数据项。

为了保证正确性，有必要在特定场合中引入ACID。

---

## RPC 规定

### CreatePrimarySegmentRPC

**参数**

`SegmentID string`：新建的SegmentID。注意，SegmentID实际上是由topic+first_byte_in_topic两个信息组成的。

**返回**

`Err Error`

**接收方实现**

1. bookie根据负载、其他bookie的可用空间以及config.ReplicateNumber发送CreateBackupSegmentRPC到其他bookie。
2. 在ZK上设置该segment的znode。
3. 当已发送的CreateBackupSegmentRPC都已经成功了那么就返回OK。
4. 如果没有足够的满足条件的bookie，那么返回error。
5. 只要error被返回，zk中的znode就被删除。

**错误规定**

`CreatePrimarySegmentNoEnoughBookies`：不能找到`Config.ReplicateNumber`个bookies作为follower。选择限制个数为`Config.ReplicateNumber`而不是`Config.MinimumReplicaNumber`的原因是：假如`Config.MinimumReplicaNumber`=1，此时bookie立即返回，但其他bookie都没有负责这个segment，这个segment将会永远处于无bookie负责状态。

**其他说明**

只要从ZK获取到的bookies信息后，分析发现有足够的可用bookies，那么我们就认为接下来的与其他bookies的互动是很可能顺利进行的，也就是说，其他bookies没有crash。

### CreateBackupSegmentRPC

**参数**

`SegmentID string` 新建的Segment的ID。

**返回**

`Err Error`

**Receivere Implementation**

1. Bookie新建一个Segment。

### ReadSegmentRPC

**参数**

`ID string`：待读取的Segment的ID。

`BeginPosition int`：待读取的Segment中的起点。

`Size int`：期望从该Segment中读取的字节数。

**返回**

`Err Error`：错误信息。

`Size int`：实际读取的字节数。

`Contents [][]byet`：若干个消息字节slice。

**接收方实现**

打开指定的文件，seek到指定位置读取指定大小。

### AppendPrimarySegmentRPC

**参数**

`ID string` ：待写入的Segment的ID。

`Content []byet`：待写入的内容。

**返回**

`Err Error`：错误信息。 

**接收方实现**

---

## 状态规定

### Bookie

**State On ZooKeeper**

ZnodeName: [ip:port] 

`Used int`    已使用 #MB 存储空间。

`Free int`     空闲的#MB存储空间。

**State On Local Memory**

### Segment

**State on ZooKeeper**

`Bookies []BookieAddress`  负责该段的所有Bookie地址。

`Leader BookieAddress`    ephemeral znode，PrimarySegment所在的Bookie地址。     

`State string`    该段的状态，只能为："Open", "Close"。

**Storage Spec**

- Segment是一个预分配的大小为config.max-segment-size的append-only文件，其名称格式由调用方指定。但其是："topic/begin_byte"。其中begin_byte是文件中的第一个字节在topic下的字节位置。

- 段的物理组织是一个连续的槽，每个槽的结构是大小为4字节的槽大小+有效负载。

### 成员关系

**Heartbeat mechanism**

节点在bookie znode中建立自己的ephemeral znode，100毫秒会话保活信息。

**Segment Leader Election**

每个follower watch段的Leader znode，如果被删除，那么选出最合适的作为leader。

---

## 过程规定

### Bookie初始化

步骤及解释

1. 读取配置

2. 初始化存储状态
   
   1. 检测`Config.StorageDirectoryPath` 是否存在。如果不存在，那么就创建该文件夹。
   
   2. 检测`Config.StorageDirectoryPath` 中所有段的大小和所有段的payload，从而设置`Bookie.StorageFree`和`Bookie.StorageUsed`。

3. 初始化ZK状态
   
   1. 设置自己的ephemeral znode的名字为`[IPAddress:Port]` ，数据为`BookieState`的字节。

4. 开启对外服务(包括非存储层和存储层的其他Bookie发送的请求)

---

## 交互

### 文件系统

Bookie在文件系统中存储段数据。

段数据存储格式如下：

```
<header:payload_size,state> <payload>
```

支持的操作：创建段、写段、读段、关闭段、删除段。

### 其他Bookie

一个Bookie需要了解其他bookie和与其他bookie通信才能完成某些特定任务。

**Leader Election**    当发生故障时，所有的bookie都应该按照预定的算法选出segment的唯一leader。

**Backup installation**    当初始化一个segment或者某个segment的follower发生故障指定时间段后，leader需要从其他可用bookies中选出新的follower，并且将segment发送给他们。更新zk上的信息。

### ZooKeeper

系统管理员应该预先在ZooKeeper上添加以下路径的znode。

- `/bookies`：管理bookies集群信息。

- `/segments`: 管理segments的信息。

- `/locks/bookies`：bookies锁，用于分配follower。

Bookie Ensemble在ZK上的znode结构如`docs/diagrams/storage-layer.drawio-ZK hierarchy`所示。

ZooKeeper为Bookie Ensemble提供以下的服务：

**领袖选举**    zk提供必要的信息来服务segment的leader election。

**服务发现**    Broker通过访问ZK节点，发现应该找哪些bookie完成读请求和追加请求。

**集群关系**    如果一个follower bookie出现故障宕机，我们应该给leader一个配置的时间来决定什么时候把数据迁移到新的bookie上。这样的设计是为了避免bookie宕机后快速恢复而发生了不必要的再复制。

### Broker

---

## 问题记录

**Q：有没有必要保证故障恢复？**
