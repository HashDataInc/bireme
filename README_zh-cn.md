# dbsync

[入门指南](docs/dbsync_guide_zh-cn.md)

Dbsync 是一个 Greenplum / HashData 数据仓库的**增量**同步工具。目前支持 MySQL 数据源。  

[Greenplum](http://greenplum.org/) 是一个高级，功能齐全的开源数据仓库，为PB级数据量提供强大而快速的分析。它独特地面向大数据分析，由世界上最先进的基于成本的查询优化器提供支持，可在大数据量上提供高分析查询性能。

[HashData](http://www.hashdata.cn) 则是基于 Greenplum 构建弹性的云端数据仓库。  

Dbsync 采用 DELETE + COPY 的方式，将数据源的修改记录同步到 Greenplum / HashData ，相较于INSERT + UPDATE + DELETE的方式，COPY 方式速度更快，性能更优。

Dbsync 特性与约束：

* 采用小批量加载的方式提升数据同步的性能，默认加载延迟时间为10秒钟。
* 所有表在目标数据库中必须有主键
* 目前支持 Maxwell 数据源。

## 1.1 系统架构  

![architecture](docs/architecture.png)

Dbsync 支持多数据源同步工作，可以同时从多个数据源并行读取数据，并将数据同步加载到目标数据库。

## 1.2 Maxwell + Kafka 数据源

Maxwell + Kafka 是 dbsync目前支持的一种数据源类型，架构如下图：

![data_source](docs/data_source.png)
  
* [Maxwell](http://maxwells-daemon.io/) 是一个 MySQL binlog 的读取工具，它可以实时读取 MySQL 的 binlog，并生成 JSON 格式的消息，作为生产者发送给 Kafka。  
* [Kafka](http://kafka.apache.org/) 是一个分布式数据流平台，它可以接收、存储生产者发布的消息，并供给消费者使用。  

## 1.3 Dbsync工作原理

Dbsync 从数据源读取数据 (Record)，将其转化为内部格式 (Row) 并缓存，当缓存数据达到一定量，将这些数据合并为一个任务 (Task)，每个任务包含两个集合，delete 集合与insert 集合，最后把这些数据更新到目标数据库。

![dbsync](docs/dbsync.png)

## 1.4 配置文件介绍

配置文件包括两部分：  

* 基本配置文件：默认为 **config.properties**，包含 dbsync 的基本配置。  
* 表映射文件：**\<source_name\>.properties**，每一个数据源都对应一个该类文件，指定待同步的表及该表在目标数据库中对应的表。 \<source_name\> 在 config.properties 文件中指定。

### 1.4.1 config.properties

**必要参数**
  
|参数|描述|
|:---:|:---:|
|target.url|目标数据库的地址，格式:<br>jdbc:postgresql://\<ip\>:\<port\>/\<database\>|
|target.user|用于连接目标数据库的用户名|
|target.passwd|用于连接目标数据库的密码|
|data.source|指定数据源，即 \<source_name\>，多个数据源用逗号分隔开，忽略空白字符|
|\<source_name\>.type|指定数据源的类型，例如 maxwell|

**注:** 数据源名称只是符号，方便在Dbsync中指代，可以根据需求修改。

**Maxwell 数据源参数**

|参数|描述|
|:---:|:---:|
|\<source_name\>.kafka.server|数据源的 Kafka 地址，格式:  <br>\<ip\>:\<port\>|
|\<source_name\>.kafka.topic|数据源在 Kafka 中对应的 topic|


**其它参数**

|参数|描述|默认值|
|:---:|:---:|:---:|
|bookkeeping.url|用于记录同步状态的状态数据库地址|同 target.url|
|bookkeeping.user|状态数据库的用户名|同 target.user|
|bookkeeping.passwd|状态数据库的密码|同 target.passwd|
|bookkeep.interval|更新状态到数据库的时间间隔，单位毫秒|1000|
|bookkeep.table_name|用于记录状态的表名|bookkeeping|
|metrics.reporter|Dbsync指定了两种监控模式，一种是 console ，另外一种是 jmx ，如果不需要监控，可以指定为 none|console|
|metrics.reporter.console.interval|输出 metrics 的时间间隔，当 metrics.reporter 为 console 有效，单位秒|10|
|loader.conn_pool.size|目标数据库的连接个数，小于等于 Change Loader 的数量|10|
|loader.task_queue.size|每个 Change Loader 中 Task 队列的长度|2|
|transform.pool.size|用于 Transform 的线程池大小|10|
|merge.pool.size|用于 Merge 的线程池大小|10|
|merge.interval|最大 Merge 间隔，单位毫秒|10000|
|merge.batch.size|单次 Merge 中最多的 Row 的数量|50000|

### 1.4.2 \<source_name\>.properties

数据源的配置文件中，指定该数据源包括的表，以及在目标数据库中对应的表。

```
<OriginTable_1> = <MappedTable_1>
<OriginTable_2> = <MappedTable_2>
...
```

## 1.5 参考

[Maxwell 参考](http://maxwells-daemon.io/)  
[Kafka 参考](http://kafka.apache.org/)