# 快速开始

在这一部分，将演示如何用 dbsync 同步 MySQL 的一张表 demo.test 到 GreenPlum 数据库 public.test。在开始之前，用户需要部署好 MySQL，Kafka 以及 GreenPlum。  
**注:** 必须保证所有表都包含主键。

## 1. 准备工作

### 1.1 配置 MySQL

为保证 Maxwell 读取到 binlog，需要将 Replication 设置为 row-based。

```
$ vi my.cnf

[mysqld]
server-id=1
log-bin=master
binlog_format=row
```

为 Maxwell 创建用户 maxwell 并赋予权限

```
mysql> GRANT ALL on maxwell.* to 'maxwell'@'%' identified by 'XXXXXX';
mysql> GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE on *.* to 'maxwell'@'%';

```

创建新表 demo.test

```
create database demo;
create table demo.test (id int primary key, name varchar(10));
``` 

### 1.2 创建 Kafka topic

为 Maxwell 创建名为 mytopic 的 topic，--zookeeper 指定 zookeeper 的服务器。

### 1.3 在 GreenPlum 中新建表

```
create table test (id int primary key, name varchar(10));
```

### 1.4 启动 Maxwell 

```
bin/maxwell --user='maxwell' --password='XXXXXX' \
--host=mysqlhost --producer=kafka \
--kafka_topic = mytopic \
--kafka.bootstrap.servers=kafkahost:9092
```

--host 参数指定 MySQL 所在主机，--kafka.bootstrap.servers 参数指定 Kafka 所在主机

## 2 dbsync 配置及启动

### 2.1 修改 etc/config.properties

在 config.properties 文件中指定目标数据库的连接信息，数据源信息

```
target.url = jdbc:postgresql://gpdbhost:5432/XXXXXX
target.user = XXXXXX
target.passwd = XXXXXX

data_source = mysql

mysql.type = maxwell
mysql.kafka.server = kafkahost:9092
mysql.kafka.topic = mytopic
```

### 2.2 修改表映射文件

在 config.properties 文件中指定数据源为 mysql，因此需要新建文件 etc/mysql.properties，加入修改

```
demo.test = public.test
```

### 2.3 启动 dbsync
在 dbsync 主目录下，执行

```
bin/dbsync start
```

当输出 The dbsync has started. 表明Dbsync成功启动。  
启动后，在 MySQL 的 demo.test 表中插入数据，就会被同步到 GreenPlum 的public.test表。

需要停止时，执行

```
bin/dbsync stop
```

dbsync 的输出及日志位于 logs 文件夹下。
