# 快速开始

# Part 1： Maxwell 数据源

在这一部分，将演示如何用 bireme 和 maxwell 同步 MySQL 的一张表 demo.test 到 GreenPlum 数据库 public.test。在开始之前，用户需要部署好 MySQL，Kafka 以及 GreenPlum。  
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

## 2 bireme 配置及启动

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

### 2.3 启动 bireme
在 bireme 主目录下，执行

```
bin/bireme start
```

当输出 The bireme has started. 表明Bireme成功启动。  
启动后，在 MySQL 的 demo.test 表中插入数据，就会被同步到 GreenPlum 的public.test表。

需要停止时，执行

```
bin/bireme stop
```

bireme 的输出及日志位于 logs 文件夹下。

**JMX**

|环境变量|缺省值|描述|
|:---:|:---:|:---:|
|JMX_PORT||JMX 端口号|
|JMX_AUTH|false|JMX 客户端是否需要认证|
|JMX_SSL|false|JMX 连接是否使用 SSL/TLS|
|JMX_LOG4J|true|Log4J JMX MBeans 是否应该被禁用|

默认会允许本地 JMX 连接，如果用户希望关闭 JMX，可以设置环境变量 `JMX_DISABLE=false`

**HEAP SIZE**

用户可以通过设置环境变量 MAX_HEAP 来指定 JVM 的 -Xmx 参数。`-Xmx${MAX_HEAP}m`

# Part 2: Debezium 数据源

在这一部分，将演示如何用 bireme 和 debezium 同步 Postgres 的一张表 public.source 到 GreenPlum 数据库 public.target。为了方便，我们使用 docker 部署 Postgres，Kafka 以及 Kafka Connect。

## 1. 准备工作

### 1.1 启动 docker 容器

**启动 Postgres**

```
$ docker run -it --name Postgres -p 5432:5432 -d debezium/postgres:latest
```

**启动 Zookeeper**

```
$ docker run -it --name Zookeeper -p 2181:2181 -p 2888:2888 -p 3888:3888 -d debezium/zookeeper:0.5
```

**启动 Kafka**

```
$ docker run -it --name Kafka -p 9092:9092 \
				-e ZOOKEEPER_CONNECT=Zookeeper:2181 \
				--link Zookeeper:Zookeeper -d debezium/kafka:0.5
```

**启动 Kafka Connect**

```
$ docker run -it --name Connect -p 8083:8083 \
				-e BOOTSTRAP_SERVERS=Kafka:9092 \
				-e CONFIG_STORAGE_TOPIC=my_connect_configs \
				-e OFFSET_STORAGE_TOPIC=my_connect_offsets \
				--link Zookeeper:Zookeeper --link Kafka:Kafka --link Postgres:Postgres \
				-d debezium/connect:0.5
```

### 1.2 监听 Postgres 数据库

使用 `curl` 发送请求给 Kafka Connect，请求的格式为 JSON 包含了 connector 的配置信息。

```
$ CONNECTOR_CONFIG='
{
    "name": "inventory-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "postgres",
        "database.server.name": "debezium"
    }
}'

$ curl -i -X POST -H "Accept:application/json" \
                -H "Content-Type:application/json" localhost:8083/connectors/ \
                -d "${CONNECTOR_CONFIG}"
```

### 1.3 分别在 Postgres 和 Greenplum 中创建表

Postgres 中创建 source 表

```
create table source (id int primary key, name varchar(10));
```

Greenplum 中创建 target 表

```
create table 表 (id int primary key, name varchar(10));
```

## 2 bireme 配置及启动

### 2.1 修改 etc/config.properties

在 config.properties 文件中指定目标数据库的连接信息，数据源信息

```
target.url = jdbc:postgresql://gpdbhost:5432/XXXXXX
target.user = XXXXXX
target.passwd = XXXXXX

data_source = debezium

debezium.type = debezium
debezium.kafka.server = kafkahost:9092
```
**Note:** data_source 的值必须与 1.2 部分中 `database.server.name` 配置项一致，这里不需要指定 Kafka topic。

### 2.2 修改表映射文件

在 config.properties 文件中指定数据源为 debezium，因此需要新建文件 etc/debezium.properties，加入修改


```
public.source = public.target
```

### 2.3 Start bireme

参照 Part 1 中相同步骤。
