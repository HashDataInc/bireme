# Getting Started Guide

In this section, we will demonstrate how to use bireme to synchronize a table named *demo.test* in MySQL to another table named *public.test* in GreenPlum database. Before we start, users need to deploy MySQL, Kafka and GreenPlum.

**Note:** All tables must contain primary key.

## 1. Preparation

## 1.1 MySQL configuration

Maxwell can only operate when row-based replication is on.

```
$ vi my.cnf

[mysqld]
server-id=1
log-bin=master
binlog_format=row
```

Create maxwell user and grant it authority.

```
mysql> GRANT ALL on maxwell.* to 'maxwell'@'%' identified by 'XXXXXX';
mysql> GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE on *.* to 'maxwell'@'%';

```

Create a new table *demo.test*.

```
create database demo;
create table demo.test (id int primary key, name varchar(10));
``` 

### 1.2 Create topic in Kafka

Create a topic named *mytopic* for Maxwell producing message.

```
bin/kafka-topics.sh --create --zookeeper zookeeperhost:2181 --replication-factor 1 --partitions 1 --topic mytopic
```

### 1.3 Create corresponding table in GreenPlum

```
create table test (id int primary key, name varchar(10));
```

### 1.4 Start Maxwell

```
bin/maxwell --user='maxwell' --password='XXXXXX' \
--host=mysqlhost --producer=kafka \
--kafka_topic = mytopic \
--kafka.bootstrap.servers=kafkahost:9092
```

Use `--host` option and `--kafka.bootstrap.servers` option to designate MySQL host and Kafka host.

## 2 Config bireme and start

### 2.1 Edit etc/config.properties

Specify connection infomation to the target database, as well as data source information.

```
target.url = jdbc:postgresql://gpdbhost:5432/XXXXXX
target.user = XXXXXX
target.passwd = XXXXXX

data_source = mysql

mysql.type = maxwell
mysql.kafka.server = kafkahost:9092
mysql.kafka.topic = mytopic
```

### 2.2 Edit table mapping file

As we appoint *mysql* as data source name, we need to create a file *mysql.properties* in *etc* directory and insert the following.

```
demo.test = public.test
```

### 2.3 Start bireme

```
bin/bireme start
```

When you get the message *The bireme has started*, bireme has successfully started. Then you could insert data in MySQL and it will be synced to GreenPlum.

If you need to stop, execute the following.

```
bin/bireme stop
```

Log files are located in *logs* directory.


