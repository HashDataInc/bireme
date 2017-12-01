# Getting Started Guide

# Part 1 Maxwell Data Source

In this part, we will demonstrate how to use bireme cooperated with maxwell to synchronize a table named *demo.test* in MySQL to another table named *public.test* in GreenPlum database. Before we start, users need to deploy MySQL, Kafka and GreenPlum.

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

**JMX**

|Environment Variable|Default|Description|
|:---:|:---:|:---:|
|JMX_PORT||Enables JMX and specifies the port number that will be used for JMX.|
|JMX_AUTH|false|Whether JMX clients must use password authentication when connecting.|
|JMX_SSL|false|Whether JMX clients connect using SSL/TLS.|
|JMX_LOG4J|true|Whether the Log4J JMX MBeans should be disabled.|

By default we allow local JMX connections. If you want to close JMX, set environment variable `JMX_DISABLE=false`.

**HEAP SIZE**

You can use `MAX_HEAP` variable to set the maxmium memory of JVM. The value is used to specify the JVM parameter `-Xmx${MAX_HEAP}m`

# Part 2 Debezium Data Source

In this part, we will demonstrate how to use bireme cooperated with debezium to synchronize a table named *public.source* in Postgres to another table named *public.target* in GreenPlum database. For convenience, we will use docker to set up Postgres, Kafka and Kafka Connect.

**Note:** All tables must contain primary key.

## 1. Preparation

### 1.1 Set up in docker

**Start Postgres**

```
$ docker run -it --name Postgres -p 5432:5432 -d debezium/postgres:latest
```

**Start Zookeeper**

```
$ docker run -it --name Zookeeper -p 2181:2181 -p 2888:2888 -p 3888:3888 -d debezium/zookeeper:0.5
```

**Start Kafka**

```
$ docker run -it --name Kafka -p 9092:9092 \
				-e ZOOKEEPER_CONNECT=Zookeeper:2181 \
				--link Zookeeper:Zookeeper -d debezium/kafka:0.5
```

**Start Kafka Connect**

```
$ docker run -it --name Connect -p 8083:8083 \
				-e BOOTSTRAP_SERVERS=Kafka:9092 \
				-e CONFIG_STORAGE_TOPIC=my_connect_configs \
				-e OFFSET_STORAGE_TOPIC=my_connect_offsets \
				--link Zookeeper:Zookeeper --link Kafka:Kafka --link Postgres:Postgres \
				-d debezium/connect:0.5
```

### 1.2 Monitor the Postgres database

We will use `curl` to submit to Kafka Connect service a JSON request message with information about the connector we want to start.

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

### 1.3 Create table in Postgres and Greenplum separately

In Postgres

```
create table target (id int primary key, name varchar(10));
```

In Greenplum

```
create table source (id int primary key, name varchar(10));
```

## 2 Config bireme and start

### 2.1 Edit etc/config.properties

Specify connection infomation to the target database, as well as data source information.

```
target.url = jdbc:postgresql://gpdbhost:5432/XXXXXX
target.user = XXXXXX
target.passwd = XXXXXX

data_source = debezium

debezium.type = debezium
debezium.kafka.server = kafkahost:9092
```
**Note:** The value of data_source must be identical to the `database.server.name` configuration in Section 1.2 and we don't need to designate a topic.

### 2.2 Edit table mapping file

As we appoint *debezium* as data source name, we need to create a file *debezium.properties* in *etc* directory and insert the following.

```
public.source = public.target
```

### 2.3 Start bireme

Reference the same step in Part 1.

