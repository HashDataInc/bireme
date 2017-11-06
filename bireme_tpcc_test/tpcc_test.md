# Bireme tpcc 测试

### Kafka 的准备
在 kafka 新建一个 topic，命名为 maxwell，可以根据实际情况指定 partition 的个数。

### 数据源的准备

我们用 docker 启动数据源

* **安装 docker 及下载镜像**

```
yum install docker
service docker start
docker pull mysql
docker pull zendesk/maxwell
docker pull gaishimo/tpcc-mysql
```

* **修改测试脚本配置**

修改测试用的环境变量

|配置项|解释|默认值|
|:---:|:---:|:---:|
|KAFKA_SERVER|Kafka 的 IP 地址|必须指定|
|WAREHOUSES|tpcc 测试的 warehouses 数量|5|
|CONNECTIONS|tpcc 测试的连接数|5|
|WARMUP_TIME|tpcc 测试的预热时间，单位秒|60|
|BANCHMARK_TIME|tpcc 测试的执行时间，单位秒|1800|
|MYSQL_VOLUME|mysql 存放数据路径|/var/lib/mysql|

* **执行测试脚本**

```
./source.sh
```

### HashData 的准备

* **建表和索引**

执行 create\_table\_index.sql 文件

### 启动 Bireme

参考[README.md](../README.md)

### 数据一致性检验

sqlCheckSum 函数可以查询表的全部内容，做统一处理，并计算md5值。通过比较源端和目标端表的md5值，检验是否一致。

|参数|解释|
|:---:|:---:|
|dbtype|数据库类型，mysql 或者是 postgres|
|host|数据库IP地址|
|port|数据库端口号|
|user|数据库用户名|
|passwd|数据库密码|
|db|数据库名称|
|table|数据库表名|
|key|主键，可以是多个参数|