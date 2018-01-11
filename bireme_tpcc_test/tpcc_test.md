# Bireme tpcc 测试

### tpcc client 的准备

* **tpcc 主机准备**

  * 在 QingCloud 上启动主机，选择配置 4core2G

  * 下载并编译 tpcc-mysql
```
git clone https://github.com/Percona-Lab/tpcc-mysql.git
cd tpcc-mysql/src
make
```

* **启动 mysql**

在 QingCloud 上启动 MySQL Plus 服务，选择配置 8core16G * 3 nodes, 100G/node


* **修改脚本配置**

将 bireme_tpcc_test 目录下的 init.sh 和 parallel_load.sh 脚本复制到 tpcc-mysql 目录下

|配置项|解释|默认值|
|:---:|:---:|:---:|
|MYSQL|MySQL 的 IP 地址|必须指定|
|USER|MySQL 的用户名|必须指定|
|PW|MySQL 的密码|必须指定|
|WH|Warehouse 数量|500|
|STEP|每个并发线程负责加载的 Warehouse 数量|100|

* **执行脚本**

```
./init.sh
./parallel_load.sh
```

更多关于 tpcc-mysql 的配置与使用，参考 [tpcc-mysql](https://www.hi-linux.com/posts/38534.html)。

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
