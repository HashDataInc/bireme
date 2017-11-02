# Bireme 测试

## Kafka 的准备
在青云上部署 kafka 新建一个 topic，命名为 maxwell，可以根据实际情况指定 partition 的个数。

## 数据源的准备

### 安装 docker 及下载镜像

```
yum install docker
service docker start
docker pull mysql
docker pull zendesk/maxwell
docker pull gaishimo/tpcc-mysql
```
如果镜像下载较慢，可以编辑 /etc/docker/daemon.json 文件，并重新启动 docker

```
{
"registry-mirrors" : [
    "https://dr0aaazu.mirror.aliyuncs.com"
  ]
}
```

### 修改测试脚本配置
编辑 source.sh 文件，修改配置参数

|配置项|解释|
|:---:|:---:|
|KAFKA_SERVER|Kafka 的 IP 地址|
|WAREHOUSES|tpcc 测试的 warehouses 数量|
|CONNECTIONS|tpcc 测试的连接数|
|WARMUP_TIME|tpcc 测试的预热时间，单位秒|
|BANCHMARK_TIME|tpcc 测试的执行时间，单位秒|

### 执行测试脚本

```
./source.sh
```

## Bireme 的准备

### 安装 java

```
curl -O https://pek3a.qingstor.com/hashdata-public/tools/centos7/jdk-8u144-linux-x64.rpm
rpm -ivh jdk-8u144-linux-x64.rpm
```

### 设置 JAVA_HOME

```
export JAVA_HOME=/usr/java/jdk1.8.0_144/jre
```

### 启动 jstatd

```
policy=${HOME}/.jstatd.all.policy
[ -r ${policy} ] || cat >${policy} <<'POLICY'
grant codebase "file:${java.home}/../lib/tools.jar" {
permission java.security.AllPermission;
};
POLICY

jstatd -J-Djava.security.policy=${policy} &
```

**注：** 这里遇到一点问题，如果远程一直看不到 jstatd，可以尝试修改 /etc/hosts 文件，将 127.0.0.1 替换为当前主机的内网 IP

## HashData 的准备

### 建表和索引

执行 create\_table\_index.sql 文件