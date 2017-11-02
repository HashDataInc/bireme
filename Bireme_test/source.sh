set -xeu

WAREHOUSES=1
CONNECTIONS=5
WARMUP_TIME=60
BANCHMARK_TIME=300
KAFKA_SERVER=192.168.110.16

DOCKER_RUN='docker run -it'
DOCKER_EXEC='docker exec -it'
MYSQL_EXEC=$DOCKER_EXEC' MySQL mysql -uroot -p123456'

$DOCKER_RUN --name MySQL -p 3306:3306 \
            -e MYSQL_ROOT_PASSWORD=123456 \
            -d mysql:latest \
            --server-id=1 --log-bin=master --binlog_format=row

until $(docker logs MySQL | grep -q "Server hostname (bind-address)")
do
    sleep 2
done

$MYSQL_EXEC -e "GRANT ALL on maxwell.* to 'maxwell'@'%' identified by '123456';"
$MYSQL_EXEC -e "GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE on *.* to 'maxwell'@'%';"

$DOCKER_RUN --name Maxwell \
            --link MySQL:MySQL \
            -d zendesk/maxwell \
            bin/maxwell --user=root --password=123456 \
            --producer_partition_by=table \
            --host=MySQL --producer=kafka \
            --kafka.bootstrap.servers=${KAFKA_SERVER}:9092

until $(docker logs Maxwell | grep -q "Connected to MySQL")
do
    sleep 2
done

$MYSQL_EXEC -e "create database demo;"

$MYSQL_EXEC demo -e "$($DOCKER_RUN --rm gaishimo/tpcc-mysql cat create_table.sql add_fkey_idx.sql)"

$DOCKER_RUN --rm --link MySQL:mysql gaishimo/tpcc-mysql tpcc_load demo root '123456' $WAREHOUSES

$DOCKER_RUN --rm --link MySQL:mysql gaishimo/tpcc-mysql tpcc_start -d demo -u root -p '123456' -w $WAREHOUSES -c $CONNECTIONS -r $WARMUP_TIME -l $BANCHMARK_TIME

