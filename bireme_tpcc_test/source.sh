set -xe

DOCKER_RUN='docker run -it'
DOCKER_EXEC='docker exec -it'
MYSQL_EXEC=$DOCKER_EXEC' MySQL mysql -uroot -p123456'

if [ -z "$WAREHOUSES" ]
then
    WAREHOUSES=5
fi

if [ -z "$CONNECTIONS" ]
then
    CONNECTIONS=5
fi

if [ -z "$WARMUP_TIME" ]
then
    WARMUP_TIME=60
fi

if [ -z "$BENCHMARK_TIME" ]
then
    BENCHMARK_TIME=1800
fi

if [ -z "$MYSQL_VOLUME" ]
then
    MYSQL_VOLUME=/var/lib/mysql
fi

if [ -z "$KAFKA_SERVER" ]
then
    echo '"KAFKA_SERVER" is not set.' >&2
    exit 1
fi


mysql_setup() {
    $DOCKER_RUN --name MySQL -p 3306:3306 \
            -v ${MYSQL_VOLUME}:/var/lib/mysql
            -e MYSQL_ROOT_PASSWORD=123456 \
            -d mysql:latest \
            --server-id=1 --log-bin=master --binlog_format=row

    until $(docker logs MySQL | grep -q "Server hostname (bind-address)")
    do
        sleep 2
    done

    $MYSQL_EXEC -e "GRANT ALL on maxwell.* to 'maxwell'@'%' identified by '123456';"
    $MYSQL_EXEC -e "GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE on *.* to 'maxwell'@'%';"
    $MYSQL_EXEC -e "create database demo;"
}

maxwell_setup() {
    $DOCKER_RUN --name Maxwell \
            --link MySQL:MySQL \
            -d zendesk/maxwell \
            bin/maxwell --user=maxwell --password=123456 \
            --producer_partition_by=table \
            --host=MySQL --producer=kafka \
            --kafka.bootstrap.servers=${KAFKA_SERVER}:9092

    until $(docker logs Maxwell | grep -q "Connected to MySQL")
    do
        sleep 2
    done
}

start_tpch() {
    $MYSQL_EXEC demo -e "$($DOCKER_RUN --rm gaishimo/tpcc-mysql cat create_table.sql add_fkey_idx.sql)"

    $DOCKER_RUN --rm --link MySQL:mysql gaishimo/tpcc-mysql tpcc_load demo root '123456' $WAREHOUSES

    $DOCKER_RUN --rm --link MySQL:mysql gaishimo/tpcc-mysql tpcc_start -d demo -u root -p '123456' -w $WAREHOUSES -c $CONNECTIONS -r $WARMUP_TIME -l $BENCHMARK_TIME
}

mysql_setup

maxwell_setup

start_tpch

