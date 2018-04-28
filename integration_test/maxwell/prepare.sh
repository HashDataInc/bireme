set -xeu

MYSQL_EXEC=$DOCKER_EXEC' MySQL mysql -uroot -p123456'
MYSQL_SETUP=$(cat $SOURCE_DIR/mysql_setup.sql)
PG_SETUP=$(cat $SOURCE_DIR/pg_setup.sql)

$DOCKER_RUN --name MySQL -p 3306:3306 \
            -e MYSQL_ROOT_PASSWORD=123456 \
            -d mysql:5.7 \
            --server-id=1 --log-bin=master --binlog_format=row
$DOCKER_RUN --name Postgres -p 5432:5432 -d postgres:latest
$DOCKER_RUN --name Zookeeper -p 2181:2181 -d zookeeper:latest
$DOCKER_RUN --name Kafka -p 9092:9092 \
            -e ZOOKEEPER_CONNECT=Zookeeper:2181 \
            --link Zookeeper:Zookeeper -d debezium/kafka:0.5
while $($MYSQL_EXEC -e "select 1;" | grep -q "ERROR")
do
    sleep 1
done

$MYSQL_EXEC -e "GRANT ALL on maxwell.* to 'maxwell'@'%' identified by '123456';"
$MYSQL_EXEC -e "GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE on *.* to 'maxwell'@'%';"

$DOCKER_RUN --name Maxwell \
            --link MySQL:MySQL --link Kafka:Kafka \
            -d zendesk/maxwell \
            bin/maxwell --user=root --password=123456 \
            --host=MySQL --producer=kafka \
            --kafka.bootstrap.servers=Kafka:9092

until $(docker logs Maxwell | grep -q "Connected to MySQL")
do
    sleep 1
done

$MYSQL_EXEC -e "CREATE DATABASE demo;"
$MYSQL_EXEC demo -e "${MYSQL_SETUP}"

$DOCKER_EXEC -u postgres Postgres psql -c "${PG_SETUP}"