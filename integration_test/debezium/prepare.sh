set -xeu

PREPARE_DATA=$(cat $SOURCE_DIR/setup.sql)
CONNECTOR_CONFIG='
{
    "name": "inventory-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "postgres",
        "database.server.name": "hashdata"
    }
}'

$DOCKER_RUN --name Zookeeper -p 2181:2181 -p 2888:2888 -p 3888:3888 -d debezium/zookeeper:0.5
$DOCKER_RUN --name Kafka -p 9092:9092 \
            -e ZOOKEEPER_CONNECT=Zookeeper:2181 \
            --link Zookeeper:Zookeeper -d debezium/kafka:0.5
$DOCKER_RUN --name Postgres -p 5432:5432 -d debezium/postgres:latest
$DOCKER_RUN --name Connect -p 8083:8083 \
            -e BOOTSTRAP_SERVERS=Kafka:9092 \
            -e CONFIG_STORAGE_TOPIC=my_connect_configs \
            -e OFFSET_STORAGE_TOPIC=my_connect_offsets \
            --link Zookeeper:Zookeeper --link Kafka:Kafka --link Postgres:Postgres \
            -d debezium/connect:0.5

until $(docker logs Connect | grep -q "Finished starting connectors and tasks")
do
    sleep 1
done

curl -i -X POST -H "Accept:application/json" \
                -H "Content-Type:application/json" localhost:8083/connectors/ \
                -d "${CONNECTOR_CONFIG}"

$DOCKER_EXEC -u postgres Postgres psql -c "${PREPARE_DATA}"