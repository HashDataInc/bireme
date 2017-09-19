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
        "database.server.name": "debezium_CI"
    }
}'

curl -i -X POST -H "Accept:application/json" \
                -H "Content-Type:application/json" localhost:8083/connectors/ \
                -d "${CONNECTOR_CONFIG}"

$DOCKER_EXEC -u postgres Postgres psql -c "${PREPARE_DATA}"