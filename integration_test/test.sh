set -xeu

DOCKER_EXEC='docker exec -it'
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

examine_result() {
	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from numericsource order by id;" >> source.txt
	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from charsource order by id;" >> source.txt
	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from timesource order by id;" >> source.txt
	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from binarysource order by id;" >> source.txt

	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from numerictarget order by id;" >> target.txt
	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from chartarget order by id;" >> target.txt
	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from timetarget order by id;" >> target.txt
	$DOCKER_EXEC -u postgres Postgres psql -t -c "select * from binarytarget order by id;" >> target.txt

	if [[ -z `diff source.txt target.txt` ]]; then
		echo "Data are identical!"
		DIFF=0
	else
		echo "Data are different!"
		DIFF=1
	fi
}

curl -i -X POST -H "Accept:application/json" \
                -H "Content-Type:application/json" localhost:8083/connectors/ \
                -d "${CONNECTOR_CONFIG}"

$DOCKER_EXEC -u postgres Postgres psql -c "${PREPARE_DATA}"

${BIREME}/bin/bireme start
sleep 20
${BIREME}/bin/bireme stop

rm -f source.txt target.txt
touch source.txt target.txt
examine_result
rm -f source.txt target.txt

exit ${DIFF}
