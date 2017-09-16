#!/usr/bin/env bash

set -xeu

host="localhost"
port=5432
username=postgres

docker_exec='docker exec -it'

create_table(){
    echo "*********** begin to create table ***********"
    $docker_exec Postgres psql -h ${host} -p ${port} -U ${username} -c "create table source (id int primary key, one bytea, two bytea, three bytea);"
    
    $docker_exec Postgres psql -h ${host} -p ${port} -U ${username} -c "create table target (id int primary key, one bytea, two bytea, three bytea);"
    echo "*********** tables were created ***********"
}

insert_data(){
	$docker_exec Postgres psql -h ${host} -p ${port} -U ${username} -c "insert into source values(3, decode('1A1B1C3D1F', 'hex'), decode('ABCDEF1234', 'hex'), decode('12345678900A0B', 'hex')); "
    $docker_exec Postgres psql -h ${host} -p ${port} -U ${username} -c "insert into source values(4, decode('1A1B1C3D1F', 'hex'), decode('ABCDEF1234', 'hex'), decode('12345678900A0B', 'hex')); "
}

check_result(){
    rm -f output.txt
    $docker_exec Postgres psql -h ${host} -p ${port} -U ${username} -c "select * from source order by id" >> standard.txt

    $docker_exec Postgres psql -h ${host} -p ${port} -U ${username} -c "select * from target order by id" >> output.txt

    if
        diff output.txt standard.txt
    then
        echo "successfully importing data "
    else
        echo " Error importing data "
    fi

    rm output.txt standard.txt
}

mvn docker:start

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{"name": "inventory-connector","config": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector","database.hostname": "postgres","database.port": "5432","database.user": "postgres","database.password": "postgres","database.dbname" : "postgres","database.server.name": "debezium1"}}'
sleep 20

create_table
sleep 10
insert_data

mvn clean package
tar -xf target/bireme.tar.gz
rm -rf bireme/etc
cp -rf integration_test/etc bireme/

bireme/bin/bireme start
sleep 10
bireme/bin/bireme stop

check_result

rm -rf bireme
mvn docker:stop

