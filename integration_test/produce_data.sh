set -xeu

DOCKER_EXEC='docker exec -it'
SETUP='${SOURCE_DIR}/setup.sql'
CREATE_TABLE=`cat ${SETUP}`

create_table(){
    $DOCKER_EXEC -u postgres Postgres psql -c "${CREATE_TABLE}"
    
    #$DOCKER_EXEC -u postgres Postgres psql -c "create table target (id int primary key, one bytea, two bytea, three bytea);"
}

create_table