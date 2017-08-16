#!/usr/bin/env bash

set -eu

#if you want to pass parameters from command-line,you have to give five parameters
db_cnt=2     #default is 2,or $1
int_size=10   #default is 10,or $2
char_size=10  #default is 10,or $3

threads=10     #default is 10,or $4
queries=1000  #default is 1000,or $5

load_type='write'
int_col='avg(intcol'
char_col='charcol'
select_content=''
docker_exec='docker exec -it'
docker_detach='docker exec -itd'
select_content=''

export HOSTNAME=$(hostname)

check_init(){
  while !($docker_exec MySQL mysql -uroot -p123456 -e "show databases" > /dev/null)
  do
    sleep 2s
  done
  echo "*********** mysql init done ***********"
}

init(){

  echo "*********** Before you test, make sure that you have configured the IP address in the YML file correctly ***********"

  echo "*********** now we are trying to do docker-compose up ***********"
  docker-compose up -d
  check_init
  echo "*********** docker-compose up down ***********"
}


load(){
  echo "*********** now we are trying to do load data ***********"
  for loop in $(seq 1 $db_cnt)
  do
    $docker_exec MySQL mysql -uroot -p123456 -e "drop database if exists test_load_$loop;"

    $docker_detach MySQL mysqlslap -uroot -p123456 -a --auto-generate-sql-load-type=$load_type \
        --auto-generate-sql-guid-primary --number-char-cols=$char_size \
        --number-int-cols=$int_size --create-schema=test_load_$loop \
        -c $threads --number-of-queries=$queries --no-drop
  done

}

check_result(){
  echo "*********** check if data load ended ***********"
  while ($docker_exec MySQL ps aux | grep --quiet mysqlslap)
  do
    sleep 0.5s
  done
  echo "*********** data load ended ***********"

  echo "*********** now we are trying to check data ***********"
  for loop in $(seq 1 $int_size)
  do
    select_content=${select_content}${int_col}${loop}'),'
  done

  sql_sen="select ${select_content%?} from t1"

  for loop in $(seq 1 $db_cnt)
  do
    $docker_exec MySQL mysql -uroot -p123456 test_load_${loop} -e "${sql_sen}"
  done
}

clean(){
  echo "*********** clean all these containers ***********"
  docker-compose down -v
  echo "*********** containers are cleaned ***********"
}

while getopts "d:i:c:t:q:" arg #选项后面的冒号表示该选项需要参数
  do
  case $arg in
    d)
      db_cnt=$OPTARG #参数存在$OPTARG中
      ;;
    i)
      int_size=$OPTARG
      ;;
    c)
      char_size=$OPTARG
      ;;
    t)
      threads=$OPTARG
      ;;
    q)
      queries=$OPTARG
      ;;
    ?)  #当有不认识的选项的时候arg为?
      echo "./mysql_test.sh [options]"
      echo "-d number of databases"
      echo "-i number of int columes"
      echo "-c number of char columes"
      echo "-t number of threads connect to a db"
      echo "-q number of total queries"
      exit 1
      ;;
  esac
done

clean
init
load
check_result
