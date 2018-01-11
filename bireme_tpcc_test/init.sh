#!/usr/bin/env bash

set -ex

MYSQL=192.168.0.251
USER=hashdata
PW=hashdata

echo "create database tpcc;" | mysql -h $MYSQL -u $USER -p $PW
mysql  -h $MYSQL -u $USER -p $PW tpcc <./create_table.sql
mysql -h $MYSQL -u $USER -p $PW tpcc <./add_fkey_idx.sql
