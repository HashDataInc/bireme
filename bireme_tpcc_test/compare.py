import os
import sys

from sqlCheckSum import sqlCheckSum

try:
    table = sys.argv[2]
    key = sys.argv[3:]

    if sys.argv[1] == "mysql":
        mysqlIP = os.environ['MYSQL_IP']
        mysqlPort = 3306
        mysqlUser = os.environ['MYSQL_USER']
        mysqlPasswd = os.environ['MYSQL_PASSWD']
        mysqlDB = os.environ['MYSQL_DB']
        print "MD5 for table " + table + " in MySQL is " + sqlCheckSum("mysql", mysqlIP, mysqlPort, mysqlUser, mysqlPasswd, mysqlDB, table, *key)

    elif sys.argv[1] == "hashdata":
        pgIP = os.environ['PG_IP']
        pgPort = 5432
        pgUser = os.environ['PG_USER']
        pgPasswd = os.environ['PG_PASSWD']
        pgDB = os.environ['PG_DB']
        print "MD5 for table " + table + " in Hashdata is " + sqlCheckSum("postgres", pgIP, pgPort, pgUser, pgPasswd, pgDB, table, *key)

except Exception as e:
    if str(type(e)) == "<type 'exceptions.KeyError'>":
        print "Environment variable " + str(e) + " is not found."
    else:
        print e
