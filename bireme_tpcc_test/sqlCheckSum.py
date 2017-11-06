import hashlib
import MySQLdb
import psycopg2
import binascii
import re

def sqlCheckSum(dbtype, host, port, user, passwd, db, table, *key):
    try:
        connection = getConnection(dbtype, host, port, user, passwd, db)
        dbhandler = connection.cursor()
        nameAndType = getNameAndType(dbhandler, dbtype, table)
        nameAndType.sort()

        names = [line[0] for line in nameAndType]
        types = [line[1] for line in nameAndType]

        dbhandler.execute("SELECT " + ", ".join(names) + " FROM " + table + " order by " + ", ".join(key))
        
        if dbtype.lower() == "mysql":
            return mysqlCheckSum(dbhandler, types)
        elif dbtype.lower() == "postgres":
            return pgCheckSum(dbhandler, types)

    except Exception as e:
        print e
    finally:
        connection.close()

def getConnection(dbtype, host, port, user, passwd, db):
    if dbtype.lower() == "mysql":
        return MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db)
    else:
        return psycopg2.connect(host=host, port=port, user=user, password=passwd, dbname=db)

def getNameAndType(dbhandler, dbtype, table):
    dbhandler.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='" + table + "' ORDER BY column_name")
    return [line for line in dbhandler.fetchall()]

def mysqlCheckSum(dbhandler, types):
    length = range(0, len(types))
    checksum = hashlib.md5()

    while True:
        line = dbhandler.fetchone()
        if not line:
            break;
 
        for i in length:
            if line[i] is None:
                checksum.update(str(line[i]) + "\t")
            elif re.search("binary.*|bit.*|blob.*", types[i]):
                checksum.update(str(binascii.hexlify(line[i])) + "\t")
            elif re.search("decimal", types[i]):
                checksum.update(str(line[i].normalize()) + "\t")
            else:
                checksum.update(str(line[i]) + "\t")

    return checksum.hexdigest()

def pgCheckSum(dbhandler, types):
    length = range(0, len(types))
    checksum = hashlib.md5()

    while True:
        line = dbhandler.fetchone()
        if not line:
            break;

        for i in length:
            if line[i] is None:
                checksum.update(str(line[i]) + "\t")
            elif re.search("bytea", types[i]):
                checksum.update(str(binascii.hexlify(line[i])) + "\t")
            elif re.search("bit", types[i]):
                checksum.update(str(hex(int(line[i], 2))[2:]) + "\t")
            elif re.search("boolean", types[i]):
                checksum.update("1\t" if line[i] else "0\t")
            elif re.search("numeric", types[i]):
                checksum.update(str(line[i].normalize()) + "\t")
            else:
                checksum.update(str(line[i]) + "\t")

    return checksum.hexdigest()