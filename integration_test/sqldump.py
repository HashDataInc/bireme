import MySQLdb
import psycopg2
import binascii
import re
from cStringIO import StringIO

def sqldump(dbtype, host, port, user, passwd, db, table, *key):
    try:
        connection = getconnection(dbtype, host, port, user, passwd, db)
        dbhandler = connection.cursor()
        types = gettypes(dbhandler, dbtype, table)
        dbhandler.execute("SELECT * FROM "+table +" order by "+" ".join(key))
        
        if dbtype.lower() == "mysql":
            return mysqldump(dbhandler, types)
        elif dbtype.lower() == "postgres":
            return pgdump(dbhandler, types)

    except Exception as e:
        print e
    finally:
        connection.close()

def getconnection(dbtype, host, port, user, passwd, db):
    if dbtype.lower() == "mysql":
        return MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db)
    else:
        return psycopg2.connect(host=host, port=port, user=user, password=passwd, dbname=db)

def gettypes(dbhandler, dbtype, table):
    if dbtype.lower() == "mysql":
        dbhandler.execute("desc "+table)
        return [line[1] for line in dbhandler.fetchall()]
    elif dbtype.lower() == "postgres":
        dbhandler.execute("SELECT data_type FROM information_schema.columns WHERE table_name='"+table+"' ORDER BY ordinal_position")
        return [line[0] for line in dbhandler.fetchall()]

def mysqldump(dbhandler, types):
    length = range(0, len(types))
    fileStr = StringIO()

    while True:
        line = dbhandler.fetchone()
        if not line:
            break;

        for i in length:
            if line[i] is None:
                fileStr.write(str(line[i])+"\t")
            elif re.search("binary.*|bit.*|blob.*", types[i]):
                fileStr.write(str(binascii.hexlify(line[i]))+"\t")
            else:
                fileStr.write(str(line[i])+"\t")
        fileStr.write("\n")

    return fileStr.getvalue()

def pgdump(dbhandler, types):
    length = range(0, len(types))
    fileStr = StringIO()

    while True:
        line = dbhandler.fetchone()
        if not line:
            break;

        for i in length:
            if line[i] is None:
                fileStr.write(str(line[i])+"\t")
            elif re.search("bytea", types[i]):
                fileStr.write(str(binascii.hexlify(line[i]))+"\t")
            elif re.search("bit", types[i]):
                fileStr.write(str(hex(int(line[i], 2))[2:])+"\t")
            elif re.search("boolean", types[i]):
                fileStr.write("1\t" if line[i] else "0\t")
            else:
                fileStr.write(str(line[i])+"\t")
        fileStr.write("\n")

    return fileStr.getvalue()