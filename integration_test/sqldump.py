import MySQLdb
import MySQLdb.cursors
import psycopg2
import psycopg2.extras
import binascii
import re
from cStringIO import StringIO

def sqldump(dbtype, host, port, user, passwd, db, table, *key):
    try:
        nameAndType = getNameAndType(dbtype, host, port, user, passwd, db, table)
        nameAndType.sort()

        if dbtype.lower() == "postgres":
            names = ["\"" + line[0] + "\"" for line in nameAndType]
        elif dbtype.lower() == "mysql":
            names = [line[0] for line in nameAndType]
            
        types = [line[1] for line in nameAndType]

        dbhandler = getHandler(dbtype, host, port, user, passwd, db)
        dbhandler.execute("SELECT " + ", ".join(names) + " FROM " + table + " order by " + ", ".join(key))
        
        if dbtype.lower() == "mysql":
            return mysqlDump(dbhandler, types)
        elif dbtype.lower() == "postgres":
            return pgDump(dbhandler, types)

    except Exception as e:
        print e


def getHandler(dbtype, host, port, user, passwd, db):
    if dbtype.lower() == "mysql":
        conn = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db, cursorclass=MySQLdb.cursors.SSCursor)
        return conn.cursor()
    else:
        conn = psycopg2.connect(host=host, port=port, user=user, password=passwd, dbname=db)
        return conn.cursor('my_cursor', cursor_factory=psycopg2.extras.DictCursor)

def getNameAndType(dbtype, host, port, user, passwd, db, table):
    if dbtype.lower() == "mysql":
        conn = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db, cursorclass=MySQLdb.cursors.SSCursor)
    else:
        conn = psycopg2.connect(host=host, port=port, user=user, password=passwd, dbname=db)

    dbhandler = conn.cursor()
    dbhandler.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='" + table + "' ORDER BY column_name")
    return [line for line in dbhandler.fetchall()]

def mysqlDump(dbhandler, types):
    length = range(0, len(types))
    fileStr = StringIO()

    for line in dbhandler:
        if not line:
            break;

        for i in length:
            if line[i] is None:
                fileStr.write(str(line[i]) + "\t")
            elif re.search("binary.*|bit.*|blob.*", types[i]):
                fileStr.write(str(binascii.hexlify(line[i])) + "\t")
            elif re.search("decimal", types[i]):
                fileStr.write(str(line[i].normalize()) + "\t")
            else:
                fileStr.write(str(line[i]) + "\t")
        fileStr.write("\n")

    return fileStr.getvalue()

def pgDump(dbhandler, types):
    length = range(0, len(types))
    fileStr = StringIO()

    for line in dbhandler:
        if not line:
            break;

        for i in length:
            if line[i] is None:
                fileStr.write(str(line[i]) + "\t")
            elif re.search("bytea", types[i]):
                fileStr.write(str(binascii.hexlify(line[i])) + "\t")
            elif re.search("bit", types[i]):
                fileStr.write(str(hex(int(line[i], 2))[2:]) + "\t")
            elif re.search("boolean", types[i]):
                fileStr.write("1\t" if line[i] else "0\t")
            elif re.search("numeric", types[i]):
                fileStr.write(str(line[i].normalize()) + "\t")
            else:
                fileStr.write(str(line[i]) + "\t")
        fileStr.write("\n")

    return fileStr.getvalue()