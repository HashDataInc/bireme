import MySQLdb
import psycopg2
import re
from cStringIO import StringIO

def sqldump(dbtype, host, port, user, passwd, db, table, *key):
    try:
        connection = getconnection(dbtype, host, port, user, passwd, db)
        dbhandler = connection.cursor()
        types = gettypes(dbhandler, dbtype, table)
        dbhandler.execute("SELECT * FROM "+table +" order by "+" ".join(key))
        tuples = dbhandler.fetchall()
    except Exception as e:
        print e
    finally:
        connection.close()

    if dbtype.lower() == "mysql":
        return mysqldump(tuples, types)
    elif dbtype.lower() == "postgres":
        return pgdump(tuples, types)

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

def mysqldump(tuples, types):
    length = range(0, len(types))
    fileStr = StringIO()

    for line in tuples:
        for i in length:
            if re.search("binary.*|bit.*|blob.*|text.*", types[i]):
                fileStr.write(str(binascii.hexlify(line[i]))+"\t")
            else:
                fileStr.write(str(line[i])+"\t")
        fileStr.write("\n")
    return fileStr.getvalue()

def pgdump(tuples, types):
    length = range(0, len(types))
    fileStr = StringIO()

    for line in tuples:
        for i in length:
            if re.search("bytea", types[i]):
                fileStr.write(str(binascii.hexlify(line[i]))+"\t")
            elif re.search("bit", types[i]):
                fileStr.write(str(int(line[i], 2))+"\t")
            elif re.search("boolean", types[i]):
                fileStr.write("1" if line[i] else "0")
            else:
                fileStr.write(str(line[i])+"\t")
        fileStr.write("\n")
    return fileStr.getvalue()