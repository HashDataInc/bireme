import MySQLdb
import psycopg2
import datetime
import binascii

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "123456"
MYSQL_DB = "demo"

PG_HOST = "localhost"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_DB = "postgres"


try:
    mysqlConnection = MySQLdb.connect(host=MYSQL_HOST, port=MYSQL_PORT,
    	user=MYSQL_USER, passwd=MYSQL_PASSWORD, db=MYSQL_DB)

    dbhandler = mysqlConnection.cursor()
    
    dbhandler.execute("SELECT * FROM binarysource order by id;")
    binarySource = dbhandler.fetchall()
    dbhandler.execute("SELECT * FROM charsource order by id;")
    charSource = dbhandler.fetchall()
    dbhandler.execute("SELECT * FROM numericsource order by id;")
    numericSource = dbhandler.fetchall()
    dbhandler.execute("SELECT * FROM timesource order by id;")
    timeSource = dbhandler.fetchall()

except Exception as e:
    print e

finally:
    mysqlConnection.close()

try:
    pgConnection = psycopg2.connect(host=PG_HOST, port=PG_PORT,
                               user=PG_USER, password=PG_PASSWORD, dbname=PG_DB)

    dbhandler = pgConnection.cursor()
    
    dbhandler.execute("SELECT * FROM binarytarget order by id;")
    binaryTarget = dbhandler.fetchall()
    dbhandler.execute("SELECT * FROM chartarget order by id;")
    charTarget = dbhandler.fetchall()
    dbhandler.execute("SELECT * FROM numerictarget order by id;")
    numericTarget = dbhandler.fetchall()
    dbhandler.execute("SELECT * FROM timetarget order by id;")
    timeTarget = dbhandler.fetchall()

except Exception as e:
    print e

finally:
    pgConnection.close()

with open('source.txt', 'w') as f:
	for binary in binarySource:
		f.write(binascii.hexlify(binary[1])+"\t")
		f.write(str(binary[2]==1)+"\t")
		f.write(str(int(binascii.hexlify(binary[3]), 16))+"\t\n")

	for char in charSource:
		f.write(char[1]+"\t"+char[2]+"\t"+char[3]+"\t\n")

	for numeric in numericSource:
		f.write(str(numeric[1])+"\t"+str(numeric[2])+"\t\n")

	for time in timeSource:
		f.write(str(time[1])+"\t")
		f.write(str((datetime.datetime.min+time[2]).time())+"\t\n")

with open('target.txt', 'w') as f:
	for binary in binaryTarget:
		f.write(binascii.hexlify(binary[1])+"\t")
		f.write(str(binary[2])+"\t")
		f.write(str(int(binary[3], 2))+"\t")
		f.write("\n")

	for char in charTarget:
		f.write(char[1]+"\t"+char[2]+"\t"+char[3]+"\t\n")

	for numeric in numericTarget:
		f.write(str(numeric[1])+"\t"+str(numeric[2])+"\t\n")

	for time in timeTarget:
		f.write(str(time[1])+"\t"+str(time[2])+"\t\n")

with open("source.txt") as source:
   with open("target.txt") as target:
      if source.read() == target.read():
      	exit(0)
      else:
      	exit(1)