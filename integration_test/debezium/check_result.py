import sys
sys.path.append("../")
from sqldump import sqldump

with open('source.txt', 'w') as f:
	table = sqldump("postgres","127.0.0.1", 5432, "postgres", "postgres", "postgres", "binarysource", "id")
	f.write(table)
	table = sqldump("postgres","127.0.0.1", 5432, "postgres", "postgres", "postgres", "charsource", "id")
	f.write(table)
	table = sqldump("postgres","127.0.0.1", 5432, "postgres", "postgres", "postgres", "numericsource", "id")
	f.write(table)
	table = sqldump("postgres","127.0.0.1", 5432, "postgres", "postgres", "postgres", "timesource", "id")
	f.write(table)
	
with open('target.txt', 'w') as f:
	table = sqldump("postgres","127.0.0.1", 5432, "postgres", "postgres", "postgres", "binarytarget", "id")
	f.write(table)
	table = sqldump("postgres","127.0.0.1", 5432, "postgres", "postgres", "postgres", "chartarget", "id")
	f.write(table)
	table = sqldump("postgres","127.0.0.1", 5432, "postgres", "postgres", "postgres", "numerictarget", "id")
	f.write(table)
	table = sqldump("postgres","127.0.0.1", 5432, "postgres", "postgres", "postgres", "timetarget", "id")
	f.write(table)
	
with open("source.txt") as source:
   with open("target.txt") as target:
      if source.read() == target.read():
      	exit(0)
      else:
      	exit(1)