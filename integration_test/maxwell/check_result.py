import sys
import os
sys.path.append(os.environ["TEST_DIR"])
from sqldump import sqldump

with open('source.txt', 'w') as f:
    table = sqldump("mysql","127.0.0.1", 3306, "root", "123456", "demo", "binarysource", "id")
    f.write(table)
    table = sqldump("mysql","127.0.0.1", 3306, "root", "123456", "demo", "charsource", "id")
    f.write(table)
    table = sqldump("mysql","127.0.0.1", 3306, "root", "123456", "demo", "numericsource", "aid", "bid")
    f.write(table)
    table = sqldump("mysql","127.0.0.1", 3306, "root", "123456", "demo", "timesource", "id")
    f.write(table)

with open('target.txt', 'w') as f:
    table = sqldump("postgres","127.0.0.1", 5432, "postgres", "postgres", "postgres", "binarytarget", "id")
    f.write(table)
    table = sqldump("postgres","127.0.0.1", 5432, "postgres", "postgres", "postgres", "chartarget", "id")
    f.write(table)
    table = sqldump("postgres","127.0.0.1", 5432, "postgres", "postgres", "postgres", "numerictarget", "aid", "bid")
    f.write(table)
    table = sqldump("postgres","127.0.0.1", 5432, "postgres", "postgres", "postgres", "timetarget", "id")
    f.write(table)

with open("source.txt") as source:
    with open("target.txt") as target:
        sourceContent = source.read()
        targetContent = target.read()
        if sourceContent == targetContent:
            print "Source:\n" + sourceContent
            print "Target:\n" + targetContent
            exit(0)
        else:
            print "Source:\n" + sourceContent
            print "Target:\n" + targetContent
            exit(1)