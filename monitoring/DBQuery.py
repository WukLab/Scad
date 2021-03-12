import sqlite3
import time

conn = sqlite3.connect('profiling.db')

s = time.time()
cursor = conn.execute("SELECT NAME, ID, TIMESTAMP, FIELD, VALUE from PERFPROF")
e = time.time()
print('Query execution time (s): ' + str(e - s) + '\n')

for row in cursor:
    print(str(row) + "\n")

conn.close()