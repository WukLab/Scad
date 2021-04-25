#!/usr/bin/env python3

import os
import sqlite3
import time

from config import *

if (os.path.isfile(PERF_DB_FILENAME)):
    conn = sqlite3.connect(PERF_DB_FILENAME)
    s = time.time()
    cursor = conn.execute("SELECT NAME, ID, TIMESTAMP, FIELD, VALUE from PERFPROF")
    e = time.time()
    print('Query execution time (s): ' + str(e - s) + '\n')
    for row in cursor:
        print(str(row) + "\n")
    conn.close()
else:
    print('Error: No profiling DB exists to be read!')