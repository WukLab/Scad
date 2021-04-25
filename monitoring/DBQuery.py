#!/usr/bin/env python3

import os
import sqlite3
import time

from config import *

if (os.path.isfile(PERF_DB_FILENAME)):
    conn = sqlite3.connect(PERF_DB_FILENAME)
    s = time.time()
    cursor = conn.execute("SELECT NAME, POINT, FIELD, VALUE, LOGTIME from PERFPROF")
    e = time.time()
    print('Query execution time (s): ' + str(e - s) + '\n')
    for row in cursor:
        print(str(row))
    conn.close()
else:
    print('Error: No profiling DB exists to be read!')