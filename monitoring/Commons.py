import sqlite3

def CreatePerfDB(db_filename):
    conn = sqlite3.connect(db_filename)
    conn.execute('''CREATE TABLE PERFPROF
         (NAME INT TEXT KEY     NOT NULL,
         ID           TEXT    NOT NULL,
         TIMESTAMP      REAL     NOT NULL,
         FIELD        TEXT      NOT NULL,
         VALUE          REAL    NOT NULL);''')
    conn.close()