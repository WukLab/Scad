import sqlite3

def CreatePerfDB(db_filename):
    conn = sqlite3.connect(db_filename)
    conn.execute('''CREATE TABLE PERFPROF
         (NAME INT TEXT KEY     NOT NULL,
         POINT           TEXT    NOT NULL,
         FIELD        TEXT      NOT NULL,
         VALUE          REAL    NOT NULL,
         LOGTIME      TEXT     NOT NULL);''')
    conn.close()