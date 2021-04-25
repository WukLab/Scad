#!/usr/bin/env python3

from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
import os
import sqlite3
import time

from Commons import CreatePerfDB
from config import *

def ParseMessageBody(body):
    current_time = time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime())

    if (not os.path.isfile(PERF_DB_FILENAME)):
        # create a DB file if not available
        CreatePerfDB(PERF_DB_FILENAME)
    conn = sqlite3.connect(PERF_DB_FILENAME)

    for field in body.split('&'):
        [k,v] = field.split('=')
        if k=='object':
            object_name = v
        else:
            point = k
            vals = v.split(',')[1:]
            if len(vals) >= 2:
                cpu, mem = vals[0], vals[1]
                vals = vals[2:]
                conn.execute("INSERT INTO PERFPROF (NAME,POINT,FIELD,VALUE,LOGTIME) \
                              VALUES ('"+object_name+"', '"+point+"', 'CPU', "+cpu+", '"+current_time+"' )")
                conn.execute("INSERT INTO PERFPROF (NAME,POINT,FIELD,VALUE,LOGTIME) \
                              VALUES ('"+object_name+"', '"+point+"', 'MEM', "+mem+", '"+current_time+"' )")
            while len(vals) > 0:
                name, rx_bytes, tx_bytes = vals[0:3]
                vals = vals[3:]
                conn.execute("INSERT INTO PERFPROF (NAME,POINT,FIELD,VALUE,LOGTIME) \
                              VALUES ('"+object_name+"', '"+point+"', 'RXBytes-"+name+"', "+rx_bytes+", '"+current_time+"' )")
                conn.execute("INSERT INTO PERFPROF (NAME,POINT,FIELD,VALUE,LOGTIME) \
                              VALUES ('"+object_name+"', '"+point+"', 'TXBytes-"+name+"', "+tx_bytes+", '"+current_time+"' )")
    
    conn.commit()
    conn.close()

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'Hello, world!')
    
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        body = self.rfile.read(content_length)
        ParseMessageBody(body.decode('ascii'))
        self.send_response(200)
        self.end_headers()
        response = BytesIO()
        response.write(b'Received')
        self.wfile.write(response.getvalue())

PORT = 8315
httpd = HTTPServer(('localhost', PORT), SimpleHTTPRequestHandler)
httpd.serve_forever()