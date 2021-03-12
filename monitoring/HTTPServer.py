from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO
import sqlite3

db_file = 'profiling.db'

def ParseMessageBody(body):
    conn = sqlite3.connect(db_file)

    for field in body.split('&'):
        [k,v] = field.split('=')
        if k=='object':
            object_name = v
        else:
            vals = v.split(',')[1:]
            while len(vals) > 0:
                name, rx_bytes, tx_bytes = vals
                vals = vals[3:]
                conn.execute("INSERT INTO PERFPROF (NAME,ID,TIMESTAMP,FIELD,VALUE) \
                              VALUES ('"+object_name+"', '', "+str(k)+", 'RXBytes-"+name+"', "+rx_bytes+" )")
                conn.execute("INSERT INTO PERFPROF (NAME,ID,TIMESTAMP,FIELD,VALUE) \
                              VALUES ('"+object_name+"', '', "+str(k)+", 'TXBytes-"+name+"', "+tx_bytes+" )")
    
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