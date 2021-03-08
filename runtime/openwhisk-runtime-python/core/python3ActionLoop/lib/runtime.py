import os
import struct
import json
from disagg import LibdAction

# Libd action infomation, prepare for launch

def create_msg(cmd, params):
    content = json.dumps({ 'cmd': cmd, 'params': params }).encode('ascii')
    size = struct.pack("<I", len(content))
    return size + content

def fetch_msg(fifo):
    size = struct.unpack("<I", os.read(fifo, 4))[0]
    content = os.read(fifo, size).decode('ascii')
    return json.loads(content)

