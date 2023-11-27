from datetime import datetime
import psutil
import os
import sys
import subprocess
import signal
from time import sleep

argv = sys.argv
# command = ["sudo"] + argv[1:]
command = argv[1:]
# Use Subprocess to spin up another process and get the pid of the process
# Mute the subprocess and set the nice value using preexec_fn
nice_value = 10  # Set your desired nice value here
proc = subprocess.Popen(
    command, 
    stdout=subprocess.DEVNULL, 
    stderr=subprocess.DEVNULL,
)
pid = proc.pid

# print(f'Process ID: {pid}')
py = psutil.Process(pid)
while True:
    if proc.poll() is not None:
        break
    # if not psutil.Process(pid).is_running():
    #     print('Process has ended')
    #     break
    cpuUse = py.cpu_percent()
    now_datetime = datetime.now()
    print(f'{now_datetime} CPU Usage: {cpuUse}%')
    sleep(0.5)
proc.kill()