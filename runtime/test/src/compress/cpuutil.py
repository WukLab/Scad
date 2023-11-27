from datetime import datetime
import psutil
import os
import sys
import subprocess
import signal
from time import sleep, time_ns

def main(child_log_file):
    argv = sys.argv
    # command = ["sudo"] + argv[1:]
    command = argv[1:]
    # Use Subprocess to spin up another process and get the pid of the process
    # Mute the subprocess and set the nice value using preexec_fn
    nice_value = 10  # Set your desired nice value here
    proc = subprocess.Popen(
        command, 
        stdout=subprocess.DEVNULL, 
        stderr=child_log_file,
    )
    pid = proc.pid

    # print(f'Process ID: {pid}')
    py = psutil.Process(pid)
    print(f'st,ed,cpu')
    while True:
        if proc.poll() is not None:
            break
        # if not psutil.Process(pid).is_running():
        #     print('Process has ended')
        #     break
        st_now_datetime = time_ns()
        cpuUse = py.cpu_percent()
        ed_now_datetime = time_ns()
        print(f'{st_now_datetime},{ed_now_datetime},{cpuUse}')
        sleep(0.5)

    proc.kill()

if __name__ == '__main__':
    with open('child.log', 'w+') as child_log_file:
        main(child_log_file)