#!/usr/bin/env python3

# Copyright (c) 2021 Princeton University
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from datetime import datetime
import json
from optparse import OptionParser
import os
import pickle
import sqlite3
import sys
import time

CONTAINER_INFO_FETCH_PERIOD = 0.5   # in seconds
CONTAINER_SAMPLING_PERIOD = 0.1     # in seconds
TIMESTAMP_DECIMAL_PRECISION = 6     # 1 us

def GetContainers(latest_known_cont_id=None):
    ## this call is expensive, n=1 => ~45ms, each additional container reported takes ~1ms
    if latest_known_cont_id is None:  
        # return all containers if no history is known
        stream = os.popen("docker ps --no-trunc --format '{{ .ID }},{{ .Names }}'")
    else:
        # only check for new containers since the latest known 
        # on average this is much faster than the the if branch
        stream = os.popen("docker ps --filter 'since="+latest_known_cont_id+"' --no-trunc --format '{{ .ID }},{{ .Names }}'")
    output = stream.read().split('\n')
    container_info = {'ID':[], 'NAME':[]}
    for i in range(len(output)):
        line = output[i]
        line_parsed = line.split(',')
        if len(line_parsed)<2:
            continue
        else:
            container_info['ID'].append( line_parsed[0] )
            container_info['NAME'].append( line_parsed[1] )

    return container_info


def GetContainerMemoryUsageInBytes(container_id, ref_time=0):
    ## each call benchmarked to take ~170us
    try:
        with open("/sys/fs/cgroup/memory/docker/"+container_id+"/memory.usage_in_bytes", "r") as f:
            value = f.read()
            return [round(time.time()-ref_time, TIMESTAMP_DECIMAL_PRECISION), int(value[:-1])]   # since this is accumulated CPU time, a timestamp should be returned too
    except:
        print('Could not read the cgroup info. Incorrect container_id or dead container.')
        return [round(time.time()-ref_time, TIMESTAMP_DECIMAL_PRECISION), 0]


def GetContainerCPUAcct(container_id, ref_time=0):
    ## each call benchmarked to take ~170us
    try:
        with open("/sys/fs/cgroup/cpu,cpuacct/docker/"+container_id+"/cpuacct.usage", "r") as f:
            value = f.read()
            return [round(time.time()-ref_time, TIMESTAMP_DECIMAL_PRECISION), int(value[:-1])]   # since this is accumulated CPU time, a timestamp should be returned too
    except:
        print('Could not read the cgroup info. Incorrect container_id or dead container.')
        return [round(time.time()-ref_time, TIMESTAMP_DECIMAL_PRECISION), 0]


def MeasureProfilingOverhead(func, measurement_count=10):
    delay = []
    for i in range(measurement_count):
        if i==0:
            start = time.time()
            c = func()
            end = time.time()
        else:
            if len(c['ID'])>0:
                latest_known_cont_id = c['ID'][0]
            start = time.time()
            c = func(latest_known_cont_id=latest_known_cont_id)
            end = time.time()
        delay.append(end-start)
        print(delay[-1])
        # print(value)
        time.sleep(0.25)
    print('Average run time (s):' + str(1.0*sum(delay)/len(delay)))


def RunProfilingLoop(main_start_time, monitoring_duration, db_file=None):
    last_container_info_time = 0
    profiling_data = {}
    known_containers = []
    container_id_to_name_mapping = {}
    latest_known_cont_id = None

    if db_file is not None:
        try:
            conn = sqlite3.connect(db_file)     # open connection to the sqlite DB if provided
        except:
            print("Error: Issues connecting to the DB! [DB name provided: " + db_file +" ]")
            return False

    while True:
        cur_time = time.time()
        if (cur_time - main_start_time) > monitoring_duration:
            break
        if (cur_time - last_container_info_time) > CONTAINER_INFO_FETCH_PERIOD:
            if latest_known_cont_id is None:
                container_info = GetContainers()        # update the container information
            else:
                container_info = GetContainers(latest_known_cont_id=latest_known_cont_id)
            last_container_info_time = time.time()
            if len(container_info['ID'])>0:
                latest_known_cont_id = container_info['ID'][0]
                print('latest_known_cont_id: ' + str(latest_known_cont_id))
            if db_file is not None:
                conn.commit()
        for i in range(len(container_info['ID'])):
            container_id = container_info['ID'][i]
            if container_id not in known_containers:
                known_containers.append( container_id )
                profiling_data[container_id] = {'MemoryUsageInBytes':{'timestamps':[],'vals':[]}, 'ContainerCPUAcct':{'timestamps':[],'vals':[]}}
                container_id_to_name_mapping[container_id] = container_info['NAME'][i]
        for container_id in known_containers:   # conduct measurements for last-known running containers
            # Memory
            [t1, v1] = GetContainerMemoryUsageInBytes(container_id=container_id, ref_time=main_start_time)
            # CPU
            [t2, v2] = GetContainerCPUAcct(container_id=container_id, ref_time=main_start_time)
            if db_file is None:
                profiling_data[container_id]['MemoryUsageInBytes']['timestamps'].append( t1 )
                profiling_data[container_id]['MemoryUsageInBytes']['vals'].append( v1 )
                profiling_data[container_id]['ContainerCPUAcct']['timestamps'].append( t2 )
                profiling_data[container_id]['ContainerCPUAcct']['vals'].append( v2 )
            else:
                conn.execute("INSERT INTO PERFPROF (NAME,ID,TIMESTAMP,FIELD,VALUE) \
                              VALUES ('"+container_id_to_name_mapping[container_id]+"', '"+container_id+"', "+str(t1)+", 'MemUsageBytes', "+str(v1)+" )")
                conn.execute("INSERT INTO PERFPROF (NAME,ID,TIMESTAMP,FIELD,VALUE) \
                              VALUES ('"+container_id_to_name_mapping[container_id]+"', '"+container_id+"', "+str(t2)+", 'CPUAcct', "+str(v2)+" )")

        time.sleep(CONTAINER_SAMPLING_PERIOD)
    
    if db_file is not None:
        conn.commit()
        conn.close()
    
    return profiling_data


def main(argv):
    """
    The main function.
    """
    main_start_time = time.time()   # log the start time

    parser = OptionParser()
    parser.add_option("-d", "--duration", dest="duration",
                      help="Measurement duration in seconds", type="int")
    (options, args) = parser.parse_args()

    if not options.duration:
        print("Error: You should provide measurement duration!")
        return False

    conn = sqlite3.connect('profiling.db')
    conn.execute('''CREATE TABLE PERFPROF
         (NAME INT TEXT KEY     NOT NULL,
         ID           TEXT    NOT NULL,
         TIMESTAMP      REAL     NOT NULL,
         FIELD        TEXT      NOT NULL,
         VALUE          REAL    NOT NULL);''')
    conn.close()
    time.sleep(0.5)

    # conn = sqlite3.connect('profiling.db')
    # container_id = '10ac9ee49fd3647ebc22fb8b5b58b4c6a0a3778029a14748f8228daf75936316'
    # [t, v] = GetContainerMemoryUsageInBytes(container_id=container_id)
    # field = "MemUsageBytes"
    # print([t,v])
    # conn.execute("INSERT INTO PERFPROF (NAME,ID,TIMESTAMP,FIELD,VALUE) \
    #     VALUES ('Controller', '"+container_id+"', "+str(t)+", '"+field+"', "+str(v)+" )")
    # conn.commit()
    # conn.close()


    ## Actual Profiling
    profiling_data = RunProfilingLoop(main_start_time=main_start_time, 
                                      monitoring_duration=options.duration,
                                      db_file='profiling.db')
    # print( json.dumps(profiling_data, indent=4) )

    ## Testing
    # MeasureProfilingOverhead(GetContainers, measurement_count=25)


if __name__ == "__main__":
    main(sys.argv)