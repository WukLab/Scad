#! /bin/bash

# Get the scale factor from the command line
SF=$1
if [ -z "$SF" ]; then
    echo "Usage: $0 <scale factor>"
    echo "Example: $0 1"
    echo "Example2: $0 5"
    exit 1
fi

# Prepare data
if [ ! -d "/data/tpcds_data_sf_${SF}" ]; then
    echo "Error: /data/tpcds_data_sf_${SF} does not exist."
    exit 1
fi

pid=$(pgrep -f "python -m http.server 8123")
if [ ! -z "$pid" ]; then
    echo "Python HTTP server is already running at port 8123: $pid"
    echo "Killing it ..."
    kill $pid
fi

cd /data/tpcds_data_sf_${SF}
# Spin up a Python HTTP server
python -m http.server 8123 &
echo "Python HTTP server is running at port 8123"


stem="tpcds_1.sf_${SF}"
mem_log_file="${stem}.mem.log"
lifetime_file="${stem}.lifetime.csv"
line_activity_file="${stem}.line_activity.csv"

cd /home/junda/Scad/runtime/test/src/tpcds_1_cpp
make clean
make
rm analysis_memory/${mem_log_file}
LOG_FILE_NAME="analysis_memory/${mem_log_file}" ./tpcds_1_mem

cd /home/junda/Scad/runtime/test/src/tpcds_1_cpp/analysis_memory
python lifetime.py ../reference_mem.cpp --target_function main_ >| ${lifetime_file}
python memusage.py  ${mem_log_file} --source_path ../reference_mem.cpp --exec_path ../tpcds_1_mem >| ${line_activity_file}
python final.py --lifetime_csv_path ${lifetime_file} --line_activity_csv_path ${line_activity_file} >| ${stem}.final.csv
python final.py --lifetime_csv_path ${lifetime_file} --line_activity_csv_path ${line_activity_file} --display_markdown

# Kill the Python HTTP server
pid=$(pgrep -f "python -m http.server 8123")
echo "Killing Python HTTP server at port 8123: $pid"
kill $pid
