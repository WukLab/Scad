#! /bin/bash
LOG_FILE_NAME="analysis/output/tpcds_1.$(date +"%Y%m%d_%H%M%S").log" 
LOG_FILE_NAME=${LOG_FILE_NAME} ./tpcds_1
# DISABLE_HOOK=true ./tpcds_1