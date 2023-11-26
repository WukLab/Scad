python lifetime.py ../reference_mem.cpp --target_function main_ > tpcds.lifetime.csv
python memusage.py  ./tpcds_1.mem.log --source_path ../reference_mem.cpp --exec_path ../tpcds_1_mem > tpcds.line_activity.csv
python final.py --lifetime_csv_path tpcds.lifetime.csv --line_activity_csv_path tpcds.line_activity.csv