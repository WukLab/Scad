program=$1
# Check if the program cpp file exists
if [ ! -f "${program}.cpp" ]; then
    echo "Error: ${program}.cpp does not exist"
    exit 1
fi

make ${program}_annotated.out

# Check if the program executable exists
if [ ! -f "${program}_annotated.out" ]; then
    echo "Error: ${program}_annotated.out does not exist after make. Did you forget to add the make target?"
    exit 1
fi

python cpuutil.py ./${program}_annotated.out >| ${program}.cpu_trace.csv
mv -f child.log ${program}.stage_trace.csv
python cpualign.py ${program}.cpu_trace.csv ${program}.stage_trace.csv >| tee ${program}.cpu_trace_aligned.csv