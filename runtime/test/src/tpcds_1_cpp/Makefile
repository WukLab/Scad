PYTHON_CONFIG := python3-config
# CFLAGS  := $(CFLAGS) -O3
CFLAGS  := $(CFLAGS) $(shell python3-config --cflags)
CFLAGS  := $(CFLAGS) -I$(shell python3 -c "import numpy as np; print(np.get_include());")
CFLAGS  := $(CFLAGS) -g -pg -O0 -std=c++17
LDFLAGS := -lpython3.8 $(shell python3-config --ldflags) 

.PHONY: all clean

all: tpcds_1 tpcds_1_mem tpcds_1_cpu

clean:
	rm *.o tpcds_1 tpcds_1_mem tpcds_1_cpu

reference.o: reference.c
	g++ -c $(CFLAGS) -g -o $@ -fPIC $^

reference_cpu.o: reference_cpu.cpp
	g++ -c $(CFLAGS) -g -o $@ -fPIC $^

reference_mem.o: reference_mem.cpp
	g++ -c $(CFLAGS) -g -o $@ -fPIC $^

tpcds_1: reference.o
	g++ $^ -o $@ $(LDFLAGS)

tpcds_1_mem: reference_mem.o
	g++ $^ -o $@ $(LDFLAGS) -ltcmalloc -lprofiler

tpcds_1_cpu: reference_cpu.o
	g++ $^ -o $@ $(LDFLAGS)

