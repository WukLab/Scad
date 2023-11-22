PYTHON_CONFIG := python3-config
CFLAGS  := $(CFLAGS) $(shell python3-config --cflags)
CFLAGS  := $(CFLAGS) -I$(shell python3 -c "import numpy as np; print(np.get_include());")
LDFLAGS := -lpython3.8 $(shell python3-config --ldflags)

.PHONY: all clean

all: tpcds_1

clean:
	rm reference.o tpcds_1 profiler.o tpcds_1_profiler

reference.o: reference.c
	gcc -c $(CFLAGS) -g -o $@ -fPIC $^

tpcds_1: reference.o
	gcc $^ -o $@ $(LDFLAGS)

profiler.o: profiler.cpp
	g++ -c $(CPPFLAGS) -g -o $@ -fPIC $^

tpcds_1_profiler: profiler.o reference.o
	g++ $^ -o $@ $(LDFLAGS) -Wl,-eprofiler_main -ltcmalloc
