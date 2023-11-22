PYTHON_CONFIG := python3-config
CFLAGS  := $(CFLAGS) $(shell python3-config --cflags)
CFLAGS  := $(CFLAGS) -I$(shell python3 -c "import numpy as np; print(np.get_include());")
CFLAGS  := $(CFLAGS) -g -pg -O0 -std=c++17
LDFLAGS := -lpython3.8 $(shell python3-config --ldflags) -ltcmalloc -lprofiler

.PHONY: all clean

all: tpcds_1

clean:
	rm reference.o tpcds_1

reference.o: reference.cpp
	g++ -c $(CFLAGS) -g -o $@ -fPIC $^

tpcds_1: reference.o
	g++ $^ -o $@ $(LDFLAGS)

