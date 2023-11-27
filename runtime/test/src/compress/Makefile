# Compile the simplemt program
CC=g++
CFLAGS=-g -Wall -Wextra -std=c++17 -O0
LDFLAGS=-lpthread

OBJECTS=simplemt.out zstdmt.out

.PHONY: all clean

all: $(OBJECTS)

simplemt.out: simplemt.cpp
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

zstdmt.out: zstdmt.cpp
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) -lzstd -pthread

clean: 
	rm -f *.o *.out