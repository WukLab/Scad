TARGETS    := client server

LD       := gcc
INCLUDES := -I../include
CFLAGS   := -O2 -g -Wall $(INCLUDES)
LDFLAGS  := $(LDFLAGS) -ld

.PNONY: all install clean

all: $(TARGETS)

client: client.o
	$(LD) -o $@ $(LDFLAGS) $^

server: server.o
	$(LD) -o $@ $(LDFLAGS) $^

clean:
	rm *.o $(TARGETS)
