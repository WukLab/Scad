TARGETS    := client_uverbs

LD       := gcc
INCLUDES := -I../include
CFLAGS   := -O2 -g -Wall $(INCLUDES)
LDFLAGS  := $(LDFLAGS) -ld -lnanomsg -libverbs -lcurl

.PNONY: all install clean

all: $(TARGETS)

client_uverbs: client_uverbs.o
	$(LD) -o $@ $^ $(LDFLAGS)

clean:
	rm *.o $(TARGETS)
