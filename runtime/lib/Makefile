TRANSPORTS      := ./transports
TRANSPORTS_SRCS := $(wildcard $(TRANSPORTS)/*.c)
TRANSPORTS_OBJS := $(patsubst $(TRANSPORTS)/%.c,$(TRANSPORTS)/%.o,$(TRANSPORTS_SRCS))

INTERFACES      := ./interfaces
INTERFACES_SRCS := $(wildcard $(INTERFACES)/*.c)
INTERFACES_OBJS := $(patsubst $(INTERFACES)/%.c,$(INTERFACES)/%.o,$(INTERFACES_SRCS))

UTIL_OBJS  := map.o durl.o
OBJECTS    := libd.o libd_transport.o libd_transports.o $(UTIL_OBJS) $(TRANSPORTS_OBJS) $(INTERFACES_OBJS)
TARGETS    := libd.so

LD       := gcc
INCLUDES := -I./include
CFLAGS   := -fPIC -O2 -g -std=c11 -Wall $(INCLUDES) -DDEBUG
LDFLAGS  := $(LDFLAGS) -lnanomsg

.PNONY: all install clean

install: libd.so
	cp libd.so /usr/lib

all: $(TARGETS)

libd.so: $(OBJECTS)
	$(LD) -shared -o $@ $(LDFLAGS) $^

clean:
	rm $(OBJECTS) $(TARGETS)
	make -C test clean
