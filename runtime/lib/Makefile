TRANSPORTS      := ./transports
TRANSPORTS_SRCS := $(wildcard $(TRANSPORTS)/*.c)
TRANSPORTS_OBJS := $(patsubst $(TRANSPORTS)/%.c,$(TRANSPORTS)/%.o,$(TRANSPORTS_SRCS))

INTERFACES      := ./interfaces
INTERFACES_SRCS := $(wildcard $(INTERFACES)/*.c)
INTERFACES_OBJS := $(patsubst $(INTERFACES)/%.c,$(INTERFACES)/%.o,$(INTERFACES_SRCS))

PLUGINS      := ./plugins
PLUGINS_SRCS := $(wildcard $(PLUGINS)/*.c)
PLUGINS_OBJS := $(patsubst $(PLUGINS)/%.c,$(PLUGINS)/%.o,$(PLUGINS_SRCS))

DYN_OBJS   := $(TRANSPORTS_OBJS) $(INTERFACES_OBJS) $(PLUGINS_OBJS)
UTIL_OBJS  := map.o durl.o
OBJECTS    := libd.o libd_plugin.o libd_plugins.o libd_transport.o libd_transports.o $(UTIL_OBJS) $(DYN_OBJS)
TARGETS    := libd.so

LD       := gcc
INCLUDES := -I./include
LIBS     := -lnanomsg -lcurl -lpthread -libverbs
CFLAGS   := -fPIC -O2 -g -std=c11 -Wall $(INCLUDES) -DDEBUG
LDFLAGS  := $(LDFLAGS) $(LIBS)

.PNONY: all install clean ext_python ext_npjoin ext_npjoin_install

install: libd.so
	cp libd.so /usr/lib

all: $(TARGETS)

libd.so: $(OBJECTS)
	$(LD) -shared -o $@ $(LDFLAGS) $^

# Extensions.
# Python interface module using cython
ext_python: libd.so
	python ext/python/setup.py build_ext -i

ext_npjoin:
	python ext/np_join/setup.py build_ext -i
ext_npjoin_install: ext_npjoin
	pip install ext/np_join

memory_server: ext/host/memory_server.o $(OBJECTS)
	$(LD) -o $@ $^ $(LDFLAGS)

clean:
	rm $(OBJECTS) $(TARGETS) *.so
	make -C test clean

