#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "libd.h"
#include "libd_transport.h"
#include "interfaces/libd_trdma.h"
#include "transports/rdma_local.h"

// TODO: change the order of init.
// interface implementations
static int _init(struct libd_transport *trans) {
    int ret;
    init_config_for(trans, struct local_rdma_state);
    get_local_state(rstate,trans,struct local_rdma_state);

    init_config_require(size, config_to_ull);
    ret = rstate->mem = malloc(rstate->size);

    return ret;
}

static int _connect(struct libd_transport *trans) {
    return 0;
}

static int _terminate(struct libd_transport * trans) {
    int ret;
    get_local_state(rstate,trans,struct local_rdma_state);
    dprintf("calling terminate with %d size", rstate->size);

    ret = free(rstate->mem);

    return 0;
}

// actions
static void * _reg(struct libd_transport *trans, size_t s, void *buf) {
    get_local_state(rstate,trans,struct local_rdma_state);

    if (buf == NULL) {
        buf = malloc(s);
    }

    return buf;
}

static int _read(struct libd_transport *trans,
                size_t size, uint64_t addr, void * buf) {
    get_local_state(rstate,trans,struct local_rdma_state);
    return memcpy(buf, rstate->mem + addr, size);
}

static int _write(struct libd_transport *trans,
                  size_t size, uint64_t addr, void * buf) {
    get_local_state(rstate,trans,struct local_rdma_state);
    return memcpy(rstate->mem + addr, buf, size);
}

// Async calls unimplemented
static int _async_write() {
    get_local_state(rstate,trans,struct local_rdma_state);
    return -1;
}
static int _async_read() {
    get_local_state(rstate,trans,struct local_rdma_state);
    return -1;
}
static int _async_poll(struct libd_transport * trans, int id) {
    get_local_state(rstate,trans,struct local_rdma_state);
    return -1;
}

// export struct
struct libd_trdma rdma_local = {
    .trans = {
        .init = _init,
        .terminate = _terminate,
        .connect = _connect
    },

    .reg = _reg,
    .read = _read,
    .write = _write,
    .write_async = _write_async,
    .read_async = _read_async,
    .poll_async = _poll_async
};

