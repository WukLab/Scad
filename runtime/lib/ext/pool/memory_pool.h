#ifndef _MEMORY_POOL_H_
#define _MEMORY_POOL_H_

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "transports/rdma_uverbs.h"

#define MPOOL_MSG_SIZE_LIMIT (4096)

enum {
    // SERVER_POOL/MR operations
    MPOOL_ALLOC = 0,
    MPOOL_FREE,
    MPOOL_EXTEND,

    // QP operations
    MPOOL_OPEN = 0x10,
    MPOOL_CLOSE,
};

enum {
    MPOOL_STATUS_OK,
    MPOOL_STATUS_OOM,
    MPOOL_STATUS_NEXIST,
    MPOOL_STATUS_RDMA
};

// Protocol
// alloc<ALLOC> <-> select<id, status, selfinfo>
// alloc<FREE> <-> select<id, status>
// alloc<EXTEND> <-> select<id, status, mrinfo>
// select<OPEN, id, peerinfo> <-> select<id, status>
// normally we do not close this.
// select<CLOSE, id> <-> select<id>

typedef struct darray {
    size_t len;
    size_t elsize;
    char * data;
} darray;

static inline darray* darray_new(size_t elsize, size_t n) {
    darray * arr = (darray *)calloc(1, sizeof(darray));
    arr->elsize = elsize;
    if (n)
        arr->data = (char *)calloc(n, elsize);
    return arr;
}

static inline void darray_set_size(darray* arr, int size) {
    size_t nsize = size * arr->elsize;
    arr->data = realloc(arr->data, nsize);
    memset(arr->data + arr->len * arr->elsize, arr->data + nsize, 0);
    arr->len = size;
}

static inline void * darray_index(darray* arr, int n) {
    return (void *)(arr->data + n * arr->elsize);
}

static inline void darray_free(darray* arr) {
    free(arr->data);
    free(arr);
}

struct mp_select {
    union {
        uint64_t status;
        uint64_t size;
    };
    uint16_t op_code;
    uint16_t msg_size;
    int16_t id;
    int16_t conn_id;
    uint8_t msg[0];
} __attribute__((packed));

struct mp_element {
    uint16_t id;
    darray /* struct rdma_conns */ * conns;
};

#define MPOOL_DATA_SIZE_LIMIT (4096 - sizeof(struct mp_select))

#endif
