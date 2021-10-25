#ifndef _MEMORY_POOL_H_
#define _MEMORY_POOL_H_

#include <stdint.h>
#include <glib.h>
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


struct mp_select {
    uint64_t size;
    uint16_t op_code;
    union {
        uint16_t status;
        uint16_t msg_size;
    };
    int16_t id;
    int16_t conn_id;
    uint8_t msg[0];
} __attribute__((packed));

struct mp_element {
    GArray /* struct rdma_conns */ * conns;
};

#define MPOOL_DATA_SIZE_LIMIT (4096 - sizeof(struct mp_select))

#endif
