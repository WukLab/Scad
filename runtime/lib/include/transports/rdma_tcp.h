#ifndef _RDMA_TCP_H
#define _RDMA_TCP_H

#define TCP_RDMA_OP_NOP (0)
#define TCP_RDMA_OP_REP (1)
#define TCP_RDMA_OP_WRITE (2)
#define TCP_RDMA_OP_READ (3)
#define TCP_RDMA_OP_ALLOC (4)

struct tcp_rdma_cmd {
    uint64_t addr;
    uint64_t size;
    uint16_t seq;
    uint8_t status;
    uint8_t op;
    uint8_t data[0];
};

struct tcp_rdma_state {
    struct libd_tstate tstate;

    int sock;
    int sock_initd;
    int size;
    char * url;
    void * mr;
};

#endif

