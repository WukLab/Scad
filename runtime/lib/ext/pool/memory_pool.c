#include <stdio.h>
#include <errno.h>

// domain socket
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "libd.h"
#include "memory_pool.h"
#include "transports/rdma_uverbs.h"

static struct ibv_pd      *_pd      = NULL;
static struct ibv_context *_context = NULL;

#define CQ_SIZE (16)
#define SOCKET_FILE "/tmp/memorypool.sock"

#define MR_ACCESS (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ)

// we only have single connection (from local invoker)
int socket_setup(char * const socketpath) {
    int fd, len, ret;
    struct sockaddr_un  un;

    memset(&un, 0, sizeof(un));
    un.sun_family = AF_UNIX;
    strcpy(un.sun_path, socketpath);
    len = offsetof(struct sockaddr_un, sun_path) + strlen(socketpath);

    // unlink socket if exists
    unlink(socketpath);

    // create a UNIX domain datagram socket
    if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
        return fd;

    if ((ret = bind(fd, (struct sockaddr *)&un, len)) < 0) 
        return ret;

    return fd;
}

// set the conn ready to use
void conn_setup(struct rdma_conn * conn, struct rdma_conn * old) {
    conn->context = _context;
    conn->pd = _pd;

    // parameters ..
    conn->gid  = RDMA_GID;
    conn->port = RDMA_PORT;
    conn->cq   = ibv_create_cq(_context, CQ_SIZE, NULL, NULL, 0);
    create_qp(conn);

    conn->num_mr = 0;
    conn->mr     = NULL;

    if (old != NULL) {
        conn->num_mr = old->num_mr;
        conn->mr     = old->mr;
    }
}

static inline struct mp_element * get_by_id(GArray * elements, uint16_t id) {
    struct mp_element * melement;
    for (int i = 0; i < elements->len; i++) {
        melement = &g_array_index(elements,
                      struct mp_element, i);
        if (melement->id == id)
            return melement;
    }
    return NULL;
}


int main(int argc, char *argv[]) {
    char * socketpath;
    int num_devices = 2;
    char * device_name;

    int ret;
    int sfd, fd, bytes, cur_size;
    char buf[MPOOL_MSG_SIZE_LIMIT];

    struct sockaddr_un remote;
    socklen_t socklen;

    struct mp_select *mselect;

    /* struct mp_element */ GArray * elements; 
    struct mp_element *melement;
    struct rdma_conn *conn;

    socketpath = argc > 1 ? argv[1] : SOCKET_FILE;
    device_name = argc > 2 ? argv[2] : RDMA_DEVICE_NAME;

    dprintf("start memory pool on %s with file %s", device_name, socketpath);

    if ((sfd = socket_setup(socketpath)) < 0)
        return sfd;

    if ((ret = listen(sfd, 1)) < 0)
        return ret;

    socklen = sizeof(remote);
    if ((fd = accept(sfd, (struct sockaddr *)&remote, &socklen)) < 0) {
        dprintf("accept");
        return fd;
    }

    dprintf("bind socket to fd %d", fd);

    // setup global RDMA connections
    // TODO: error checking
    _context = create_context(num_devices, device_name);
    _pd = ibv_alloc_pd(_context);

    elements = g_array_new(FALSE, TRUE, sizeof(struct mp_element));

    // main serve loop
    for (;;) {
        dprintf("serving socket requests");
        mselect = (struct mp_select *)buf;
        bytes = recv(fd, buf, sizeof(struct mp_select), 0);
        dprintf("get request, with msg size %d", bytes);
        // TODO: = 0 means fd is closed
        if (bytes <= 0) {
            dprintf("recv %d error, %d %s", fd, bytes, strerror(errno));
            return -1;
        }

        if (mselect->msg_size > 0) 
            bytes = recv(fd, buf + sizeof(struct mp_select),
                            mselect->msg_size, 0);

        switch (mselect->op_code) {
            case MPOOL_ALLOC:
                dprintf("Get ALLOC, id %d, conn_id %d, size %lu",
                        mselect->id, mselect->conn_id, mselect->size);
                // allocates new elements
                melement = get_by_id(elements, mselect->id);
                if (melement == NULL) {
                    g_array_set_size(elements, elements->len + 1);
                    melement = &g_array_index(elements,
                                  struct mp_element, mselect->id);
                    melement->id = mselect->id;
                    melement->conns =
                        g_array_new(FALSE, TRUE, sizeof(struct rdma_conn));
                }

                // expend the size
                cur_size = melement->conns->len;
                g_array_set_size(melement->conns, cur_size + mselect->conn_id);

                // get more connections
                for (int i = cur_size; i < cur_size + mselect->conn_id; i++) {
                    conn = &g_array_index(melement->conns,
                                  struct rdma_conn, i);

                    // create or copy mr info
                    if (i == 0) {
                        conn_setup(conn, NULL);
                        create_mr(conn, mselect->size, MR_ACCESS, NULL);
                    } else
                        conn_setup(conn, &g_array_index(melement->conns,
                                  struct rdma_conn, 0));

                    dprintf("before dump info");
                    // assamble message and reply
                    // extract mr info to message
                    mselect->msg_size =
                        extract_info_inplace(conn, &mselect->msg,
                                            MPOOL_DATA_SIZE_LIMIT);
                    mselect->status = MPOOL_STATUS_OK;
                    mselect->conn_id = i;

                    dprintf("Reply ALLOC, eid %d connid %d msgSize %d",
                            mselect->id, mselect->conn_id, mselect->msg_size);

                    send(fd, buf,
                        mselect->msg_size + sizeof(struct mp_select), 0);
                }
                break;
            case MPOOL_FREE:
                melement = get_by_id(elements, mselect->id);
                if (melement == NULL) {
                    mselect->status = MPOOL_STATUS_NEXIST;
                    send(fd, buf, sizeof(struct mp_select), 0);
                    break;
                }

                // free the pool
                // free MR
                conn = &g_array_index(melement->conns, struct rdma_conn,
                                      0);
                if (conn != NULL) {
                    for (int i = 0; i < conn->num_mr; i++) {
                        void * buf = conn->mr[i].addr;
                        ibv_dereg_mr(conn->mr + i);
                        free(buf);
                    }
                }

                // free memory inside conn
                for (int i = 0; i < melement->conns->len; i++) {
                    conn = &g_array_index(melement->conns,
                            struct rdma_conn, i);
                    if (conn->qp)
                        ibv_destroy_qp(conn->qp);
                    if (conn->cq)
                        ibv_destroy_cq(conn->cq);
                }

                // free garray
                g_array_free(melement->conns, TRUE);
                // but do not free melement, may change the index
                break;

            case MPOOL_EXTEND:
                // TODO: update old_mr
                // send new MR
                break;

            case MPOOL_OPEN: // open a new conn for new connection
                dprintf("Get OPEN id %d connid %d", mselect->id, mselect->conn_id);
                melement = get_by_id(elements, mselect->id);
                if (melement == NULL) {
                    mselect->status = MPOOL_STATUS_NEXIST;
                    send(fd, buf, sizeof(struct mp_select), 0);
                    break;
                }

                conn = &g_array_index(melement->conns, struct rdma_conn,
                                      mselect->conn_id);
                conn->peerinfo = (struct conn_info *)mselect->msg;

                // post send msg
                qp_stm_reset_to_init(conn);
                qp_stm_init_to_rtr(conn);

                // reply msg
                // we can safely change the code, since peerinfo is useless
                mselect->status = MPOOL_STATUS_OK;
                mselect->msg_size = 0;
                send(fd, buf, sizeof(struct mp_select), 0);
                break;
            case MPOOL_CLOSE:
                goto cleanup;
                break;
            default:
                dprintf("Error: get Unkonw requests");
                break;
        }

    }

cleanup:
    unlink(socketpath);
    ibv_close_device(_context);
}
