#include <stdio.h>

// domain socket
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "libd.h"
#include "memory_pool.h"
#include "transports/rdma_uverbs.h"

static struct ibv_pd      *_pd      = NULL;
static struct ibv_context *_context = NULL;

#define GID     (0)
#define PORT    (1)
#define CQ_SIZE (16)

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
    conn->gid  = GID;
    conn->port = PORT;
    conn->cq   = ibv_create_cq(_context, CQ_SIZE, NULL, NULL, 0);
    conn->num_mr = NULL
        conn->mr     = NULL

    if (old != NULL) {
        conn->num_mr = old->num_mr;
        conn->mr     = old->mr;
    }
}


int main(int argc, char *argv[]) {
    char * socketpath = argv[1];
    int fd, bytes, info_bytes, cur_size;
    char buf[MPOOL_MSG_SIZE_LIMIT];

    struct mp_select *mselect;

    /* struct mp_element */ GArray * elements; 
    struct mp_element *melement;
    struct rdma_conn *conn;

    if ((fd = socket_setup(socketpath)) < 0)
        return fd;

    // setup global RDMA connections
    // TODO: error checking
    int num_devices;
    char * device_name;
    _context = create_context(num_devices, device_name);
    _pd = ibv_alloc_pd(_context);

    elements = g_array_new(FALSE, TRUE, sizeof(struct mp_element));

    // main serve loop
    for (;;) {
        mselect = (struct mp_select *)buf;
        bytes = recv(fd, buf, sizeof(struct mp_select), 0);
        if (bytes < 0)
            return -1;

        if (mselect->msg_size > 0) 
            bytes = recv(fd, buf + sizeof(struct mp_select),
                            mselect->msg_size, 0);

        switch (mselect->op_code) {
            case MPOOL_ALLOC:
                // allocate or get new element
                if (mselect->id < 0) {
                    mselect->id = elements->len;
                    g_array_set_size(elements, mselect->id + 1);
                }

                cur_size = mselect->conn_id;
                melement = &g_array_index(elements,
                              struct mp_element, cur_size);
                melement->conns =
                    g_array_sized_new(FALSE, TRUE, struct rdma_conns,
                                      cur_size);

                for (int i = 0; i < cur_size; i++) {
                    conn = &g_array_index(elements->conns,
                                  struct rdma_conn, i);

                    // create or copy mr info
                    if (i == 0) {
                        conn_setup(conn, NULL);
                        create_mr(conn, malloc->size, MR_ACCESS, NULL);
                    } else
                        conn_setup(conn, &g_array_index(elements->conns,
                                  struct rdma_conn, 0));
                        
                    
                    // assamble message and reply
                    mselect-> size =
                        extract_info_inplace(conn, &mp_select->msg,
                                            MPOOL_DATA_SIZE_LIMIT);
                    mselect->status = MPOOL_STATUS_OK;
                    mselect->conn_id = i;

                    send(fd, buf,
                        mselect->size + sizeof(struct mpool_select), 0);
                }
                break;
            case MPOOL_FREE:
                melement = &g_array_index(elements,
                              struct mp_element, mselect->id);
                if (melement == NULL) {
                    mselect->status = MPOOL_STATUS_NEXIST;
                    send(fd, buf, sizeof(struct mpool_select), 0);
                    break;
                }

                // free the pool
                // free MR
                conn = &g_array_index(melement->conns, struct rdma_conn,
                                      0);
                if (conn != NULL) {
                    for (int i = 0; i < conn->num_mr; i++) {
                        void * buf = rstate->conn->mr[i].addr;
                        ibv_dereg_mr(rstate->conn->mr + i);
                        free(buf);
                    }
                }

                // free memory inside conn
                for (int i = 0; i < melement->conns->size; i++) {
                    conn = &g_array_index(melement->conns,
                            struct rdma_conn, i);
                    ibv_destroy_qp(conn->qp);
                    ibv_destroy_cq(conn->cq);
                }

                // free garray
                g_array_free(melement->conn, TRUE);
                // but do not free melement, may change the index
                break;

            case MPOOL_EXTEND:
                // TODO: update old_mr
                // send new MR
                break;

            case MPOOL_OPEN: // open a new conn for new connection
                melement = &g_array_index(elements,
                              struct mp_element, mselect->id);

                // TODO: keep this?
                if (mselect->conn_id < 0) {
                    // <0 will allocate new connection
                    mselect->conn_id = melement->conns->len;
                    g_array_set_size(elements, mselect->conn_id + 1);
                }
                conn = &g_array_index(melement->conns, struct rdma_conn,
                                      mselect->conn_id);
                conn->peerinfo = mselect->msg;

                // post send msg
                qp_stm_reset_to_init(conn);
                qp_stm_init_to_rtr(conn);

                // reply msg
                // we can safely change the code, since peerinfo is useless
                mselect->status = MPOOL_STATUS_OK;
                send(fd, buf, sizeof(struct mpool_select), 0);
                break;
            case MPOOL_CLOSE:
                goto cleanup;
                break;
        }

    }

cleanup:
    unlink(socketpath);
    ibv_close_device(_context);
}
