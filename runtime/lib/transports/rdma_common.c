#include <string.h>
#include <infiniband/verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include "transports/rdma_uverbs.h"
#include "libd.h"

struct ibv_context *create_context(int _num_devices, const char *device_name) {
    int num_devices;
    struct ibv_context * context = NULL;
	struct ibv_device** device_list = ibv_get_device_list(&num_devices);
	for (int i = 0; i < num_devices; i++){
		dprintf("device %i, %s | %s\n", i, ibv_get_device_name(device_list[i]), device_name);
		if (strcmp(device_name, ibv_get_device_name(device_list[i])) == 0) {
			context = ibv_open_device(device_list[i]);
			break;
		}
	}

	ibv_free_device_list(device_list);

	return context;
}

int create_qp(struct rdma_conn *conn) {
    struct ibv_qp_init_attr qp_init;
    memset(&qp_init, 0, sizeof(qp_init));
    qp_init.qp_type = IBV_QPT_RC;
    qp_init.sq_sig_all = 1;       // always work completion
    qp_init.send_cq = conn->cq;         // completion queue for cq
    qp_init.recv_cq = conn->cq;         // completion queue for sq
    qp_init.cap.max_send_wr = 1; 
    qp_init.cap.max_recv_wr = 1;
    qp_init.cap.max_send_sge = 1;
    qp_init.cap.max_recv_sge = 1;
    // TODO: set other ATTRs

    conn->qp = ibv_create_qp(conn->pd, &qp_init);
    if (conn->qp == NULL) {
        dprintf("create qp fail\n");
        return -1;
    }
    return 0;
}

int create_mr(struct rdma_conn *conn, size_t size, int access,
              void * buffer) {
    struct ibv_mr *mr;

    if (buffer == NULL)
        buffer = malloc(size);

    if (buffer == NULL) {
        dprintf("malloc fail");
        return -1;
    }

    dprintf("buffer malloced");
    mr = ibv_reg_mr(conn->pd, buffer, size, access);
    if (mr == NULL) {
        dprintf("register MR fail");
        return -1;
    }

    conn->num_mr += 1;
    conn->mr = realloc(conn->mr, conn->num_mr * sizeof(struct ibv_mr));
    if (conn->mr == NULL) {
        dprintf("realloc fail\n");
        return -1;
    }

    memcpy(conn->mr + conn->num_mr - 1, mr, sizeof(struct ibv_mr));
    // printf("CREATE MR ... %d = %p\n", conn->num_mr, mr->addr);

    return 0;
}

int qp_stm_reset_to_init(struct rdma_conn *conn) {
    int ret;
    struct ibv_qp_attr qp_attr;

    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.qp_state = IBV_QPS_INIT;
    qp_attr.port_num = conn->port;
    qp_attr.pkey_index = 0;
    qp_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

    // TODO: match here with attrs
    ret = ibv_modify_qp(conn->qp, &qp_attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
    if (ret != 0) {
        dprintf("init fail %d\n", ret);
        return -1;
    }
    return 0;
}

int qp_stm_init_to_rtr(struct rdma_conn *conn) {
    int ret;
    struct ibv_qp_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));

    qp_attr.qp_state = IBV_QPS_RTR;
    qp_attr.path_mtu = IBV_MTU_1024;
    qp_attr.rq_psn = 0;
    qp_attr.max_dest_rd_atomic = 1;
    qp_attr.min_rnr_timer = 0x12;

    qp_attr.ah_attr.is_global = 0;
    qp_attr.ah_attr.sl = 0;
    qp_attr.ah_attr.src_path_bits = 0;
    qp_attr.ah_attr.port_num = conn->peerinfo->port;

    qp_attr.dest_qp_num = conn->peerinfo->qp_number;
    qp_attr.ah_attr.dlid = conn->peerinfo->local_id;

    if (conn->gid != RDMA_PROTOCOL_IB) {
        qp_attr.ah_attr.is_global = 1;
        qp_attr.ah_attr.grh.sgid_index = conn->gid;
        qp_attr.ah_attr.grh.dgid = conn->peerinfo->gid;
        qp_attr.ah_attr.grh.hop_limit = 0xFF;
        qp_attr.ah_attr.grh.traffic_class = 0;
    }

    ret = ibv_modify_qp(conn->qp, &qp_attr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);

    if (ret != 0) {
        dprintf("rtr fail %d, roce %d, %s", ret, conn->gid, strerror(errno));
        dprintf("  more info conn %p PORT %d QPN %d RGID %d LGID %d", conn, conn->peerinfo->port, conn->peerinfo->qp_number, conn->peerinfo->gid, conn->gid);
        return -1;
    }

    return 0;
}

int qp_stm_rtr_to_rts(struct rdma_conn *conn) {

    int ret;
    struct ibv_qp_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));

    qp_attr.qp_state = IBV_QPS_RTS;
    qp_attr.sq_psn = 0;
    qp_attr.timeout = 16; // See doc
    qp_attr.retry_cnt      = 7;
    qp_attr.rnr_retry      = 7; /* infinite */
    qp_attr.max_rd_atomic  = 1;

    ret = ibv_modify_qp(conn->qp, &qp_attr, IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC);
    if (ret != 0) {
        dprintf("rts fail\n");
        return -1;
    }

    return 0;
}

int extract_info(struct rdma_conn *conn, void **buf) {

    int ret;
    int info_size = sizeof(struct conn_info) + conn->num_mr * sizeof(struct ibv_mr);
    *buf = calloc(1, info_size);
    struct conn_info *info = (struct conn_info*) *buf;

    if (conn->gid != RDMA_PROTOCOL_IB) {
        union ibv_gid gids;
        ret = ibv_query_gid(conn->context, conn->port, conn->gid, &gids);
	if (ret != 0) dprintf("WRONG GID %d", ret);
        info->gid = gids;
    } else {
        struct ibv_port_attr port_attr;
        ibv_query_port(conn->context, conn->port, &port_attr);
        info->local_id = port_attr.lid;
    }

    info->port = conn->port;
    info->qp_number = conn->qp->qp_num;
    info->num_mr = conn->num_mr;
    if (conn->num_mr != 0) 
        memcpy(&info->mr, conn->mr, conn->num_mr * sizeof(struct ibv_mr));

    return info_size;
}

int extract_info_inplace(struct rdma_conn *conn, void *buf, size_t size) {

    int info_size = sizeof(struct conn_info) + conn->num_mr * sizeof(struct ibv_mr);
    if (info_size > size)
        return -1;
    struct conn_info *info = (struct conn_info*)buf;

    if (conn->gid != RDMA_PROTOCOL_IB) {
        union ibv_gid gids;
        ibv_query_gid(conn->context, conn->port, conn->gid, &gids);
        info->gid = gids;
    } else {
        struct ibv_port_attr port_attr;
        ibv_query_port(conn->context, conn->port, &port_attr);
        info->local_id = port_attr.lid;
    }

    info->port = conn->port;
    info->qp_number = conn->qp->qp_num;
    info->num_mr = conn->num_mr;
    if (conn->num_mr != 0) 
        memcpy(&info->mr, conn->mr, conn->num_mr * sizeof(struct ibv_mr));

    return info_size;
}

