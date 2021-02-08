#include <stdio.h>
#include "libd.h"
#include "libd_trdma_server.h"

int main() {
    int ret;
    char * server_durl = "component1trans1;rdma_tcp_server;url,tcp://*:2333;size,65535;";

    struct libd_action * action = libd_action_init("233", "localhost");
    ret = libd_action_add_transport(action, server_durl);
    dprintf("initilize action, ret %d", ret);

    struct libd_transport *trans = libd_action_get_transport(action, "component1trans1");
    dprintf("get transport, ret %p", trans);

    ret = libd_trdma_server_serve(trans);
    dprintf("serve, ret %p", trans);

    return ret;
}

