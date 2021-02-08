#include <stdio.h>
#include "libd.h"
#include "libd_trdma.h"

int main() {
    int ret;
    const char * wrong_durl = "component1trans1;rdma_tcp;url,tcp://localhost:2333;";
    const char * correct_durl = "component1trans1;rdma_tcp;url,tcp://localhost:2333;size,2345;";

    struct libd_action * action = libd_action_init("233", "localhost");
    ret = libd_action_add_transport(action, correct_durl);
    printf("initilize action, ret %d\n", ret);

    struct libd_transport *trans = libd_action_get_transport(action, "component1trans1");
    printf("initilize action, ret %p\n", trans);

    // run tests
    char send_buf[256] = "hello disaggregation!";
    char recv_buf[256] = { 0 };

    ret = libd_trdma_write(trans, 32, 0, send_buf);
    dprintf("after sending, ret %d", ret);

    ret = libd_trdma_read(trans, 32, 0, recv_buf);
    dprintf("after recving, ret %d, msg %s", ret, recv_buf);


    return ret;
}

