#include <stdio.h>

#include "libd.h"
#include "interfaces/libd_trdma_server.h"

#define ACTIVATION_ID ("00000000")

#define SIZE (1024 * 1024 * 64)


int main(int argc, char *argv[]) {
    char * default_port = "2333";
    char * implementation = "tcp";
    char * server_template = "server;rdma_%s_server;url,tcp://*:%s;size,%d;";

    char server_config[1024];
    // parse config
    if (argc >= 2)
        default_port = argv[1];
    if (argc >= 3)
        implementation = argv[2];
        
    sprintf(server_config, server_template, implementation, default_port, SIZE);

    struct libd_action * action =
        libd_action_init(ACTIVATION_ID, 0, NULL);
    libd_action_add_transport(action, server_config);

    struct libd_transport * trans =
        libd_action_get_transport(action, "server");

    // Will block at this instruction!
    libd_trdma_server_serve(trans);
    printf("Server setup\n");

    for (;;);
    fprintf(stderr, "Reach end of main function, EXIT\n");
    return -1;
}
