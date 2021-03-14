#include <stdio.h>

#include "libd.h"
#include "libd_trdma_server.h"

#define ACTIVATION_ID ("00000000")
#define SERVER_URL ("localhost:8081")


int main(int argc, char *argv[]) {
    char * default_port = "2333";
    char * server_template = "server;rdma_tcp_server;url,tcp://*:%s;size,65535;";
    
    char server_config[1024];
    if (argc == 2)
        sprintf(server_config, server_template, argv[1]);
    else 
        sprintf(server_config, server_template, default_port);

    struct libd_action * action =
        libd_action_init(ACTIVATION_ID, SERVER_URL);
    libd_action_add_transport(action, server_config);

    struct libd_transport * trans =
        libd_action_get_transport(action, "server");

    // Will block at this instruction!
    libd_trdma_server_serve(trans);

    fprintf(stderr, "Reach end of main function, EXIT\n");
    return -1;
}
