from libc cimport stdint

# interfaces for libd.h
cdef extern from "libd.h":
    cdef struct libd_transport:
        pass
    cdef struct libd_action:
        pass

    # TODO: check if needed in API
    # statemachine functions 
    # int libd_transport_init      (libd_transport* trans); 
    # int libd_transport_connect   (libd_transport* trans);
    # int libd_transport_recover   (libd_transport* trans);
    # int libd_transport_terminate (libd_transport* trans);

    # Actions interfaces
    libd_action * libd_action_init(char* aid, int argc, char ** argv);
    int libd_action_free(libd_action* action);

    int libd_action_add_transport(libd_action* action, char* durl);
    int libd_action_config_transport(libd_action* action, char* name, char* durl);
    libd_transport* libd_action_get_transport(libd_action* action, char* name);

    # plugin
    int libd_plugin_invoke(libd_action * action,
                    const char * name, int cmd, void * args);

# Interfaces for different transport
# RDMA Interface
cdef extern from "interfaces/libd_trdma.h":
    void* libd_trdma_reg (libd_transport* trans, size_t size, void* buf);
    int libd_trdma_read  (libd_transport* trans, size_t size, stdint.uint64_t addr, void* buf);
    int libd_trdma_write (libd_transport* trans, size_t size, stdint.uint64_t addr, void* buf);

# RDMA Server Interface
cdef extern from "interfaces/libd_trdma_server.h":
    int libd_trdma_server_serve (libd_transport *trans);

