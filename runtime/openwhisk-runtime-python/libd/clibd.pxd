cdef extern from "libd/include/libd.h":
    ctypedef struct libd_transport:
        pass
    ctypedef struct libd_action:
        pass

    # statemachine functions 
    int libd_transport_init      (libd_transport* trans); 
    int libd_transport_connect   (libd_transport* trans);
    int libd_transport_recover   (libd_transport* trans);
    int libd_transport_terminate (libd_transport* trans);

    # Actions interfaces
    struct libd_action * libd_action_init(char* aid, char* server_url);
    int libd_action_free(libd_action* action);

    int libd_action_add_transport(libd_action* action, char* durl);
    int libd_action_config_transport(libd_action* action, char* name, char* durl);
    libd_transport* libd_action_get_transport(libd_action* action, char* name);

# Interfaces for Cython Applications
# RDMA Interface
cdef extern from "libd/include/libd_trdma.h":
    void* libd_trdma_reg   (libd_transport* trans, size_t size, void* buf);
    int libd_trdma_read  (libd_transport* trans, size_t size, uint64_t addr, void* buf);
    int libd_trdma_write (libd_transport* trans, size_t size, uint64_t addr, void* buf);

# RDMA Server Interface
cdef extern from "libd/include/libd_trdma_server.h":
    int libd_trdma_server_serve (struct libd_transport *trans);
