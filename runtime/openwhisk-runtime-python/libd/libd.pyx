cimport clibd

cdef class LibdAction:
    cdef clibd.libd_action* _c_action

    def __cinit__(self, aid, server_url):
        self._c_action = clibd.libd_action_init(aid, server_url)
        if self._c_action is NULL:
            return MemoryError()

    def __dealloc__(self):
        if self._c_action is not NULL:
            clibd.libd_action_free(self._c_action)

    # Member Functions
    def add_transport(self):
        pass

    def config_transport(self):
        pass

    def get_transport(self):
        pass

cdef class LibdTransport:
    pass

cdef class LibdTransportRDMA:

