cimport clibd
from functools import partial

# Pack APIs to pure python objects
cdef class LibdAction:
    cdef clibd.libd_action * _c_action

    def __cinit__(self, str aid, str server_url):
        self._c_action = clibd.libd_action_init(
            aid.encode('ascii'), server_url.encode('ascii'))
        if self._c_action is NULL:
            raise MemoryError()

        # Python init part
        self.transports = {}

    # del will call free
    def __dealloc__(self):
        if self._c_action is not NULL:
            clibd.libd_action_free(self._c_action)

    # Member Functions
    def add_transport(self, durl):
        # TODO: add ttype here
        c_durl = durl.enocde('ascii')
        return clibd.libd_action_add_transport(self._c_action, c_durl)

    def config_transport(self, name, durl):
        return clibd.libd_action_config_transport(
            self._c_action, name.encode('ascii'), durl.encode('ascii'))

    def get_transport(self, name, ttype):
        if name not in self.transports:
            # raise exception if type is not found
            # delay construct of object by function wrap
            trans = {
                'rdma': partial(LibdTransportRDMA, self)
            }[ttype](name)
            self.transports[name] = trans
        # raise exception if name is not found
        return self.transports[name]

    # Python APIs
    # Reset methods for reuse of this instance

cdef class LibdTransport:
    cdef clibd.libd_transport * _c_trans
    def __cinit__(self, LibdAction action, char * name, *argv):
        self.action = action
        self._c_trans = clibd.libd_action_get_transport(
                action._c_action, name)
        # TODO: check this error
        if self._c_trans is NULL:
            raise MemoryError()

cdef class LibdTransportRDMA(LibdTransport):
    # use of buf is required
    cdef char * _c_buf
    cdef public char [:] buf

    def __cinit__(self, LibdAction action, char * name, *argv):
        self.initd = False

    def reg(self, size):
        self._c_buf = <char *> clibd.libd_trdma_reg(
            self._c_trans, size, self._c_buf)

        if self._c_buf is NULL:
            raise MemoryError()
        self.size = size
        self.initd = True
        self.buf = <char [:size]>self._c_buf

    def read(self, size, addr):
        if (self.initd == False):
            raise MemoryError()
        return clibd.libd_trdma_read(self._c_trans, size, addr, self._c_buf)

    def wirte(self, size, addr):
        if (self.initd == False):
            raise MemoryError()
        return clibd.libd_trdma_write(self._c_trans, size, addr, self._c_buf)

