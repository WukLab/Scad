from disagg import *
import struct

activation_id = "0000"
server_url = "0000"

transport_name = 'client1'
server_port = 2333
memory_block_size = 65536

def main(args, action):
    int_value = 12345

    # get connection to memory object by name
    trans = action.get_transport(transport_name, 'rdma')

    # register buffer for rdma
    trans.reg(1024)

    # trans.buf is the zero-copy rdma buffer, can be accessed as normal python buffer
    # forexample, you can use pack to pack a python object into buffer
    # or you can use the buffer for binary data
    struct.pack_into('@I', trans.buf, 0, int_value)

    # write will issue an rdma request to remote
    # currently, all calls are sync. Will provide async APIs
    # address is the address for remote buffer, offset is the offset for local buffer
    # if the connection is not ready, this will be blocked
    trans.write(4, addr = 0, offset = 0)

    # read is the same
    trans.read(4, addr = 0, offset = 4)
    fetched_value = struct.unpack_from('@I', trans.buf[4:])[0]

    # verify the value
    print('API test int = {}, fetch = {}'.format(int_value, fetched_value),
            fetched_value == int_value)

if __name__ == '__main__':

    # in real launch, this part will be handled by serverless system
    action = LibdAction(activation_id, server_url)

    transport_url = "{};rdma_tcp;url,tcp://localhost:{};size,{};".format(
        transport_name, server_port, memory_block_size);

    action.add_transport(transport_url)
    main(None, action)

