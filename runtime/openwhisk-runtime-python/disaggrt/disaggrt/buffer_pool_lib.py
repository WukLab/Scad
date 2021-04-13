from disagg import *
import struct
import threading
import time
import pickle
import sys
import copy
import codecs
import copyreg
import collections
import json

def action_setup():
    # some parameters to set up trans; 
    activation_id = "0000"
    server_url = "0000"
    transport_name = 'client1'
    server_port = 2333
    memory_block_size = buffer_size
    # @todo should be read from a config

    cv = threading.Condition()
    # in real launch, this part will be handled by serverless system
    action = LibdAction(cv, activation_id, server_url)

    transport_url = "{};rdma_tcp;".format(transport_name)
    action.add_transport(transport_url)

    def delayed_config():
        time.sleep(5)
        extra_url = "url,tcp://localhost:{};size,{};".format(
            server_port, memory_block_size)
        action.config_transport(transport_name, extra_url)
        with cv:
            cv.notify_all()

    config_thread = threading.Thread(
        target = delayed_config)
    config_thread.start()
    config_thread.join()
    return action


# global parameters
page_size = 256
page_buffer_size = 655360
metadata_reserve_mem = 0
buffer_size = page_buffer_size
# @todo hint prefetch
# current strategy is to use list of numpy array

class local_page_node:
    def __init__(self, begin_offset, id = -1):
        self.id = id
        # offset indicates location in buffer
        self.begin_offset = begin_offset
        self.free_offset = self.begin_offset
        self.dirty_bit = 0
        self.prev = None
        self.next = None

    def write_to_remote_if_dirty(self, trans, remote_addr, write_size):
        if self.dirty_bit == 1:
            trans.write(write_size, remote_addr, self.begin_offset)

    def get_free_space(self):
        return page_size - self.get_block_size()

    def set_block_size(self, cur_block_size):
        self.free_offset = self.begin_offset + cur_block_size + 1

    def get_block_size(self):
        return self.free_offset - self.begin_offset - 1

    def write_to_local(self, buf, data):
        # @todo currently we do not support finer granularity in a page so reset free_offset each time
        self.free_offset = self.begin_offset
        data_size = len(data)
        buf[self.free_offset:self.free_offset + data_size] = data
        self.free_offset = self.free_offset + data_size + 1
        self.dirty_bit = 1

    def set_id(self, id):
        self.id = id

class remote_page_node:
    def __init__(self, begin_addr, block_size = 0):
        self.begin_addr = begin_addr
        self.block_size = block_size
    
    def set_block_size(self, block_size):
        self.block_size = block_size

    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, dict):
        self.__dict__ = dict

class buffer_metadata:
    def __init__(self, page_seq, remote_addr, remote_table):
        self.page_seq = page_seq
        self.remote_addr = remote_addr
        self.remote_table = remote_table
        
    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, dict):
        self.__dict__ = dict


class buffer_pool:
    def __init__(self, trans, metadata = None):
        '''
        @todo currently, assume append only
        @todo currently, assume read write is sync
        assumptions 
        current workload -> overall memory needed by the working set are not extremely larger than t
        '''
        self.trans = trans
        # page_id -> page_node (only page in local)
        self.page_table = dict()
        self.cur_free_page_idx = 0
        if metadata is None:
            # reserve space for metadata communication
            self.remote_addr = metadata_reserve_mem
            self.page_seq = 0
            # page_id -> begin_addr (currently we do full page write)
            self.remote_table = dict()
        else:
            # set up buffer pool with metadata
            self.remote_addr = metadata.remote_addr
            self.remote_table = metadata.remote_table
            self.page_seq = metadata.page_seq
        assert(page_buffer_size % page_size == 0)
        self.buffer_page_num = int(page_buffer_size / page_size)
        # map buf_offset = idx * blocksize -> page_id
        self.offset_table = [-1] * self.buffer_page_num 

    def gen_new_id(self):
        res = self.page_seq
        self.page_seq = self.page_seq + 1    
        return res

    def get_buffer_metadata(self):
        # @todo this is not what we should do; just for rush ddl, i flush local page_table before sending out metadata
        for local_page_id in self.page_table:
            cur_local_page = self.page_table[local_page_id]
            cur_remote_page = self.remote_table[local_page_id]
            cur_local_page.write_to_remote_if_dirty(self.trans, cur_remote_page.begin_addr, cur_remote_page.block_size)
        return buffer_metadata(self.page_seq, self.remote_addr, self.remote_table)

    # update buffer to get request_page_num free space in local buffer
    # in read mode, we will also pull related page id to local
    def get_buffer_offset(self, page_id_list, isRead=False):
        global page_size
        request_page_num = len(page_id_list)
        cur_available_page_num = self.buffer_page_num - self.cur_free_page_idx
        if self.buffer_page_num < request_page_num:
            print("request page exceed all buffer mem; stop app")
            exit(1)
        if cur_available_page_num < request_page_num:
            last_flush_page_id = -1
            # evict first cur_available_page_num page to remote memory
            for i in range(cur_available_page_num):
                prev_page_id = self.offset_table[i]
                if last_flush_page_id != prev_page_id:
                    prev_local_node = self.page_table[prev_page_id]
                    prev_remote_page = self.remote_table[prev_page_id]
                    prev_local_node.write_to_remote_if_dirty(self.trans, prev_remote_page.begin_addr, prev_remote_page.block_size)
                    last_flush_page_id = prev_page_id
            self.cur_free_page_idx = 0
        ret_buf_offset = self.cur_free_page_idx * page_size + metadata_reserve_mem
        for i in range(request_page_num):
            # read each page into corresponding local buffer offset
            if isRead:
                cur_remote_node = self.remote_table[page_id_list[i]]
                cur_buf_offset = (self.cur_free_page_idx + i) * page_size + metadata_reserve_mem
                self.trans.read(cur_remote_node.block_size, addr=cur_remote_node.begin_addr, offset=cur_buf_offset)
            self.offset_table[self.cur_free_page_idx + i] = page_id_list[i]
        self.cur_free_page_idx = self.cur_free_page_idx + request_page_num
        return ret_buf_offset
        
    def get_remote_addr_from_id(self, page_id):
        cur_remote_page_node = self.remote_table[page_id]
        return cur_remote_page_node.begin_addr
    
    def get_block_size_from_id(self, page_id):
        cur_remote_page_node = self.remote_table[page_id]
        return cur_remote_page_node.block_size

    def get_buffer_slice(self, begin_offset, slice_size):
        return self.trans.buf[begin_offset:begin_offset+slice_size]

    def find_pages_in_mem(self, page_id_list):
        global page_size
        expect_buf_offset = -1
        for cur_page_id in page_id_list:
            if cur_page_id in self.page_table:
                cur_page_node = self.page_table[cur_page_id]
                if expect_buf_offset == -1 or expect_buf_offset == cur_page_node.begin_offset:
                    expect_buf_offset = cur_page_node.begin_offset + page_size
                else:
                    # @todo add logic deal with partial page
                    return -1
            else:
                return -1
        return self.page_table[page_id_list[0]].begin_offset

    # write data to remote memory and update metadata
    def write(self, data, cur_page_id = -1):
        global page_size
        write_size = len(data)
        # get cur_page_node
        cur_page_node = None
        # write to old page
        if cur_page_id != -1:
            if cur_page_id not in self.page_table:
                # fetch page from remote; build page node withs lru replacer update offset
                page_remote_addr = self.get_remote_addr_from_id(cur_page_id)
                cur_buf_offset = self.get_buffer_offset([cur_page_id])
                # read remote_addr to victim page offset
                self.trans.read(write_size, addr=page_remote_addr, offset=cur_buf_offset)
                # build page node
                cur_page_node = local_page_node(cur_buf_offset, cur_page_id)
            else:
                cur_page_node = self.page_table[cur_page_id]
            # update data into remote memory, for simplicity we make memory aligned
        else:
            # write to new page; gen page_id check free list first; update remote addr
            cur_page_id = self.gen_new_id()
            # build page node withs lru replacer update offset; update buf 
            cur_buf_offset = self.get_buffer_offset([cur_page_id])
            cur_page_node = local_page_node(cur_buf_offset, cur_page_id)
            cur_page_id = cur_page_node.id
            assert(cur_page_id != -1)
            # update remote table only when we create page_id; page_id 1:1 remote_addr
            self.remote_table[cur_page_id] = remote_page_node(self.remote_addr, write_size)
            self.remote_addr = self.remote_addr + page_size

        assert(cur_page_node.id != -1)
        # @todo this could be improved in finer granularity
        cur_page_node.write_to_local(self.trans.buf, data)
        self.page_table[cur_page_id] = cur_page_node
        return cur_page_id

    # return a list ["fetch_{success/fail}", fetch_data]
    def read(self, page_id_list, begin_offset = -1, end_offset = -1):
        global page_size
        cur_buf_offset = self.find_pages_in_mem(page_id_list)
        # page id are not in mem
        if cur_buf_offset == -1:
            cur_buf_offset = self.get_buffer_offset(page_id_list, True)
        cur_block_size = (len(page_id_list) - 1) * page_size
        if end_offset == -1:
            last_block_size = self.page_table[page_id_list[-1]].get_block_size()
        else:
            last_block_size = end_offset
        cur_block_size = cur_block_size + last_block_size
        if begin_offset == -1:
            cur_buf_offset = cur_buf_offset + begin_offset
        fetched_value = self.get_buffer_slice(cur_buf_offset, cur_block_size)
        return ["fetch_success", fetched_value]



# return params
def read_params():
    context_file = "virtual_file"
    f = open(context_file, 'rb')
    context_dict_in_byte = f.read()
    context_dict = pickle.loads(context_dict_in_byte)
    print(context_dict)
    # read the size of context dict
    # trans.read(4, addr = 0, offset = 0)
    # context_dict_in_byte_size = struct.unpack_from('@I', trans.buf[0:])[0]
    # trans.read(context_dict_in_byte_size, addr=4, offset = 0)
    # print("read context dictionary with size in byte: {0}".format(context_dict_in_byte_size))
    # context_dict_in_byte = trans.buf[0:0 + context_dict_in_byte_size]
    # first load from memoryslice to byte; then load byte to original data type
    # context_dict = pickle.loads(pickle.loads(context_dict_in_byte))
    return context_dict

# write params to remote memory
def write_params(context_dict):
    context_file = "virtual_file"
    f = open(context_file, 'wb')
    context_dict_in_byte = pickle.dumps(context_dict)
    f.write(context_dict_in_byte)
    # context_dict_in_byte_size = len(context_dict_in_byte)
    # size_of_int = 4
    # struct.pack_into('@I', trans.buf, 0, context_dict_in_byte_size)
    # print("write context dictionary with size in byte: {0}".format(context_dict_in_byte_size))
    # trans.buf[size_of_int:size_of_int + context_dict_in_byte_size] = context_dict_in_byte
    # trans.write(size_of_int + context_dict_in_byte_size, addr = 0, offset = 0)
