from disagg import *
import sys
import copy
import bisect
from collections import OrderedDict
import json
import math

# global parameters
page_size = 256
page_buffer_size = 655360000
metadata_reserve_mem = 0
buffer_size = page_buffer_size
# @todo hint prefetch
# current strategy is to use list of numpy array

class local_page_node:
    def __init__(self, page_id, remote_addr, block_size = 0, dirty_bit = False):
        global page_size
        self.id = page_id
        self.begin_offset = self.id * page_size
        self.remote_addr = remote_addr
        # @maybe different object are stored in the same page 
        self.block_size = block_size
        self.free_offset = self.begin_offset + self.block_size + 1
        self.dirty_bit = dirty_bit

    def write_to_remote_if_dirty(self, trans):
        if self.dirty_bit:
            trans.write(self.block_size, self.remote_addr, self.begin_offset)

    def get_free_space(self):
        return page_size - self.block_size
    
    def set_block_size(self, cur_block_size):
        self.block_size = cur_block_size

class buffer_metadata:
    def __init__(self, page_seq, remote_addr):
        self.page_seq = page_seq
        self.remote_addr = remote_addr        
    def __getstate__(self):
        return self.__dict__.copy()
    def __setstate__(self, dict):
        self.__dict__ = dict

class buffer_pool:
    def __init__(self, trans, remote_addr = 0):
        '''
        @todo currently, assume read write is sync
        assumptions 
        current workload -> overall memory needed by the working set are not extremely larger than t
        '''
        self.trans = trans
        # page_id -> local page node 
        self.page_table = dict()
        # remote addr -> page_id
        self.remote_metadata_dict = OrderedDict()
        self.cur_free_page_idx = 0
        self.remote_addr = int(remote_addr)
        assert(page_buffer_size % page_size == 0)
        self.buffer_page_num = int(page_buffer_size / page_size)

    def get_buffer_metadata(self):
        for page_id, page_node in self.page_table.items():
            if page_node.dirty_bit:
                page_node.write_to_remote_if_dirty(self.trans)
        return self.remote_addr
    
    def page_table_upate(self, mem_size, page_id_base, remote_addr, dirty=False):
        global page_size
        remote_addr_iter = remote_addr
        for buf_iter in range(0, mem_size, page_size): 
            cur_page_id = math.floor(buf_iter / page_size) + page_id_base
            cur_page_block_size = min(page_size, mem_size - buf_iter)
            self.page_table[cur_page_id] = local_page_node(cur_page_id, remote_addr_iter, block_size=cur_page_block_size, dirty_bit=dirty)
            remote_addr_iter = remote_addr_iter + cur_page_block_size

    # update buffer to get request_page_num free space in local buffer
    # if we also want to fetch the data , we will also pull related page
    def get_buffer_offset(self, mem_size, remote_addr=-1):
        global page_size
        request_page_num = math.ceil(mem_size / page_size)
        cur_available_page_num = self.buffer_page_num - self.cur_free_page_idx
        if self.buffer_page_num < request_page_num:
            print("request page exceed all buffer mem; stop app")
            exit(1)
        if cur_available_page_num < request_page_num:
            # evict page to remote memory
            for prev_page_id in range(request_page_num):
                prev_local_node = self.page_table[prev_page_id]
                prev_local_node.write_to_remote_if_dirty(self.trans)
                if prev_local_node.remote_addr in self.remote_metadata_dict:
                    self.remote_metadata_dict.pop(prev_local_node.remote_addr)
            self.cur_free_page_idx = 0
        last_page_id = self.cur_free_page_idx + request_page_num - 1
        ret_buf_offset = self.cur_free_page_idx * page_size
        if remote_addr != -1:
            self.fetch_remote_to_buffer(ret_buf_offset, remote_addr, mem_size)
            self.page_table_upate(mem_size, self.cur_free_page_idx, remote_addr)
            # store the mem_size and the first page_id
            self.remote_metadata_dict[remote_addr] = [mem_size, ret_buf_offset]
        self.cur_free_page_idx = self.cur_free_page_idx + request_page_num
        return ret_buf_offset
        
    def get_buffer_slice(self, begin_offset, slice_size):
        return self.trans.buf[begin_offset:begin_offset+slice_size]

    def find_object_in_mem(self, remote_addr, mem_size):
        global page_size

    def write_to_buffer(self, buf_offset, data, mem_size):
        self.trans.buf[buf_offset:buf_offset + mem_size] = data

    def update_buffer_to_remote(self, buf_offset, remote_addr, mem_size):
        stride_size = 1024
        for i in range(0, mem_size, stride_size):
            cur_write_size = min(stride_size, mem_size - i)
            print("reading data from {0} with size {1}".format(remote_addr, cur_write_size))
            self.trans.write(cur_write_size, remote_addr, buf_offset)
            remote_addr = remote_addr + cur_write_size
            buf_offset = buf_offset + cur_write_size
            
    def fetch_remote_to_buffer(self, buf_offset, remote_addr, mem_size):
        stride_size = 1024
        for i in range(0, mem_size, stride_size):
            cur_read_size = min(stride_size, mem_size - i)
            print("reading data from {0} with size {1}".format(remote_addr, cur_read_size))
            self.trans.read(cur_read_size, remote_addr, buf_offset)
            remote_addr = remote_addr + cur_read_size
            buf_offset = buf_offset + cur_read_size

    # @todo for now only consider write to new memory
    def write(self, data, remote_addr = -1):
        # @todo check whether there is enough memory left
        global page_size
        cur_mem_size = len(data)
        # update local page table
        if remote_addr == -1:
            remote_addr = self.remote_addr
            cur_buf_offset = self.get_buffer_offset(cur_mem_size)
            cur_page_id = cur_buf_offset / page_size
            self.remote_metadata_dict[remote_addr] = [cur_mem_size, cur_buf_offset]
            self.write_to_buffer(cur_buf_offset, data, cur_mem_size)
            self.page_table_upate(cur_mem_size, cur_page_id, remote_addr, dirty=True)
            self.remote_addr = remote_addr + cur_mem_size
            return remote_addr

    def find_offset_of_remote_addr(self, remote_addr, mem_size):
        if remote_addr in self.remote_metadata_dict:
            cur_object_mem_size, cur_object_begin_offset = self.remote_metadata_dict[remote_addr]
            if mem_size <= cur_object_mem_size:
                # get page_offset
                return cur_object_begin_offset
            else:
                # requesting obejct larger than we currently stored 
                return -1
        address_keys = self.remote_metadata_dict.keys()
        left_possible_index = bisect.bisect_left(address_keys, remote_addr)
        if left_possible_index == 0:
            return -1
        possible_remote_addr = address_keys[left_possible_index-1]
        possible_object_mem_size, possible_object_page_offset = self.remote_metadata_dict[possible_remote_addr]
        if (remote_addr + mem_size) < (possible_remote_addr + possible_object_mem_size):
            cur_object_begin_offset = possible_object_page_offset + (remote_addr - possible_remote_addr)
            return cur_object_begin_offset
        else:
            return -1

    def read(self, remote_addr, mem_size):
        # check wether in the pool already
        cur_begin_offset = self.find_offset_of_remote_addr(remote_addr, mem_size)
        if cur_begin_offset != -1:
            return True, self.get_buffer_slice(cur_begin_offset, mem_size)
        # not in local, pull from remote
        cur_begin_offset = self.get_buffer_offset(mem_size, remote_addr)
        return True, self.get_buffer_slice(cur_begin_offset, mem_size)


    
