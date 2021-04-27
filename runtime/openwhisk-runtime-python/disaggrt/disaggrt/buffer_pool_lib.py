from disagg import *
import sys
import copy
import bisect
from collections import OrderedDict
import json
import math

# global parameters
page_size = 256
# default, 64M
page_buffer_size = 1024 * 1024 * 64
metadata_reserve_mem = 0
buffer_size = page_buffer_size
# @todo hint prefetch
# current strategy is to use list of numpy array

class local_page_node:

    def __init__(self, page_id, transport_name, remote_addr, block_size, dirty_bit = False):
        global page_size
        self.id = page_id
        self.transport_map = dict()
        # @todo finer granularity dirty bit
        self.transport_map[transport_name] = [remote_addr, block_size, 0]
        self.block_size = block_size
        self.dirty_bit = dirty_bit
        
    def write_to_remote_if_dirty(self, trans_map):
        global page_size
        if self.dirty_bit:
            base_offset = self.id * page_size
            for transport_name, trans_info in self.transport_map.items():
                cur_remote_addr, cur_block_size, cur_offset = trans_info
                trans_map[transport_name].write(cur_block_size, cur_remote_addr, cur_offset + base_offset)
                # print("Sending {0}byte data to {1} at offset {2}".format(cur_block_size, transport_name,  cur_offset + base_offset))

    def add_transport_pair(self, transport_name, remote_addr, cur_block_size):
        cur_trans_offset = self.block_size
        self.block_size = self.block_size + cur_block_size
        self.transport_map[transport_name] = [remote_addr, cur_block_size, cur_trans_offset]

    def get_free_space(self):
        return page_size - self.block_size

    def get_block_size(self):
        return self.block_size
    
    def set_block_size(self, cur_block_size):
        self.block_size = cur_block_size

class buffer_pool:
    def __init__(self, trans_map, trans_metadata = {}):
        '''
        @todo currently, assume read write is sync
        assumptions 
        current workload -> overall memory needed by the working set are not extremely larger than t
        '''
        self.trans_map = trans_map
        # page_id -> local page node 
        self.base_trans_name = list(self.trans_map.keys())[0]
        self.page_table = dict()
        # trans name -> last free remote_addr 
        self.trans_metadata = trans_metadata
        # remote addr -> page_id
        self.remote_metadata_dict = OrderedDict()
        self.cur_free_page_idx = 0
        assert(page_buffer_size % page_size == 0)
        self.buffer_page_num = int(page_buffer_size / page_size)

    def get_buffer_metadata(self):
        for page_id, page_node in self.page_table.items():
            if page_node.dirty_bit:
                page_node.write_to_remote_if_dirty(self.trans_map)
        return self.trans_metadata
    
    def buffer_pool_upate(self, page_id_base, remote_metadata_list, dirty=False):
        global page_size
        buf_iter = 0
        page_iter = -1
        for remote_metadata_per_transport in remote_metadata_list:
            cur_transport_name, cur_remote_addr, cur_mem_size_in_byte = remote_metadata_per_transport
            remote_addr_iter = cur_remote_addr
            cur_metadata_key = "{0}_{1}".format(cur_transport_name, cur_remote_addr)
            mem_size_cnt = cur_mem_size_in_byte
            # per transport we have a block; cross block check at start
            if page_iter != -1:
                prev_block_size = self.page_table[page_iter].get_block_size()
                cur_free_space = page_size - prev_block_size
                if cur_free_space > 0:
                    cur_buf_offset = page_iter * page_size + prev_block_size
                    mem_size_cnt -= cur_free_space
                    self.page_table[page_iter].add_transport_pair(cur_transport_name, remote_addr_iter, min(cur_mem_size_in_byte, cur_free_space))
                    remote_addr_iter = remote_addr_iter + min(cur_mem_size_in_byte, cur_free_space)
                    if mem_size_cnt > 0:
                        page_iter = page_iter + 1
                else:
                    page_iter = page_iter + 1
                    cur_buf_offset = page_iter * page_size
            else:
                page_iter = page_id_base
                cur_buf_offset = page_iter * page_size
            self.remote_metadata_dict[cur_metadata_key] = [cur_mem_size_in_byte, cur_buf_offset]
            while mem_size_cnt > 0:
                cur_page_block_size = min(page_size, mem_size_cnt)
                self.page_table[page_iter] = local_page_node(page_iter, cur_transport_name, remote_addr_iter, block_size=cur_page_block_size, dirty_bit=dirty)
                remote_addr_iter = remote_addr_iter + cur_page_block_size
                mem_size_cnt = mem_size_cnt - cur_page_block_size
                if cur_page_block_size == page_size:
                    page_iter = page_iter + 1
        
    # update buffer to get request_page_num free space in local buffer
    # if we also want to fetch the data , we will also pull related page
    def get_buffer_offset(self, mem_size, remote_metadata_list = []):
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
                prev_local_node.write_to_remote_if_dirty(self.trans_map[prev_local_node.trans_port_name])
                prev_metadata_key = "{0}_{1}".format(prev_local_node.trans_port_name, prev_local_node.remote_addr)
                if prev_metadata_key in self.remote_metadata_dict:
                    self.remote_metadata_dict.pop(prev_metadata_key)
            self.cur_free_page_idx = 0

        last_page_id = self.cur_free_page_idx + request_page_num - 1
        ret_buf_offset = self.cur_free_page_idx * page_size
        if len(remote_metadata_list) > 0:
            self.fetch_remote_to_buffer(ret_buf_offset, remote_metadata_list, mem_size)
            # update page table and remote metadata dict
            self.buffer_pool_upate(self.cur_free_page_idx, remote_metadata_list)

        self.cur_free_page_idx = self.cur_free_page_idx + request_page_num
        return ret_buf_offset

    def add_transport(self, trans_port_name, trans):
        self.trans_map[trans_port_name] = trans
        
    def get_buffer_slice(self, trans_port_name, begin_offset, slice_size):
        return self.trans_map[trans_port_name].buf[begin_offset:begin_offset+slice_size]

    def write_to_buffer(self, trans_port_name, buf_offset, data, mem_size):
        self.trans_map[trans_port_name].buf[buf_offset:buf_offset + mem_size] = data

    def update_buffer_to_remote(self, buf_offset, trans_port_name, remote_addr, mem_size):
        stride_size = 1024
        for i in range(0, mem_size, stride_size):
            cur_write_size = min(stride_size, mem_size - i)
            self.trans_map[trans_port_name].write(cur_write_size, remote_addr, buf_offset)
            remote_addr = remote_addr + cur_write_size
            buf_offset = buf_offset + cur_write_size
            
    def fetch_remote_to_buffer(self, buf_offset, remote_metadata_list, mem_size):
        stride_size = 1024
        for remote_metadata_per_transport in remote_metadata_list:
            cur_trans_port_name, cur_remote_addr, cur_mem_size_in_byte = remote_metadata_per_transport
            for i in range(0, cur_mem_size_in_byte, stride_size):
                cur_read_size = min(stride_size, cur_mem_size_in_byte - i)
                # print("fetch from mem {0} at remote addr {1} with size {2}".format(cur_trans_port_name, cur_remote_addr, cur_read_size))
                self.trans_map[cur_trans_port_name].read(cur_read_size, cur_remote_addr, buf_offset)
                cur_remote_addr = cur_remote_addr + cur_read_size
                buf_offset = buf_offset + cur_read_size
    
    def parse_metadata_key(self, cur_metadata_key):
        cur_transport_name, cur_remote_addr = cur_metadata_key.split('_')
        return cur_transport_name, int(cur_remote_addr)

    # @todo for now only consider write to new memory
    def write(self, transport_name, data, remote_addr = -1):
        # @todo check whether there is enough memory left shift to another possible remote mem if possible
        global page_size
        # @todo currently assume write to single trans?
        cur_mem_size = len(data)
        # update local page table
        if remote_addr == -1:
            if transport_name not in self.trans_metadata:
                self.trans_metadata[transport_name] = 0
            remote_addr = self.trans_metadata[transport_name]
            cur_buf_offset = self.get_buffer_offset(cur_mem_size)
            cur_page_id = cur_buf_offset // page_size
            cur_metadata_list = [[transport_name, remote_addr, cur_mem_size]]
            self.write_to_buffer(transport_name, cur_buf_offset, data, cur_mem_size)
            self.buffer_pool_upate(cur_page_id, cur_metadata_list, dirty=True)
            self.trans_metadata[transport_name] = remote_addr + cur_mem_size
            # consider alignment in cut
            return [[transport_name, remote_addr, cur_mem_size]]

    def get_local_buf_offset(self, request_transport_name, request_remote_addr, request_mem_size):
        for remote_metadata_key, remote_metadata_info in self.remote_metadata_dict.items():
            cur_trans_name, cur_remote_addr = self.parse_metadata_key(remote_metadata_key)
            cur_mem_size, cur_buf_offset = remote_metadata_info
            if cur_trans_name == request_transport_name:
                if (cur_remote_addr + cur_mem_size) <= (request_remote_addr + request_mem_size):
                    return cur_buf_offset + (request_remote_addr - cur_remote_addr)
        return -1

    def find_remote_object_in_local(self, mem_size, remote_metadata_list):
        prev_buf_offset = -1
        res_buf_offset = -1
        for remote_metadata_per_transport in remote_metadata_list:
            cur_trans_port_name, cur_remote_addr, cur_mem_size_in_byte = remote_metadata_per_transport
            local_buf_offset = self.get_local_buf_offset(cur_trans_port_name, cur_remote_addr, cur_mem_size_in_byte)
            if local_buf_offset == -1:
                return -1
            if prev_buf_offset != local_buf_offset:
                if prev_buf_offset != -1:
                    return -1
                else:
                    res_buf_offset = local_buf_offset
            prev_buf_offset = local_buf_offset + cur_mem_size_in_byte
        return res_buf_offset

    def read(self, remote_metadata_list, mem_size):
        # check wether in the pool already
        cur_begin_offset = self.find_remote_object_in_local(mem_size, remote_metadata_list)
        if cur_begin_offset == -1:
             # not in local, pull from remote
            cur_begin_offset = self.get_buffer_offset(mem_size, remote_metadata_list)
        trans_port_name = remote_metadata_list[0][0]
        return True, self.get_buffer_slice(trans_port_name, cur_begin_offset, mem_size)
