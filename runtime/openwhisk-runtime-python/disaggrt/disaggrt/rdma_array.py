import sys
import copy
import collections
import numpy as np
from math import ceil
from . import buffer_pool_lib
import json

def prod(it):
    p = 1
    for e in it:
        p = p * e
    return p

def parse_typestr(element_typestr):
    byte_char = element_typestr[2]
    return int(byte_char)

class remote_array_metadata:
    def __init__(self, remote_addr, mem_size, dtype, element_byte, element_per_block, array_shape):
        self.dtype = dtype
        self.element_byte = element_byte
        self.element_per_block = element_per_block 
        self.remote_addr = remote_addr
        self.mem_size = mem_size
        self.shape = array_shape
    
    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, dict):
        self.__dict__ = dict

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

# @todo add asarray
# apply logistic regression split
class remote_array():
    # @todo current input should be changed to block iterator
    def __init__(self, buffer_pool, input_ndarray = None, metadata = None):
        # init from a real array or a metadata
        self.buffer_pool = buffer_pool
        # indicating whether the array is materialized on local
        self.local_array = None
        if input_ndarray is not None:
            array_shape = input_ndarray.shape
            input_ndarray = input_ndarray.ravel()
            page_id_list = []
            block_size = buffer_pool_lib.page_size
            element_typestr = input_ndarray.__array_interface__['typestr']
            element_byte = parse_typestr(element_typestr)
            assert(block_size % element_byte == 0)
            element_per_block = int(block_size / element_byte)
            input_array_in_byte = input_ndarray.tobytes()
            mem_size = len(input_array_in_byte)
            remote_start_addr = self.buffer_pool.write(input_array_in_byte)
            if remote_start_addr == -1:
                print("write data failure due to start address prblem")
                exit(1)
            else:
                self.metadata = remote_array_metadata(remote_start_addr, mem_size, input_ndarray.dtype, element_byte, element_per_block, array_shape)
        else:
            self.metadata = metadata
        
    def get_array_from_buf(self, buf):
        return np.frombuffer(buf, dtype=self.metadata.dtype)

    def get_page_id_from_idx(self, idx):
        page_id_list = self.metadata.page_id_list
        element_per_block = self.metadata.element_per_block
        cur_begin_idx = self.metadata.begin_offset
        idx = idx - (element_per_block - cur_begin_idx)
        if idx < 0:
            return page_id_list[0]
        cur_block_idx = int(ceil((idx / element_per_block)))
        return page_id_list[cur_block_idx]

    def get_block_offset_from_idx(self, idx):
        element_per_block = self.metadata.element_per_block
        cur_begin_idx = self.metadata.begin_offset
        # @todo we ignore slice for now There is bug here!
        idx = idx - (element_per_block - cur_begin_idx)
        if idx < 0:
            return idx + element_per_block
        return idx % element_per_block

    def get_block(self, cur_page_id):
        fetch_result = self.buffer_pool.read([cur_page_id])
        fetch_status = fetch_result[0]
        if fetch_status == False:
            print("fetch_status: {0}; page invalid getting block".format(fetch_status))
            return None
        return self.get_array_from_buf(fetch_result[1])
        
    def __setitem__(self, idx, val):
        print("usin deprecated function exit")
        exit(1)

    def get_array_metadata(self):
        return self.metadata

    # return the supposed remote address given element idx
    def get_remote_addr_from_idx(self, element_idx):
        address_offset = element_idx * self.metadata.element_byte
        return self.metadata.remote_addr + address_offset

    # follow numpy and python list semantic -> [) shallow copy; currently has offset problem
    def get_slice(self, start_idx, end_idx):
        element_per_block = self.metadata.element_per_block
        start_addr = self.get_remote_addr_from_idx(start_idx)
        end_addr = self.get_remote_addr_from_idx(end_idx)
        cur_metadata = self.metadata
        cur_shape = list(cur_metadata.shape)
        cur_shape[0] = end_idx - start_idx
        slice_mem_size = end_addr - start_addr
        slice_metadata = remote_array_metadata(start_addr, slice_mem_size, cur_metadata.dtype, cur_metadata.element_byte, cur_metadata.element_per_block,tuple(cur_shape))
        return remote_array(self.buffer_pool, metadata = slice_metadata)

    # load remote_array[start_idx:end_idx) to local buffer; read only
    # @todo support finer granularity of array; for now we pull the whole array to local
    def materialize(self, start_idx = -1, end_idx = -1):
        if start_idx == -1:
            start_idx = 0
        if end_idx == -1:
            end_idx = prod(self.metadata.shape)
        start_addr = self.get_remote_addr_from_idx(start_idx)
        end_addr = self.get_remote_addr_from_idx(end_idx)
        cur_mem_size = end_addr - start_addr
        print("fetching mem from {0} with size {1}".format(start_addr, cur_mem_size))
        fetch_status, fetch_data = self.buffer_pool.read(start_addr, cur_mem_size)
        if fetch_status != True:
            print("fetch_status: {0}; page invalid getting block".format(fetch_status))
            exit(1)
        self.local_array = self.get_array_from_buf(fetch_data)
        # restore shape
        # assert(self.local_array != None)
        self.local_array.shape = self.metadata.shape
        return self.local_array
