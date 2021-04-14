import struct
import threading
import time
import pickle
import sys
import copy
import codecs
import copyreg
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
    def __init__(self, page_id_list, element_typestr, element_byte, element_per_block, begin_offset, end_offset, array_shape):
        self.page_id_list = page_id_list
        self.element_typestr = element_typestr
        self.element_byte = element_byte
        self.element_per_block = element_per_block
        # currently offset is the byte offset
        # with element_byte we could get index offset
        self.begin_offset = begin_offset
        self.end_offset = begin_offset
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
        self.local_range = [-1, -1]
        if input_ndarray is not None:
            array_shape = input_ndarray.shape
            print(array_shape)
            input_ndarray = input_ndarray.ravel()
            page_id_list = []
            block_size = buffer_pool_lib.page_size
            element_typestr = input_ndarray.__array_interface__['typestr']
            element_byte = parse_typestr(element_typestr)
            assert(block_size % element_byte == 0)
            element_per_block = int(block_size / element_byte)
            begin_offset = 0
            for i in range(0, len(input_ndarray), element_per_block):
                cur_id = self.buffer_pool.write(input_ndarray[i:i + element_per_block].tobytes())
                page_id_list.append(cur_id)
            last_block_size = self.buffer_pool.get_block_size_from_id(page_id_list[-1])
            end_offset = last_block_size / element_byte
            self.metadata = remote_array_metadata(page_id_list, element_typestr, element_byte, element_per_block, begin_offset, end_offset, array_shape)
        else:
            self.metadata = metadata
        
    def get_array_from_buf(self, buf):
        element_typestr = self.metadata.element_typestr
        if element_typestr[1:] == "i4":
            return np.frombuffer(buf, dtype=np.int32)
        elif element_typestr[1:] == "f8":
            return np.frombuffer(buf, dtype=np.float64)
        print("element type mismatch")

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
        if fetch_status != "fetch_success":
            print("fetch_status: {0}; page invalid getting block".format(fetch_status))
            return None
        return self.get_array_from_buf(fetch_result[1])
        
    def __setitem__(self, idx, val):
        cur_page_id = self.get_page_id_from_idx(idx)
        cur_block_offset = self.get_block_offset_from_idx(idx)
        fetch_block = np.copy(self.get_block(cur_page_id))
        fetch_block[cur_block_offset] = val
        # update the fetch_block
        self.buffer_pool.write(fetch_block.tobytes(), cur_page_id)

    def get_array_metadata(self):
        return self.metadata

    # follow numpy and python list semantic -> [) shallow copy; currently has offset problem
    def get_slice(self, start_idx, end_idx):
        element_per_block = self.metadata.element_per_block
        start_page_id = self.get_page_id_from_idx(start_idx)
        end_page_id = self.get_page_id_from_idx(end_idx)
        start_block_offset = self.get_block_offset_from_idx(start_idx)
        end_block_offset = self.get_block_offset_from_idx(end_idx)
        # print("offset start: {0} end: {1}".format(start_block_offset, end_block_offset))
        slice_page_list = list(range(start_page_id, end_page_id+1))
        cur_metadata = self.metadata
        slice_metadata = remote_array_metadata(slice_page_list, cur_metadata.element_typestr, cur_metadata.element_byte, cur_metadata.element_per_block, start_block_offset, end_block_offset, cur_metadata.shape)
        return remote_array(self.buffer_pool, metadata = slice_metadata)

    # load remote_array[start_idx:end_idx) to local buffer; read only
    # @todo support finer granularity of array; for now we pull the whole array to local
    def materialize(self, start_idx = -1, end_idx = -1):
        if start_idx == -1:
            start_idx = 0
        if end_idx == -1:
            print(self.metadata.shape)
            end_idx = prod(self.metadata.shape)
        start_page_id = self.get_page_id_from_idx(start_idx)
        end_page_id = self.get_page_id_from_idx(end_idx)
        if start_page_id == end_page_id:
            page_id_list = [start_page_id]
        else:
            page_id_list = list(range(start_page_id, end_page_id + 1))
        start_idx_offset = self.get_block_offset_from_idx(start_idx)
        start_buf_offset = start_idx_offset * self.metadata.element_byte
        end_idx_offset = self.get_block_offset_from_idx(end_idx)
        if end_idx_offset == 0:
            end_idx_offset = self.metadata.element_per_block
        end_buf_offset = end_idx_offset * self.metadata.element_byte
        # print("doing materialize start_offset:{0}    end_offset:{1}".format(start_buf_offset, end_buf_offset))
        fetch_result = self.buffer_pool.read(page_id_list, start_buf_offset, end_buf_offset)
        fetch_status = fetch_result[0]
        if fetch_status != "fetch_success":
            print("fetch_status: {0}; page invalid getting block".format(fetch_status))
            exit(1)
        fetch_data = fetch_result[1]
        self.local_array = self.get_array_from_buf(fetch_data)
        # restore shape
        self.local_array.shape = self.metadata.shape
        self.local_range = [start_idx, end_idx]
        return self.local_array
