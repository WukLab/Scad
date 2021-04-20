import sys
import copy
import collections
import numpy as np
from math import ceil
from . import buffer_pool_lib
from collections import OrderedDict
import json

def prod(it):
    p = 1
    for e in it:
        p = p * e
    return p


class remote_array_metadata:
    def __init__(self, remote_mem_metadata, cur_mem_size, dtype, element_byte, element_per_block, array_shape):
        self.dtype = dtype
        self.element_byte = element_byte
        self.element_per_block = element_per_block 
        self.remote_mem_metadata = remote_mem_metadata
        self.shape = array_shape
        self.cur_mem_size = cur_mem_size
    
    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, dict):
        self.__dict__ = dict

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)


# this function merges several arrays metadata into one metadata; i.e. merge several slices into one big array
# we combine the array in order of metadata_list
# @todo replace to kwargs
def merge_arrays_metadata(metadata_list):
    remote_mem_metadata = OrderedDict()
    cur_start_idx = 0
    element_byte = metadata_list[0].element_byte
    element_per_block = metadata_list[0].element_per_block
    dtype = metadata_list[0].dtype
    merged_array_size = 0
    merged_array_shape = []
    for metadata_per_transport in metadata_list:
        sub_remote_mem_metadata = metadata_per_transport.remote_mem_metadata
        merged_array_size = merged_array_size + metadata_per_transport.cur_mem_size
        cur_shape = list(metadata_per_transport.shape)
        if len(merged_array_shape) == 0:
            merged_array_shape = cur_shape
        else:
            merged_array_shape[0] = merged_array_shape[0] + cur_shape[0]
        if element_byte != metadata_per_transport.element_byte:
            print("merging different type arrays; merge failure")
            return OrderedDict()
        for sub_metadata_start_idx, sub_metadata_info in sub_remote_mem_metadata.items():
            sub_transport_name, sub_remote_addr, sub_mem_size_in_byte = sub_metadata_info
            num_of_element = sub_mem_size_in_byte // element_byte
            remote_mem_metadata[cur_start_idx] = [sub_transport_name, sub_remote_addr, sub_mem_size_in_byte]
            cur_start_idx = cur_start_idx + num_of_element
    return remote_array_metadata(remote_mem_metadata, merged_array_size, dtype, element_byte, element_per_block, tuple(merged_array_shape))

# @todo add asarray
# remote mem metadata layout [transport_name, remote_addr, cur_mem_size]
class remote_array():
    # @todo current input should be changed to block iterator
    def __init__(self, buffer_pool, input_ndarray = None, transport_name = "", metadata = None):
        # init from a real array or a metadata
        self.buffer_pool = buffer_pool
        # indicating whether the array is materialized on local
        self.local_array = None
        if input_ndarray is not None:
            array_shape = input_ndarray.shape
            input_ndarray = input_ndarray.ravel()
            cur_mem_size = len(input_ndarray)
            block_size = buffer_pool_lib.page_size
            element_byte = input_ndarray.itemsize
            assert(block_size % element_byte == 0)
            element_per_block = int(block_size / element_byte)
            input_array_in_byte = input_ndarray.tobytes()
            if transport_name == "":
                print("fail to provide transport name; app stop")
                exit(1)
            write_result_list = self.buffer_pool.write(transport_name, input_array_in_byte)
            remote_mem_metadata = self.gen_metadata_from_write_result(write_result_list, element_byte)
            if len(remote_mem_metadata) == 0:
                print("write data failure due to start address prblem")
                exit(1)
            else:
                self.metadata = remote_array_metadata(remote_mem_metadata, cur_mem_size, input_ndarray.dtype, element_byte, element_per_block, array_shape)
        else:
            self.metadata = metadata

    def gen_metadata_from_write_result(self, write_result_list, element_byte):
        remote_mem_metadata = OrderedDict()
        cur_trans_port_name, cur_remote_addr, cur_mem_size_in_byte = write_result_list[0]
        # currently we write to only one remote memory server, so the key(start_idx) set to 0
        remote_mem_metadata[0] = [cur_trans_port_name, cur_remote_addr, cur_mem_size_in_byte]
        return remote_mem_metadata
        
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

    def get_array_metadata(self):
        return self.metadata

    # return the remote_metadata list sorted by idx [request_start_idx, request_end_idx)
    def get_metadata_for_bp_from_idx(self, request_start_idx, request_end_idx):
        remote_metadata_list = []
        res_mem_size_in_byte = 0
        # per element in metadata list [transport name, remote addr, size_in_byte]
        for cur_start_idx, block_info in self.metadata.remote_mem_metadata.items():
            cur_trans_port_name, cur_remote_addr, cur_mem_size_in_byte = block_info
            num_of_element_in_cur_block= cur_mem_size_in_byte / self.metadata.element_byte 
            cur_end_idx = cur_start_idx + num_of_element_in_cur_block
            # read the following block
            if cur_end_idx < request_start_idx:
                continue
            # reset remote addr if we do not read from start of current stat idx
            if request_start_idx > cur_start_idx:
                cur_remote_addr = cur_remote_addr + (request_start_idx - cur_start_idx) * self.metadata.element_byte
            # update cur block start idx since we may need when calcualte how many mem to read
            cur_block_start_idx = max(request_start_idx, cur_start_idx)
            if request_end_idx < cur_end_idx:
                cur_mem_size_in_byte = (request_end_idx - cur_block_start_idx) * self.metadata.element_byte
                res_mem_size_in_byte = res_mem_size_in_byte + cur_mem_size_in_byte
                remote_metadata_list.append([cur_trans_port_name, cur_remote_addr, cur_mem_size_in_byte])
                break
            else:
                remote_metadata_list.append([cur_trans_port_name, cur_remote_addr, cur_mem_size_in_byte])
                res_mem_size_in_byte = res_mem_size_in_byte + cur_mem_size_in_byte
        assert(len(remote_metadata_list) > 0)
        return remote_metadata_list, res_mem_size_in_byte
 
    # follow numpy and python list semantic -> [) shallow copy; currently has offset problem
    def get_slice(self, start_idx, end_idx):
        pass
        # element_per_block = self.metadata.element_per_block
        # start_addr = self.get_remote_addr_from_idx(start_idx)
        # end_addr = self.get_remote_addr_from_idx(end_idx)
        # cur_metadata = self.metadata
        # cur_shape = list(cur_metadata.shape)
        # cur_shape[0] = end_idx - start_idx
        # slice_mem_size = end_addr - start_addr
        # slice_metadata = remote_array_metadata(start_addr, slice_mem_size, cur_metadata.dtype, cur_metadata.element_byte, cur_metadata.element_per_block,tuple(cur_shape))
        # return remote_array(self.buffer_pool, metadata = slice_metadata)

    # load remote_array[start_idx:end_idx) to local buffer; read only
    # @todo support finer granularity of array; for now we pull the whole array to local
    def materialize(self, start_idx = -1, end_idx = -1):
        if start_idx == -1:
            start_idx = 0
        if end_idx == -1:
            end_idx = prod(self.metadata.shape)
        bp_metadata_list, cur_mem_size = self.get_metadata_for_bp_from_idx(start_idx, end_idx)
        fetch_status, fetch_data = self.buffer_pool.read(bp_metadata_list, cur_mem_size)
        if fetch_status != True:
            print("fetch_status: {0}; page invalid getting block".format(fetch_status))
            exit(1)
        self.local_array = self.get_array_from_buf(fetch_data)
        # restore shape
        # assert(self.local_array != None)
        self.local_array.shape = self.metadata.shape
        return self.local_array
