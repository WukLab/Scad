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
import buffer_pool_lib
import json
from rdma_array import remote_array


def main(context_dict, action):
    # setup
    action = buffer_pool_lib.action_setup()
    transport_name = 'client1'
    trans = action.get_transport(transport_name, 'rdma')
    trans.reg(buffer_pool_lib.buffer_size)
    buffer_pool = buffer_pool_lib.buffer_pool(trans)

    # loading data
    filename = 'data/pima-indians-diabetes.csv'
    load_csv_dataset = np.genfromtxt(filename, delimiter=',')
    remote_input = remote_array(buffer_pool, input_ndarray=load_csv_dataset)
    # update context
    remote_input_metadata = remote_input.get_array_metadata()
    context_dict["remote_input"] = remote_input_metadata
    context_dict["buffer_pool_metadata"] = buffer_pool.get_buffer_metadata()
    # byteDict = pickle.dumps(context_dict)
    # @todo write to file
    buffer_pool_lib.write_params(context_dict)



action = buffer_pool_lib.action_setup()
context_dict = dict()
main(context_dict, action)