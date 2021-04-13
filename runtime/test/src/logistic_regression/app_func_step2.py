import:
  - rdma_array.py
  - 

import struct
import threading
import time
import pickle
import random
import sys
import copy
import codecs
import copyreg
import collections
import numpy as np
import buffer_pool_lib
import json
from random import randrange
from rdma_array import remote_array

def main(context_dict, action):
    # read metadata to setup
    action = buffer_pool_lib.action_setup()
    transport_name = 'client1'
    trans = action.get_transport(transport_name, 'rdma')
    trans.reg(buffer_pool_lib.buffer_size)
    buffer_pool = buffer_pool_lib.buffer_pool(trans, context_dict["buffer_pool_metadata"])
    load_csv_dataset_remote = remote_array(buffer_pool, metadata=context_dict["remote_input"])
    dataset = load_csv_dataset_remote.materialize()
    # preprocess
    dataset_minmax_minmax = list()
    for dataset_minmax_i in range(len(dataset[0])):
        dataset_minmax_col_values = [dataset_minmax_row[dataset_minmax_i] for dataset_minmax_row in dataset]
        dataset_minmax_value_min = min(dataset_minmax_col_values)
        dataset_minmax_value_max = max(dataset_minmax_col_values)
        dataset_minmax_minmax.append([dataset_minmax_value_min, dataset_minmax_value_max])
    minmax = dataset_minmax_minmax
    for normalize_dataset_row in dataset:
        for normalize_dataset_i in range(len(normalize_dataset_row)):
            normalize_dataset_row[normalize_dataset_i] = ((normalize_dataset_row[normalize_dataset_i] - minmax[normalize_dataset_i][0]) / (minmax[normalize_dataset_i][1] - minmax[normalize_dataset_i][0]))
    n_folds = 5
    l_rate = 0.1
    n_epoch = 100
    random.seed(1)
    cross_validation_split_dataset_split = list()
    cross_validation_split_dataset_copy = list(dataset)
    cross_validation_split_fold_size = int((len(dataset) / n_folds))
    for cross_validation_split_i in range(n_folds):
        cross_validation_split_fold = list()
        while (len(cross_validation_split_fold) < cross_validation_split_fold_size):
            cross_validation_split_index = randrange(len(cross_validation_split_dataset_copy))
            cross_validation_split_fold.append(cross_validation_split_dataset_copy.pop(cross_validation_split_index))
        cross_validation_split_dataset_split.append(cross_validation_split_fold)
    cross_validation_split_dataset_split_in_numpy = np.asarray(cross_validation_split_dataset_split)
    remote_cv_split = remote_array(buffer_pool, input_ndarray=cross_validation_split_dataset_split_in_numpy)

    # update context
    context_dict["remote_cv_split"] = remote_cv_split.get_array_metadata()
    print(context_dict["remote_cv_split"])
    context_dict["buffer_pool_metadata"] = buffer_pool.get_buffer_metadata()
    buffer_pool_lib.write_params(context_dict)

action = buffer_pool_lib.action_setup()
context_dict = buffer_pool_lib.read_params()
main(context_dict, action)
