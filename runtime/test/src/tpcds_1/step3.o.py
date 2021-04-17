#@ type: compute
#@ parents:
#@   - step1
#@   - step2
#@ dependents:
#@   - step4
#@ corunning:
#@   1_out_mem:
#@     trans: 1_out_mem
#@     type: rdma
#@   2_out_mem:
#@     trans: 2_out_mem
#@     type: rdma
#@   3_out_mem:
#@     trans: 3_out_mem
#@     type: rdma

import pickle
import os
import datetime
import pandas as pd
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

def main(params, action):
    tag_print = "step3"
    print(f"[tpcds] {tag_print}: begin")

    print(f"[tpcds] {tag_print}: start reading remote array")
    # for now, we create two different buffer pool; may change latter
    # load fron step1
    trans_s1 = action.get_transport('1_out_mem', 'rdma')
    trans_s1.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step1']['meta']))
    print(f"[tpcds] {tag_print}: loading params from step1", context_dict, params['step1']['columns'])

    bp_s1 = buffer_pool_lib.buffer_pool(trans_s1, context_dict["bp"])
    df_s1_arr = remote_array(bp_s1, metadata=context_dict["df"])
    df_s1_np = df_s1_arr.materialize()
    d = pd.DataFrame(data=df_s1_np, columns=params['step1']['columns'])

    # load fron step2
    trans_s2 = action.get_transport('2_out_mem', 'rdma')
    trans_s2.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step2']['meta']))
    print(f"[tpcds] {tag_print}: loading params from step2", context_dict, params['step2']['columns'])

    bp_s2 = buffer_pool_lib.buffer_pool(trans_s2, context_dict["bp"])
    df_s2_arr = remote_array(bp_s2, metadata=context_dict["df"])
    df_s2_np = df_s2_arr.materialize()
    sr = pd.DataFrame(data=df_s2_np, columns=params['step2']['columns'])
    print(f"[tpcds] {tag_print}: finish reading csv")

    df = d.merge(sr, left_on='d_date_sk', right_on='sr_returned_date_sk')
    df.drop('d_date_sk', axis=1, inplace=True)

    # build transport
    print(f"[tpcds] {tag_print}: starting writing back")
    trans_s3 = action.get_transport('3_out_mem', 'rdma')
    trans_s3.reg(buffer_pool_lib.buffer_size)

    # write back
    to_write = df.to_numpy()

    bp_s3 = buffer_pool_lib.buffer_pool(trans_s3)
    rdma_array = remote_array(bp_s3, input_ndarray=to_write)

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = bp_s3.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'columns': list(df.columns),
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

