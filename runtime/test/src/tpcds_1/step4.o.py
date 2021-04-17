#@ type: compute
#@ parents:
#@   - step3
#@ dependents:
#@   - step6
#@ corunning:
#@   3_out_mem:
#@     trans: 3_out_mem
#@     type: rdma
#@   4_out_mem:
#@     trans: 4_out_mem
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
    tag_print = "step4"

    print(f"[tpcds] {tag_print}: start reading remote array")
    # for now, we create two different buffer pool; may change latter
    # load fron step3
    trans_s3 = action.get_transport('3_out_mem', 'rdma')
    trans_s3.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step3']['meta']))
    print(f"[tpcds] {tag_print}: loading params from step3", context_dict, params['step3']['columns'])

    bp_s3 = buffer_pool_lib.buffer_pool(trans_s3, context_dict["bp"])
    df_s3_arr = remote_array(bp_s3, metadata=context_dict["df"])
    df_s3_np = df_s3_arr.materialize()
    d = pd.DataFrame(data=df_s3_np, columns=params['step3']['columns'])

    df = d.groupby(['sr_customer_sk', 'sr_store_sk']).agg(
        {'sr_return_amt': 'sum'}).reset_index()
    df.rename(columns={'sr_store_sk': 'ctr_store_sk',
                       'sr_customer_sk': 'ctr_customer_sk',
                       'sr_return_amt': 'ctr_total_return'
                       }, inplace=True)

    # build transport
    print(f"[tpcds] {tag_print}: starting writing back")
    trans_s4 = action.get_transport('4_out_mem', 'rdma')
    trans_s4.reg(buffer_pool_lib.buffer_size)

    # write back
    to_write = df.to_numpy()

    bp_s4 = buffer_pool_lib.buffer_pool(trans_s4)
    rdma_array = remote_array(bp_s4, input_ndarray=to_write)

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = bp_s4.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'columns': list(df.columns),
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

