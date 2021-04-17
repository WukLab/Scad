#@ type: compute
#@ parents:
#@   - step4
#@   - step5
#@ dependents:
#@   - step8
#@ corunning:
#@   4_out_mem:
#@     trans: 4_out_mem
#@     type: rdma
#@   5_out_mem:
#@     trans: 5_out_mem
#@     type: rdma
#@   6_out_mem:
#@     trans: 6_out_mem
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
    tag_print = "step6"

    print(f"[tpcds] {tag_print}: start reading remote array")
    # for now, we create two different buffer pool; may change latter
    # load fron step4
    trans_s4 = action.get_transport('4_out_mem', 'rdma')
    trans_s4.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step4']['meta']))
    print(f"[tpcds] {tag_print}: loading params from step4", context_dict, params['step4']['columns'])

    bp_s4 = buffer_pool_lib.buffer_pool(trans_s4, context_dict["bp"])
    df_s4_arr = remote_array(bp_s4, metadata=context_dict["df"])
    df_s4_np = df_s4_arr.materialize()
    d = pd.DataFrame(data=df_s4_np, columns=params['step4']['columns'])

    # load fron step5
    trans_s5 = action.get_transport('5_out_mem', 'rdma')
    trans_s5.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step5']['meta']))
    print(f"[tpcds] {tag_print}: loading params from step5", context_dict, params['step5']['columns'])

    bp_s5 = buffer_pool_lib.buffer_pool(trans_s5, context_dict["bp"])
    df_s5_arr = remote_array(bp_s5, metadata=context_dict["df"])
    df_s5_np = df_s5_arr.materialize()
    sr = pd.DataFrame(data=df_s5_np, columns=params['step5']['columns'])
    print(f"[tpcds] {tag_print}: finish reading csv")

    df = d.merge(sr, left_on='ctr_customer_sk', right_on='c_customer_sk')
    df = df[['c_customer_id','ctr_store_sk','ctr_customer_sk','ctr_total_return']]

    # build transport
    print(f"[tpcds] {tag_print}: starting writing back")
    trans_s6 = action.get_transport('6_out_mem', 'rdma')
    trans_s6.reg(buffer_pool_lib.buffer_size)

    # write back
    to_write = df.to_numpy()

    bp_s6 = buffer_pool_lib.buffer_pool(trans_s6)
    rdma_array = remote_array(bp_s6, input_ndarray=to_write)

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = bp_s6.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'columns': list(df.columns),
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

