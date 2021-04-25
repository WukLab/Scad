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

from numpy import genfromtxt
from numpy.lib import recfunctions as rfn
import numpy_groupies as npg
from npjoin import join

def main(params, action):
    tag_print = "step6"

    print(f"[tpcds] {tag_print}: start reading remote array")
    # for now, we create two different buffer pool; may change latter
    # load fron step4
    trans_s4_name = '4_out_mem'
    trans_s4 = action.get_transport(trans_s4_name, 'rdma')
    trans_s4.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step4']['meta']))
    print(f"[tpcds] {tag_print}: loading params from step4", context_dict)

    bp_s4 = buffer_pool_lib.buffer_pool({trans_s4_name: trans_s4}, context_dict["bp"])
    df_s4_arr = remote_array(bp_s4, metadata=context_dict["df"])
    df4 = df_s4_arr.materialize()

    # load fron step5
    trans_s5_name = '5_out_mem'
    trans_s5 = action.get_transport(trans_s5_name, 'rdma')
    trans_s5.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step5']['meta']))
    print(f"[tpcds] {tag_print}: loading params from step5", context_dict)

    bp_s5 = buffer_pool_lib.buffer_pool({trans_s5_name: trans_s5}, context_dict["bp"])
    df_s5_arr = remote_array(bp_s5, metadata=context_dict["df"])
    df5 = df_s5_arr.materialize()
    print(f"[tpcds] {tag_print}: finish reading rdma")

    join_meta = join.prepare_join_float32(df5['c_customer_sk'])
    df5_idx, df4_idx = join.join_on_table_float32(*join_meta, df4['ctr_customer_sk'])
    df6buf = np.empty(len(df4_idx) * (df4.itemsize + df5.itemsize), dtype=np.uint8)
    df = join.structured_array_merge(df6buf, df4, df5, df4_idx, df5_idx,
            ['ctr_store_sk', 'ctr_customer_sk', 'ctr_total_return'],
            ['c_customer_id'])

    # build transport
    trans_s6_name = '6_out_mem'
    print(f"[tpcds] {tag_print}: starting writing back")
    trans_s6 = action.get_transport(trans_s6_name, 'rdma')
    trans_s6.reg(buffer_pool_lib.buffer_size)

    # write back
    bp_s6 = buffer_pool_lib.buffer_pool({trans_s6_name: trans_s6})
    rdma_array = remote_array(bp_s6, input_ndarray=df, transport_name=trans_s6_name)

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = bp_s6.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

