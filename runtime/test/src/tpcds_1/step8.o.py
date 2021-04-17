#@ type: compute
#@ parents:
#@   - step6
#@   - step7
#@ corunning:
#@   6_out_mem:
#@     trans: 6_out_mem
#@     type: rdma
#@   7_out_mem:
#@     trans: 7_out_mem
#@     type: rdma
#@   8_out_mem:
#@     trans: 8_out_mem
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
    tag_print = "step8"

    print(f"[tpcds] {tag_print}: start reading remote array")
    # for now, we create two different buffer pool; may change latter
    # load fron step6
    trans_s6 = action.get_transport('6_out_mem', 'rdma')
    trans_s6.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step6']['meta']))
    print(f"[tpcds] {tag_print}: loading params from step6", context_dict, params['step6']['columns'])

    bp_s6 = buffer_pool_lib.buffer_pool(trans_s6, context_dict["bp"])
    df_s6_arr = remote_array(bp_s6, metadata=context_dict["df"])
    df_s6_np = df_s6_arr.materialize()
    d = pd.DataFrame(data=df_s6_np, columns=params['step6']['columns'])

    # load fron step7
    trans_s7 = action.get_transport('7_out_mem', 'rdma')
    trans_s7.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step7']['meta']))
    print(f"[tpcds] {tag_print}: loading params from step7", context_dict, params['step7']['columns'])

    bp_s7 = buffer_pool_lib.buffer_pool(trans_s7, context_dict["bp"])
    df_s7_arr = remote_array(bp_s7, metadata=context_dict["df"])
    df_s7_np = df_s7_arr.materialize()
    sr = pd.DataFrame(data=df_s7_np, columns=params['step7']['columns'])
    print(f"[tpcds] {tag_print}: finish reading csv")

    merged = d.merge(sr, left_on='ctr_store_sk', right_on='s_store_sk')
    merged_u = merged[['ctr_store_sk', 'c_customer_id',
                       'ctr_total_return', 'ctr_customer_sk']]
  
    r_avg = merged_u.groupby(['ctr_store_sk']).agg(
        {'ctr_total_return': 'mean'}).reset_index()
    r_avg.rename(columns={'ctr_total_return': 'ctr_avg'}, inplace=True)
  
    #print(r_avg)
    #print("aaa")
    #aaa = [d.empty, sr.empty, merged_u.empty, r_avg.empty]
    #print(",".join([str(a) for a in aaa]))
    merged_u2 = merged_u.merge(
        r_avg, left_on='ctr_store_sk', right_on='ctr_store_sk')
    final_merge = merged_u2[merged_u2['ctr_total_return']
                            > merged_u2['ctr_avg']]
    final = final_merge[['c_customer_id']].drop_duplicates().sort_values(by=[
        'c_customer_id'])
    #print(final)
    #print(cc['cc_country'])

    df = merged_u

    # build transport
    print(f"[tpcds] {tag_print}: starting writing back")
    trans_s8 = action.get_transport('8_out_mem', 'rdma')
    trans_s8.reg(buffer_pool_lib.buffer_size)

    # write back
    to_write = df.to_numpy()

    bp_s8 = buffer_pool_lib.buffer_pool(trans_s8)
    rdma_array = remote_array(bp_s8, input_ndarray=to_write)

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = bp_s8.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'columns': list(df.columns),
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

