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
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

from numpy.lib import recfunctions as rfn
import numpy_groupies as npg

def main(params, action):
    tag_print = "step4"
    print(f"[tpcds] {tag_print}: begin")

    # print(f"[tpcds] {tag_print}: start reading remote array")
    # for now, we create two different buffer pool; may change latter
    # load fron step3
    trans_s3_name = '3_out_mem'
    trans_s3 = action.get_transport(trans_s3_name, 'rdma')
    trans_s3.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step3']['meta']))
    # print(f"[tpcds] {tag_print}: loading params from step3", context_dict)

    bp_s3 = buffer_pool_lib.buffer_pool({trans_s3_name: trans_s3}, context_dict["bp"])
    df_s3_arr = remote_array(bp_s3, metadata=context_dict["df"])
    df3 = df_s3_arr.materialize()

    # data operation
    df3 = df3[~np.isnan(df3['sr_customer_sk'])&~np.isnan(df3['sr_store_sk'])]
    uniques, reverses = np.unique(df3[['sr_customer_sk', 'sr_store_sk']], return_inverse=True)
    groups = npg.aggregate(reverses, df3['sr_return_amt'], func='sum')
    df = rfn.merge_arrays([uniques, groups], flatten = True, usemask = False)
    df.dtype.names = ('ctr_customer_sk', 'ctr_store_sk', 'ctr_total_return')
    # print(f'[tpcds] {tag_print} df: ', df.itemsize, df.shape, df.dtype)
    
    # build transport
    trans_s4_name = '4_out_mem'
    # print(f"[tpcds] {tag_print}: starting writing back")
    trans_s4 = action.get_transport(trans_s4_name, 'rdma')
    trans_s4.reg(buffer_pool_lib.buffer_size)

    # write back
    bp_s4 = buffer_pool_lib.buffer_pool({trans_s4_name: trans_s4})
    rdma_array = remote_array(bp_s4, input_ndarray=df, transport_name=trans_s4_name)

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = bp_s4.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

