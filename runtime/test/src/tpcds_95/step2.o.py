#@ type: compute
#@ parents:
#@   - step1
#@ dependents:
#@   - step5
#@ corunning:
#@   1_out_mem:
#@     trans: 1_out_mem
#@     type: rdma
#@   2_out_mem:
#@     trans: 2_out_mem
#@     type: rdma

import pickle
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import *
from numpy_groupies.aggregate_numpy import aggregate

from npjoin import join
from numpy.lib import recfunctions as rfn

def main(params, action):
    tag_print = "step2"
    print(f"[tpcds] {tag_print}: begin")

    # print(f"[tpcds] {tag_print}: start reading remote array")
    # for now, we create two different buffer pool; may change latter
    # load fron step1
    trans_s1_name = '1_out_mem'
    trans_s1 = action.get_transport(trans_s1_name, 'rdma')
    trans_s1.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step1']['meta']))
    # print(f"[tpcds] {tag_print}: loading params from step1", context_dict)

    bp_s1 = buffer_pool_lib.buffer_pool({trans_s1_name: trans_s1}, context_dict["bp"])
    df_s1_arr = remote_array(bp_s1, metadata=context_dict["df"])
    cs = df_s1_arr.materialize()

    # data operation
    # TODO: cs_tmp necessary?
    cs_tmp = cs[['ws_order_number', 'ws_warehouse_sk']]
    cs_tmp = cs_tmp[~np.isnan(cs_tmp['ws_order_number']) & ~np.isnan(cs_tmp['ws_warehouse_sk'])]
    uniques, reverses = np.unique(cs_tmp[['ws_order_number']], return_inverse=True)
    groups = aggregate(reverses, cs_tmp['ws_warehouse_sk'], lambda a: len(set(a)))
    wh_uc = rfn.merge_arrays([uniques, groups], flatten = True, usemask = False)
    wh_uc.dtype.names = ('ws_order_number', 'ws_warehouse_sk')
    target_order_numbers = wh_uc[wh_uc['ws_warehouse_sk'] > 1]['ws_order_number']

    cs_sj_f1 = np.unique(cs[['ws_order_number']])
    df = cs_sj_f1[np.isin(cs_sj_f1['ws_order_number'], target_order_numbers)]

    # build transport
    trans_s2_name = '2_out_mem'
    trans_s2 = action.get_transport(trans_s2_name, 'rdma')
    trans_s2.reg(buffer_pool_lib.buffer_size)

    # write back
    buffer_pool = buffer_pool_lib.buffer_pool({trans_s2_name:trans_s2})
    rdma_array = remote_array(buffer_pool, input_ndarray=df, transport_name=trans_s2_name)

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = buffer_pool.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

