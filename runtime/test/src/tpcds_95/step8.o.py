#@ type: compute
#@ parents:
#@   - step7
#@ corunning:
#@   7_out_mem:
#@     trans: 5_out_mem
#@     type: rdma
#@   8_out_mem:
#@     trans: 6_out_mem
#@     type: rdma

import pickle
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

# import pandas as pd

def main(params, action):
    tag_print = "step8"
    print(f"[tpcds] {tag_print}: begin")

    # print(f"[tpcds] {tag_print}: start reading remote array")
    trans_s7_name = '7_out_mem'
    trans_s7 = action.get_transport(trans_s7_name, 'rdma')
    trans_s7.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step7']['meta']))
    # print(f"[tpcds] {tag_print}: loading params from step7", context_dict)

    bp_s7 = buffer_pool_lib.buffer_pool({trans_s7_name: trans_s7}, context_dict["bp"])
    df_s7_arr = remote_array(bp_s7, metadata=context_dict["df"])
    cs = df_s7_arr.materialize()

    # debug
    # df = pd.DataFrame(data=cs, columns=['cs_order_number', 'cs_ext_ship_cost', 'cs_net_profit'])
    # df.to_csv('/home/jil/serverless/Disagg-Serverless/runtime/test/src/tpcds_16/step5_6.csv')

    # data opeartion
    a1 = np.unique(cs[['ws_order_number']]).size
    a2 = np.nansum(cs['ws_ext_ship_cost'])
    a3 = np.nansum(cs['ws_net_profit'])

    print("[tpcds] final query result: ", a1, a2, a3)

    # transfer the metedata
    context_dict = {}

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

