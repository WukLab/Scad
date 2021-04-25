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
import pandas as pd
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

from numpy.lib import recfunctions as rfn
import numpy_groupies as npg
from npjoin import join

def main(params, action):
    tag_print = "step8"
    print(f"[tpcds] {tag_print}: begin")

    # print(f"[tpcds] {tag_print}: start reading remote array")
    trans_s6_name = '6_out_mem'
    trans_s6 = action.get_transport(trans_s6_name, 'rdma')
    trans_s6.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step6']['meta']))
    # print(f"[tpcds] {tag_print}: loading params from step6", context_dict)

    bp_s6 = buffer_pool_lib.buffer_pool({trans_s6_name: trans_s6}, context_dict["bp"])
    df_s6_arr = remote_array(bp_s6, metadata=context_dict["df"])
    df6 = df_s6_arr.materialize()

    # load fron step7
    trans_s7_name = '7_out_mem'
    trans_s7 = action.get_transport(trans_s7_name, 'rdma')
    trans_s7.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step7']['meta']))
    # print(f"[tpcds] {tag_print}: loading params from step7", context_dict)

    bp_s7 = buffer_pool_lib.buffer_pool({trans_s7_name: trans_s7}, context_dict["bp"])
    df_s7_arr = remote_array(bp_s7, metadata=context_dict["df"])
    df7 = df_s7_arr.materialize()
    # print(f"[tpcds] {tag_print}: finish reading rdma")

    # build transport
    trans_s8_name = '8_out_mem'
    # print(f"[tpcds] {tag_print}: starting writing back")
    trans_s8 = action.get_transport(trans_s8_name, 'rdma')
    trans_s8.reg(buffer_pool_lib.buffer_size)

    # data opeartion
    join_meta = join.prepare_join_float32(df6['ctr_store_sk'])
    df6_idx, df7_idx = join.join_on_table_float32(*join_meta, df7['s_store_sk'])
    df8buf = np.empty(len(df6_idx) * (df6.itemsize + df7.itemsize), dtype=np.uint8)
    df8 = join.structured_array_merge(df8buf, df6, df7, df6_idx, df7_idx,
            ['ctr_store_sk', 'c_customer_id', 'ctr_total_return', 'ctr_customer_sk'],
            [])

    # df8 = df8[['ctr_store_sk', 'c_customer_id', 'ctr_total_return', 'ctr_customer_sk']]
    # print('df8', df8.dtype, df8.itemsize, df8.shape)

    # groupby
    uniques, reverses = np.unique(df8['ctr_store_sk'], axis=0, return_inverse=True)
    # print('uniques:', uniques.shape, uniques, reverses)
    groups = npg.aggregate(reverses, df8['ctr_total_return'], func='mean')
    avg = rfn.merge_arrays([uniques, groups], flatten = True, usemask = False)
    avg.dtype.names = ('ctr_store_sk', 'ctr_avg')
    # print('avg', avg.dtype, avg.itemsize, avg.shape)

    # JOIN
    # u2 = rfn.join_by('ctr_store_sk', df8, avg)
    join_meta = join.prepare_join_float32(df8['ctr_store_sk'])
    df8_idx, avg_idx = join.join_on_table_float32(*join_meta, avg['ctr_store_sk'])
    # u2buf = np.empty(len(df8_idx) * (df8.itemsize + avg.itemsize), dtype=np.uint8)
    u2 = join.structured_array_merge(trans_s8.buf, df8, avg, df8_idx, avg_idx,
            [n for n in df8.dtype.names],
            [n for n in avg.dtype.names if n != 'ctr_store_sk'])

    u2 = u2[u2['ctr_total_return'] > u2['ctr_avg']]
    print('u2', u2.dtype, u2.itemsize, u2.shape)

    # unique and sort
    final = np.unique(u2['c_customer_id'])
    final = np.sort(final)
    print('final', final.dtype, final.itemsize, final.shape)

    df = final

    # write back
    bp_s8 = buffer_pool_lib.buffer_pool({trans_s8_name: trans_s8})
    rdma_array = remote_array(bp_s8, input_ndarray=df, transport_name=trans_s8_name)

    '''
    # check with reference csv
    real_final = pd.DataFrame(np.ma.getdata(final), columns=['c_customer_id'])
    # real_df3.to_csv('./tmp/df3.csv')
    correct_final = pd.read_csv('/home/jil/serverless/Disagg-Serverless/runtime/test/src/tpcds_1/ref_output_final.csv', encoding='utf8', dtype={'c_customer_id':'S16'})
    correct_final = correct_final.select_dtypes(include=object).astype(np.dtype('S16'))
    print('Checking final...')
    print('real', real_final.dtypes, real_final.shape)
    print(real_final)
    print('correct', correct_final.dtypes, correct_final.shape)
    print(correct_final)
    print("[DEBUG] equals?", real_final.equals(correct_final))
    '''

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = bp_s8.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

