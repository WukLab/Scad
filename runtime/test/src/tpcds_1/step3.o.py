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
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import *

from npjoin import join

def main(params, action):
    tag_print = "step3"
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
    df1 = df_s1_arr.materialize()

    # load fron step2
    trans_s2_name = '2_out_mem'
    trans_s2 = action.get_transport(trans_s2_name, 'rdma')
    trans_s2.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step2']['meta']))
    # print(f"[tpcds] {tag_print}: loading params from step2", context_dict)

    bp_s2 = buffer_pool_lib.buffer_pool({trans_s2_name: trans_s2}, context_dict["bp"])
    df_s2_arr = remote_array(bp_s2, metadata=context_dict["df"])
    df2 = df_s2_arr.materialize()
    # print(f"[tpcds] {tag_print}: finish reading rdma")

    # build transport
    trans_s3_name = '3_out_mem'
    # print(f"[tpcds] {tag_print}: starting writing back")
    trans_s3 = action.get_transport(trans_s3_name, 'rdma')
    trans_s3.reg(buffer_pool_lib.buffer_size)

    # data operation
    join_meta = join.prepare_join_float32(df1['d_date_sk'])
    df1_idx, df2_idx = join.join_on_table_float32(*join_meta, df2['sr_returned_date_sk'])
    # print('indexes', len(df1_idx), len(df2_idx), df1_idx, df2_idx)
    # df3buf = np.empty(len(df1_idx) * (df1.itemsize + df2.itemsize), dtype=np.uint8)
    # print('df3buf', df3buf.shape, 'itemsize', df1.itemsize + df2.itemsize)
    # print('trans_s3.buf', trans_s3.buf.shape, 'itemsize', trans_s3.buf.itemsize)
    # df = join.structured_array_merge(df3buf, df1, df2, df1_idx, df2_idx,

    # init rdma array, and allocate buffer
    bp_s3 = buffer_pool_lib.buffer_pool({trans_s3_name:trans_s3})
    merged_dtype = join.merge_dtypes(df1, df2, 
            [n for n in df1.dtype.names if n != 'd_date_sk'],
            [n for n in df2.dtype.names if n != 'sr_returned_date_sk'])
    rdma_array = init_empty_remote_array(bp_s3, "3_out_mem", merged_dtype, (len(df1_idx),))
    buf = rdma_array.request_mem_on_buffer_for_array(0, len(df1_idx))

    df = join.structured_array_merge(buf, df1, df2, df1_idx, df2_idx,
            [n for n in df1.dtype.names if n != 'd_date_sk'],
            [n for n in df2.dtype.names if n != 'sr_returned_date_sk'])
    # print(f'[tpcds] {tag_print} df: ', df.itemsize, df.shape, df.dtype)

    # write back
    rdma_array.flush_slice(0, len(df1_idx))

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = bp_s3.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

