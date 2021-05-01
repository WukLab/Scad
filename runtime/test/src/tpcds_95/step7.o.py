#@ type: compute
#@ parents:
#@   - step5
#@   - step6
#@ dependents:
#@   - step8
#@ corunning:
#@   5_out_mem:
#@     trans: 3_out_mem
#@     type: rdma
#@   6_out_mem:
#@     trans: 4_out_mem
#@     type: rdma
#@   7_out_mem:
#@     trans: 5_out_mem
#@     type: rdma

import pickle
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import *

from numpy import genfromtxt
from numpy.lib import recfunctions as rfn
from npjoin import join

# import pandas as pd

scheme_in = {
    "web_site_sk": np.dtype(np.float32),
    "web_site_id": np.dtype('S16'),
    "web_rec_start_date": np.dtype('S10'),
    "web_rec_end_date": np.dtype('S10'),
    "web_name": np.dtype('S50'),
    "web_open_date_sk": np.dtype(np.float32),
    "web_close_date_sk": np.dtype(np.float32),
    "web_class": np.dtype('S50'),
    "web_manager": np.dtype('S40'),
    "web_mkt_id": np.dtype(np.float32),
    "web_mkt_class": np.dtype('S50'),
    "web_mkt_desc": np.dtype('S100'),
    "web_market_manager": np.dtype('S40'),
    "web_company_id": np.dtype(np.float32),
    "web_company_name": np.dtype('S50'),
    "web_street_number": np.dtype('S10'),
    "web_street_name": np.dtype('S60'),
    "web_street_type": np.dtype('S15'),
    "web_suite_number": np.dtype('S10'),
    "web_city": np.dtype('S60'),
    "web_county": np.dtype('S30'),
    "web_state": np.dtype('S2'),
    "web_zip": np.dtype('S10'),
    "web_country": np.dtype('S20'),
    "web_gmt_offset": np.dtype(np.float32),
    "web_tax_percentage": np.dtype(np.float32),
}

def build_dtype(schema):
    return np.dtype({
        'names': list(schema.keys()),
        'formats': list(schema.values()),
    })

def main(params, action):
    tag_print = "step7"
    print(f"[tpcds] {tag_print}: begin")

    # print(f"[tpcds] {tag_print}: start reading remote array")
    trans_s5_name = '5_out_mem'
    trans_s5 = action.get_transport(trans_s5_name, 'rdma')
    trans_s5.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step5']['meta']))
    # print(f"[tpcds] {tag_print}: loading params from step5", context_dict)

    bp_s5 = buffer_pool_lib.buffer_pool({trans_s5_name: trans_s5}, context_dict["bp"])
    df_s5_arr = remote_array(bp_s5, metadata=context_dict["df"])
    cs = df_s5_arr.materialize()

    # load fron step2
    trans_s6_name = '6_out_mem'
    trans_s6 = action.get_transport(trans_s6_name, 'rdma')
    trans_s6.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step6']['meta']))
    # print(f"[tpcds] {tag_print}: loading params from step6", context_dict)

    bp_s6 = buffer_pool_lib.buffer_pool({trans_s6_name: trans_s6}, context_dict["bp"])
    df_s6_arr = remote_array(bp_s6, metadata=context_dict["df"])
    ca = df_s6_arr.materialize()
    # print(f"[tpcds] {tag_print}: finish reading rdma")

    # print(f"[tpcds] {tag_print}: start reading csv")
    tableurl = "http://localhost:8123/web_site.csv"
    csv = urllib.request.urlopen(tableurl)

    cc = genfromtxt(csv, delimiter='|', dtype=build_dtype(scheme_in))

    # build transport
    trans_s7_name = '7_out_mem'
    trans_s7 = action.get_transport(trans_s7_name, 'rdma')
    trans_s7.reg(buffer_pool_lib.buffer_size)
    bp_s7 = buffer_pool_lib.buffer_pool({trans_s7_name:trans_s7})

    # data operation
    join_meta = join.prepare_join_float32(cs['ws_ship_addr_sk'])
    df1_idx, df2_idx = join.join_on_table_float32(*join_meta, ca['ca_address_sk'])
    dfbuf = np.empty(len(df1_idx) * (cs.itemsize + ca.itemsize), dtype=np.uint8)
    df = join.structured_array_merge(dfbuf, cs, ca, df1_idx, df2_idx,
            [n for n in cs.dtype.names if n != 'ws_ship_addr_sk'],
            [n for n in ca.dtype.names])

    cc = cc[cc['web_company_name'] == 'pri'.encode()][['web_site_sk']]
    join_meta = join.prepare_join_float32(df['ws_web_site_sk'])
    df1_idx, df2_idx = join.join_on_table_float32(*join_meta, cc['web_site_sk'])

    merged_dtype = join.merge_dtypes(df, cc, 
            ['ws_order_number', 'ws_ext_ship_cost', 'ws_net_profit'],
            [])

    # init rdma array buf for join
    rdma_array = init_empty_remote_array(bp_s7, trans_s7_name, merged_dtype, (len(df1_idx),))
    buf = rdma_array.request_mem_on_buffer_for_array(0, len(df1_idx))

    df = join.structured_array_merge(buf, df, cc, df1_idx, df2_idx,
            ['ws_order_number', 'ws_ext_ship_cost', 'ws_net_profit'],
            [])

    print(f'[tpcds] {tag_print} df: ', df.itemsize, df.shape, df.dtype)

    # write back
    rdma_array.flush_slice(0, len(df1_idx))

    # debug
    # df = pd.DataFrame(data=df, columns=['cs_order_number', 'cs_ext_ship_cost', 'cs_net_profit'])
    # df.to_csv('/home/jil/serverless/Disagg-Serverless/runtime/test/src/tpcds_16/step5.csv')

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = bp_s7.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

