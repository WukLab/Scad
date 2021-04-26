#@ type: compute
#@ parents:
#@   - step3
#@   - step4
#@ dependents:
#@   - step6
#@ corunning:
#@   3_out_mem:
#@     trans: 3_out_mem
#@     type: rdma
#@   4_out_mem:
#@     trans: 4_out_mem
#@     type: rdma
#@   5_out_mem:
#@     trans: 5_out_mem
#@     type: rdma

import pickle
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

from numpy import genfromtxt
from numpy.lib import recfunctions as rfn
from npjoin import join

scheme_in = {
  "cc_call_center_sk": np.dtype(np.float32) ,
  "cc_call_center_id": np.dtype('S16') ,
  "cc_rec_start_date": np.dtype('S10') ,
  "cc_rec_end_date": np.dtype('S10') ,
  "cc_closed_date_sk": np.dtype(np.float32) ,
  "cc_open_date_sk": np.dtype(np.float32) ,
  "cc_name": np.dtype('S50') ,
  "cc_class": np.dtype('S50') ,
  "cc_employees": np.dtype(np.float32) ,
  "cc_sq_ft": np.dtype(np.float32) ,
  "cc_hours": np.dtype('S20') ,
  "cc_manager": np.dtype('S40') ,
  "cc_mkt_id": np.dtype(np.float32) ,
  "cc_mkt_class": np.dtype('S50') ,
  "cc_mkt_desc": np.dtype('S100') ,
  "cc_market_manager": np.dtype('S40') ,
  "cc_division": np.dtype(np.float32) ,
  "cc_division_name": np.dtype('S50') ,
  "cc_company": np.dtype(np.float32) ,
  "cc_company_name": np.dtype('S50') ,
  "cc_street_number": np.dtype('S10') ,
  "cc_street_name": np.dtype('S60') ,
  "cc_street_type": np.dtype('S15') ,
  "cc_suite_number": np.dtype('S10') ,
  "cc_city": np.dtype('S60') ,
  "cc_county": np.dtype('S30') ,
  "cc_state": np.dtype('S2') ,
  "cc_zip": np.dtype('S10') ,
  "cc_country": np.dtype('S20') ,
  "cc_gmt_offset": np.dtype(np.float32) ,
  "cc_tax_percentage": np.dtype(np.float32) ,
}

def build_dtype(schema):
    return np.dtype({
        'names': list(schema.keys()),
        'formats': list(schema.values()),
    })

def main(params, action):
    tag_print = "step5"
    print(f"[tpcds] {tag_print}: begin")

    # print(f"[tpcds] {tag_print}: start reading remote array")
    # for now, we create two different buffer pool; may change latter
    # load fron step1
    trans_s3_name = '3_out_mem'
    trans_s3 = action.get_transport(trans_s3_name, 'rdma')
    trans_s3.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step3']['meta']))
    # print(f"[tpcds] {tag_print}: loading params from step3", context_dict)

    bp_s3 = buffer_pool_lib.buffer_pool({trans_s3_name: trans_s3}, context_dict["bp"])
    df_s3_arr = remote_array(bp_s3, metadata=context_dict["df"])
    cs = df_s3_arr.materialize()

    # load fron step2
    trans_s4_name = '4_out_mem'
    trans_s4 = action.get_transport(trans_s4_name, 'rdma')
    trans_s4.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step4']['meta']))
    # print(f"[tpcds] {tag_print}: loading params from step4", context_dict)

    bp_s4 = buffer_pool_lib.buffer_pool({trans_s4_name: trans_s4}, context_dict["bp"])
    df_s4_arr = remote_array(bp_s4, metadata=context_dict["df"])
    ca = df_s4_arr.materialize()
    # print(f"[tpcds] {tag_print}: finish reading rdma")

    # print(f"[tpcds] {tag_print}: start reading csv")
    tableurl = "http://localhost:8123/call_center.csv"
    csv = urllib.request.urlopen(tableurl)

    cc = genfromtxt(csv, delimiter='|', dtype=build_dtype(scheme_in))

    # build transport
    trans_s5_name = '5_out_mem'
    # print(f"[tpcds] {tag_print}: starting writing back")
    trans_s5 = action.get_transport(trans_s5_name, 'rdma')
    trans_s5.reg(buffer_pool_lib.buffer_size)

    # data operation
    join_meta = join.prepare_join_float32(cs['cs_ship_addr_sk'])
    df1_idx, df2_idx = join.join_on_table_float32(*join_meta, ca['ca_address_sk'])
    df = join.structured_array_merge(trans_s3.buf, cs, ca, df1_idx, df2_idx,
            [n for n in cs.dtype.names if n != 'cs_ship_addr_sk'],
            [n for n in ca.dtype.names])
    # print(f'[tpcds] {tag_print} df: ', df.itemsize, df.shape, df.dtype)
    list_addr = ['Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County']
    cc = cc[np.isin(cc['cc_county'], list_addr)][['cc_call_center_sk']]
    join_meta = join.prepare_join_float32(df['cs_call_center_sk'])
    df1_idx, df2_idx = join.join_on_table_float32(*join_meta, cc['cc_call_center_sk'])
    df = join.structured_array_merge(trans_s3.buf, df, cc, df1_idx, df2_idx,
            [n for n in df.dtype.names],
            [n for n in cc.dtype.names])
    # print(f'[tpcds] {tag_print} df: ', df.itemsize, df.shape, df.dtype)

    df = df[['cs_order_number', 'cs_ext_ship_cost', 'cs_net_profit']]

    # write back
    bp_s5 = buffer_pool_lib.buffer_pool({trans_s5_name:trans_s5})
    rdma_array = remote_array(bp_s5, input_ndarray=df, transport_name=trans_s5_name)

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = bp_s3.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

