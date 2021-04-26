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
from disaggrt.rdma_array import remote_array

from numpy import genfromtxt
from numpy.lib import recfunctions as rfn
import numpy_groupies as npg
from npjoin import join

scheme_in = {
    "d_date_sk":            np.dtype(np.float32),
    "d_date_id":            np.dtype('S16'),
    "d_date":               np.dtype('S10'),
    "d_month_seq":          np.dtype(np.float32),
    "d_week_seq":           np.dtype(np.float32),
    "d_quarter_seq":        np.dtype(np.float32),
    "d_year":               np.dtype(np.float32),
    "d_dow":                np.dtype(np.float32),
    "d_moy":                np.dtype(np.float32),
    "d_dom":                np.dtype(np.float32),
    "d_qoy":                np.dtype(np.float32),
    "d_fy_year":            np.dtype(np.float32),
    "d_fy_quarter_seq":     np.dtype(np.float32),
    "d_fy_week_seq":        np.dtype(np.float32),
    "d_day_name":           np.dtype('S16'),
    "d_quarter_name":       np.dtype('S8'),
    "d_holiday":            np.dtype('S1'),
    "d_weekend":            np.dtype('S1'),
    "d_following_holiday":  np.dtype('S1'),
    "d_first_dom":          np.dtype(np.float32),
    "d_last_dom":           np.dtype(np.float32),
    "d_same_day_ly":        np.dtype(np.float32),
    "d_same_day_lq":        np.dtype(np.float32),
    "d_current_day":        np.dtype('S1'),
    "d_current_week":       np.dtype('S1'),
    "d_current_month":      np.dtype('S1'),
    "d_current_quarter":    np.dtype('S1'),
    "d_current_year":       np.dtype('S1'),
}

def build_dtype(schema):
    return np.dtype({
        'names': list(schema.keys()),
        'formats': list(schema.values()),
    })

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
    cs = df_s1_arr.materialize()

    # load fron step2
    trans_s2_name = '2_out_mem'
    trans_s2 = action.get_transport(trans_s2_name, 'rdma')
    trans_s2.reg(buffer_pool_lib.buffer_size)

    context_dict = pickle.loads(base64.b64decode(params['step2']['meta']))
    # print(f"[tpcds] {tag_print}: loading params from step2", context_dict)

    bp_s2 = buffer_pool_lib.buffer_pool({trans_s2_name: trans_s2}, context_dict["bp"])
    df_s2_arr = remote_array(bp_s2, metadata=context_dict["df"])
    cr = df_s2_arr.materialize()
    # print(f"[tpcds] {tag_print}: finish reading rdma")

    # read csv
    # print(f"[tpcds] {tag_print}: start reading csv")
    tableurl = "http://localhost:8123/date_dim.csv"
    csv = urllib.request.urlopen(tableurl)

    df = genfromtxt(csv, delimiter='|', dtype=build_dtype(scheme_in))

    # build transport
    trans_s3_name = '3_out_mem'
    trans_s3 = action.get_transport(trans_s3_name, 'rdma')
    trans_s3.reg(buffer_pool_lib.buffer_size)

    # data operation
    cs = cs[['cs_order_number', 'cs_warehouse_sk']]
    # cs = cs[~np.isnan(cs['cs_order_number'])]
    uniques, reverses = np.unique(cs[['cs_order_number']], return_inverse=True)
    groups = npg.aggregate(reverses, cs['cs_warehouse_sk'], func='nunique')
    wh_uc = rfn.merge_arrays([uniques, groups], flatten = True, usemask = False)
    print(f'[tpcds] {tag_print} wh_uc: ', wh_uc.itemsize, wh_uc.shape, wh_uc.dtype)
    wh_uc.dtype.names = ('cs_order_number', 'cs_warehouse_sk')
    print(f'[tpcds] {tag_print} wh_uc: ', wh_uc.itemsize, wh_uc.shape, wh_uc.dtype)
    target_order_numbers = wh_uc[wh_uc['cs_warehouse_sk'] > 1]['cs_order_number']
    cs_sj_f1 = cs[np.isin(cs['cs_order_number'], target_order_numbers)]

    df1 = cs_sj_f1[np.isin(cs_sj_f1['cs_order_number'], cr['cr_order_number'])]
    del cs_sj_f1

    df = df[['d_date', 'd_date_sk']]
    df = df[np.datetime64(df['d_date']) > np.datetime64('2002-02-01') & np.datetime64(df['d_date']) < np.datetime64('2002-04-01')]
    df2 = df[['d_date_sk']]

    join_meta = join.prepare_join_float32(df1['cs_ship_date_sk'])
    df1_idx, df2_idx = join.join_on_table_float32(*join_meta, df2['d_date_sk'])
    df = join.structured_array_merge(trans_s3.buf, df1, df2, df1_idx, df2_idx,
            [n for n in df1.dtype.names],
            [n for n in df2.dtype.names if n != 'd_date_sk'])
    # print(f'[tpcds] {tag_print} df: ', df.itemsize, df.shape, df.dtype)

    # write back
    bp_s3 = buffer_pool_lib.buffer_pool({trans_s3_name:trans_s3})
    rdma_array = remote_array(bp_s3, input_ndarray=df, transport_name=trans_s3_name)

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = bp_s3.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

