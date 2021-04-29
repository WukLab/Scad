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
from numpy_groupies.aggregate_numpy import aggregate

# import pandas as pd

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
    df = df[['d_date', 'd_date_sk']]

    # build transport
    trans_s3_name = '3_out_mem'
    trans_s3 = action.get_transport(trans_s3_name, 'rdma')
    trans_s3.reg(buffer_pool_lib.buffer_size)

    # data operation
    # print(f'[tpcds] {tag_print} cs: ', cs.itemsize, cs.shape, cs.dtype)
    cs_tmp = cs[['cs_order_number', 'cs_warehouse_sk']]
    cs_tmp = cs_tmp[~np.isnan(cs_tmp['cs_order_number']) & ~np.isnan(cs_tmp['cs_warehouse_sk'])]
    # print(f'[tpcds] {tag_print} cs_tmp: ', cs_tmp.itemsize, cs_tmp.shape, cs_tmp.dtype)
    uniques, reverses = np.unique(cs_tmp[['cs_order_number']], return_inverse=True)
    groups = aggregate(reverses, cs_tmp['cs_warehouse_sk'], lambda a: len(set(a)))
    wh_uc = rfn.merge_arrays([uniques, groups], flatten = True, usemask = False)
    wh_uc.dtype.names = ('cs_order_number', 'cs_warehouse_sk')
    # print(f'[tpcds] {tag_print} wh_uc: ', wh_uc.itemsize, wh_uc.shape, wh_uc.dtype, wh_uc)
    # wh_uc = wh_uc[~np.isnan(wh_uc['cs_order_number']) & ~np.isnan(wh_uc['cs_warehouse_sk'])]
    # print(f'[tpcds] {tag_print} wh_uc(after nan): ', wh_uc.itemsize, wh_uc.shape, wh_uc.dtype, wh_uc)

    target_order_numbers = wh_uc[wh_uc['cs_warehouse_sk'] > 1]['cs_order_number']
    # print(f'[tpcds] {tag_print} target_order_num: ', target_order_numbers.itemsize, target_order_numbers.shape, target_order_numbers.dtype)

    # with open('/home/jil/serverless/Disagg-Serverless/runtime/test/src/tpcds_16/saved_target_order_numbers.npy', 'wb') as f:
        # np.save(f, target_order_numbers)

    # print(f'[tpcds] {tag_print} cs: ', cs.itemsize, cs.shape, cs.dtype)
    cs_sj_f1 = cs[np.isin(cs['cs_order_number'], target_order_numbers)]
    # print(f'[tpcds] {tag_print} cs_sj_f1: ', cs_sj_f1.itemsize, cs_sj_f1.shape, cs_sj_f1.dtype)
    df1 = cs_sj_f1[np.isin(cs_sj_f1['cs_order_number'], cr['cr_order_number'])]
    del cs
    del cs_sj_f1

    # df = df[np.datetime64(df['d_date'].encode()) > np.datetime64('2002-02-01') & np.datetime64(df['d_date'].encode()) < np.datetime64('2002-04-01')]
    df = df[np.vectorize(lambda x: x[0:4] == '2002'.encode() and ((x[5:7] == '02'.encode() and x[8:] != '01'.encode()) or x[5:7] == '03'.encode()), otypes=[bool])(df['d_date'])]
    df2 = df[['d_date_sk']]

    print(f'[tpcds] {tag_print} df1: ', df1.itemsize, df1.shape, df1.dtype, df1)
    print(f'[tpcds] {tag_print} df2: ', df2.itemsize, df2.shape, df2.dtype, df2)
    join_meta = join.prepare_join_float32(df1['cs_ship_date_sk'])
    df1_idx, df2_idx = join.join_on_table_float32(*join_meta, df2['d_date_sk'])
    print("df1_idx: ", df1_idx)
    print("df2_idx: ", df1_idx)
    df = join.structured_array_merge(trans_s3.buf, df1, df2, df1_idx, df2_idx,
            [n for n in df1.dtype.names],
            [n for n in df2.dtype.names if n != 'd_date_sk'])
    print(f'[tpcds] {tag_print} df: ', df.itemsize, df.shape, df.dtype)

    # write back
    bp_s3 = buffer_pool_lib.buffer_pool({trans_s3_name:trans_s3})
    rdma_array = remote_array(bp_s3, input_ndarray=df, transport_name=trans_s3_name)

    # debug
    # df = pd.DataFrame(data=df, columns=['cs_order_number', 'cs_ext_ship_cost', 'cs_net_profit', 'cs_ship_date_sk', 'cs_ship_addr_sk', 'cs_call_center_sk', 'cs_warehouse_sk'])
    # df.to_csv('/home/jil/serverless/Disagg-Serverless/runtime/test/src/tpcds_16/step3.csv')

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = bp_s3.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }

