import os
import datetime
import pandas as pd
import numpy as np
import base64
import urllib.request
import time

from numpy import genfromtxt
from numpy.lib import recfunctions as rfn
import numpy_groupies as npg
from npjoin import join

"""
checklist:
1. numpy join only works when keys are unique.
2. read from csv, how to deal with empty value? 
3. join type. inner? outer? leftouter?
4. parallel join and filter
"""

schemas = {"step1": {
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
}, "step2": {
    "sr_returned_date_sk": np.dtype(np.float32),
    "sr_return_time_sk": np.dtype(np.float32),
    "sr_item_sk": np.dtype(np.float32),
    "sr_customer_sk": np.dtype(np.float32),
    "sr_cdemo_sk": np.dtype(np.float32),
    "sr_hdemo_sk": np.dtype(np.float32),
    "sr_addr_sk": np.dtype(np.float32),
    "sr_store_sk": np.dtype(np.float32),
    "sr_reason_sk": np.dtype(np.float32),
    "sr_ticket_number": np.dtype(np.float32),
    "sr_return_quantity": np.dtype(np.float32),
    "sr_return_amt": np.dtype(np.float32),
    "sr_return_tax": np.dtype(np.float32),
    "sr_return_amt_inc_tax": np.dtype(np.float32),
    "sr_fee": np.dtype(np.float32),
    "sr_return_ship_cost": np.dtype(np.float32),
    "sr_refunded_cash": np.dtype(np.float32),
    "sr_reversed_charge": np.dtype(np.float32),
    "sr_store_credit": np.dtype(np.float32),
    "sr_net_loss": np.dtype(np.float32)
}, "step5": {
    "c_customer_sk": np.dtype(np.float32),
    "c_customer_id": np.dtype('S16'),
    "c_current_cdemo_sk": np.dtype(np.float32),
    "c_current_hdemo_sk": np.dtype(np.float32),
    "c_current_addr_sk": np.dtype(np.float32),
    "c_first_shipto_date_sk": np.dtype(np.float32),
    "c_first_sales_date_sk": np.dtype(np.float32),
    "c_salutation": np.dtype('S10'),
    "c_first_name": np.dtype('S20'),
    "c_last_name": np.dtype('S30'),
    "c_preferred_cust_flag": np.dtype('S1'),
    "c_birth_day": np.dtype(np.float32),
    "c_birth_month": np.dtype(np.float32),
    "c_birth_year": np.dtype(np.float32),
    "c_birth_country": np.dtype('S20'),
    "c_login": np.dtype('S13'),
    "c_email_address": np.dtype('S50'),
    "c_last_review_date": np.dtype('S10'),
}, "step7":
{
    "s_store_sk": np.dtype(np.float32),
    "s_store_id": np.dtype('S16'),
    "s_rec_start_date": np.dtype('S10'),
    "s_rec_end_date": np.dtype('S10'),
    "s_closed_date_sk": np.dtype(np.float32),
    "s_store_name": np.dtype('S50'),
    "s_number_employees": np.dtype(np.float32),
    "s_floor_space": np.dtype(np.float32),
    "s_hours": np.dtype('S20'),
    "s_manager": np.dtype('S40'),
    "s_market_id": np.dtype(np.float32),
    "s_geography_class": np.dtype('S100'),
    "s_market_desc": np.dtype('S100'),
    "s_market_manager": np.dtype('S40'),
    "s_division_id": np.dtype(np.float32),
    "s_division_name": np.dtype('S50'),
    "s_company_id": np.dtype(np.float32),
    "s_company_name": np.dtype('S50'),
    "s_street_number": np.dtype('S10'),
    "s_street_name": np.dtype('S60'),
    "s_street_type": np.dtype('S15'),
    "s_suite_number": np.dtype('S10'),
    "s_city": np.dtype('S60'),
    "s_county": np.dtype('S30'),
    "s_state": np.dtype('S2'),
    "s_zip": np.dtype('S10'),
    "s_country": np.dtype('S20'),
    "s_gmt_offset": np.dtype(np.float32),
    "s_tax_precentage": np.dtype(np.float32),
}
}

def build_dtype(schema):
    return np.dtype({
        'names'   : list(schema.keys()),
        'formats' : list(schema.values()),
        })

# def rename(sarray, find, rep):
    # sarray.dtype.names = tuple([rep if n == find else n for n in sarray.dtype.names])

def main():
    time_table = {}
    print("[tpcds] step1: begin")

    print("[tpcds] step1: start reading csv")
    tableurl = "http://localhost:8123/date_dim.csv"
    csv = urllib.request.urlopen(tableurl)

    scheme_in = schemas['step1']
    df = genfromtxt(csv, delimiter='|', dtype=build_dtype(schemas['step1']))

    print(df.dtype, df.itemsize, df.shape)

    t0 = time.time()
    df1 = df[df['d_year']==2000][['d_date_sk']]
    df1 = rfn.repack_fields(df1)

    print(df1.dtype, df1.itemsize, df1.shape)
    time_table['step1'] = time.time()-t0

    print("[tpcds] step 2: begin")

    print("[tpcds] step 2: start reading csv")
    tableurl = "http://localhost:8123/store_returns.csv"
    csv = urllib.request.urlopen(tableurl)

    df2 = genfromtxt(csv, delimiter='|', dtype=build_dtype(schemas['step2']))
    print(df2.dtype, df2.itemsize, df2.shape)
    print(f"[tpcds] step 2: finish reading csv")
    t0 = time.time()
    time_table['step2'] = time.time()-t0

    print(f"[tpcds] step 3: begin")

    t0 = time.time()
    # new join implementation
    # first, we need to select a table to load into memory (usually, the smaller one)
    join_meta = join.prepare_join_float32(df1['d_date_sk'])
    # then, we join the other part and get the indexers 
    # note here, we only select the required column out
    df1_idx, df2_idx = join.join_on_table_float32(*join_meta, df2['sr_returned_date_sk'])
    print('indexes', len(df1_idx), len(df2_idx), df1_idx, df2_idx)
    # finally, we collect results into some new memory.
    # TODO: use a function to calculate output type
    # df3dtype = join.merge_dtypes(df1, df2,
    #         [n for n in df1.dtype.names if n != 'd_date_sk'], 
    #         [n for n in df2.dtype.names if n != 'sr_returned_date_sk'])
    df3buf = np.empty(len(df1_idx) * (df1.itemsize + df2.itemsize), dtype=np.uint8)
    print('df3buf', df3buf.shape, 'itemsize', df1.itemsize + df2.itemsize)
    # send the indexers and required fields into the array
    df3 = join.structured_array_merge(df3buf, df1, df2, df1_idx, df2_idx,
            [n for n in df1.dtype.names if n != 'd_date_sk'], 
            [n for n in df2.dtype.names if n != 'sr_returned_date_sk'])
    print('df3 meta', df3.itemsize, df3.shape, df3.dtype)
    # print('df3', df3)
    time_table['step3'] = time.time()-t0

    print(f"[tpcds] step 4: begin")

    t0 = time.time()
    df3 = df3[~np.isnan(df3['sr_customer_sk'])&~np.isnan(df3['sr_store_sk'])]
    uniques, reverses = np.unique(df3[['sr_customer_sk', 'sr_store_sk']], return_inverse=True)
    print(uniques.shape, uniques, reverses)
    groups = npg.aggregate(reverses, df3['sr_return_amt'], func='sum')
    df4 = rfn.merge_arrays([uniques, groups], flatten = True, usemask = False)
    df4.dtype.names = ('ctr_customer_sk', 'ctr_store_sk', 'ctr_total_return')
    print('df4', df4.dtype, df4.itemsize, df4.shape)
    time_table['step4'] = time.time()-t0

    print(f"[tpcds] step 5: begin")

    print(f"[tpcds] step 5: start reading csv")
    tableurl = "http://localhost:8123/customer.csv"
    csv = urllib.request.urlopen(tableurl)

    df5 = genfromtxt(csv, delimiter='|', dtype=build_dtype(schemas['step5']))
    t0 = time.time()
    df5 = df5[['c_customer_sk', 'c_customer_id']]
    print('df5', df5.dtype, df5.itemsize, df5.shape)
    time_table['step5'] = time.time()-t0

    print(f"[tpcds] step 6: begin")
    t0 = time.time()
    join_meta = join.prepare_join_float32(df5['c_customer_sk'])
    df5_idx, df4_idx = join.join_on_table_float32(*join_meta, df4['ctr_customer_sk'])
    print('df5_idx', df5_idx, 'df4_idx', df4_idx)
    df6buf = np.empty(len(df4_idx) * (df4.itemsize + df5.itemsize), dtype=np.uint8)
    df6 = join.structured_array_merge(df6buf, df4, df5, df4_idx, df5_idx,
            ['ctr_store_sk', 'ctr_customer_sk', 'ctr_total_return'],
            ['c_customer_id'])

    # df6 = df6[['c_customer_id','ctr_store_sk','ctr_customer_sk','ctr_total_return']]
    print('df6', df6.dtype, df6.itemsize, df6.shape)
    time_table['step6'] = time.time()-t0

    print(f"[tpcds] step 7: begin")

    print(f"[tpcds] step 7: start reading csv")
    tableurl = "http://localhost:8123/store.csv"
    csv = urllib.request.urlopen(tableurl)

    df7 = genfromtxt(csv, delimiter='|', dtype=build_dtype(schemas['step7']))
    print('df7_raw', df7.dtype, df7.itemsize, df7.shape)
    t0 = time.time()
    df7 = df7[['s_state', 's_store_sk']][df7['s_state'] == 'TN'.encode()]
    print('df7_before_repack', df7.dtype, df7.itemsize, df7.shape)
    df7 = rfn.repack_fields(df7, align=True)
    print('df7', df7.dtype, df7.itemsize, df7.shape)
    print(f"[tpcds] step 7: finish reading csv")
    time_table['step7'] = time.time()-t0
    # TODO: type is strange!

    ######################
    # STEP 8
    ######################

    print(f"[tpcds] step 8: begin")
    t0 = time.time()

    # JOIN
    # rename(df7, 's_store_sk', 'ctr_store_sk')
    # df8 = rfn.join_by('ctr_store_sk', df6, df7)
    join_meta = join.prepare_join_float32(df6['ctr_store_sk'])
    df6_idx, df7_idx = join.join_on_table_float32(*join_meta, df7['s_store_sk'])
    df8buf = np.empty(len(df6_idx) * (df6.itemsize + df7.itemsize), dtype=np.uint8)
    df8 = join.structured_array_merge(df8buf, df6, df7, df6_idx, df7_idx,
            ['ctr_store_sk', 'c_customer_id', 'ctr_total_return', 'ctr_customer_sk'],
            [])

    # df8 = df8[['ctr_store_sk', 'c_customer_id', 'ctr_total_return', 'ctr_customer_sk']]
    print('df8', df8.dtype, df8.itemsize, df8.shape)

    # groupby
    uniques, reverses = np.unique(df8['ctr_store_sk'], axis=0, return_inverse=True)
    # print('uniques:', uniques.shape, uniques, reverses)
    groups = npg.aggregate(reverses, df8['ctr_total_return'], func='mean')
    avg = rfn.merge_arrays([uniques, groups], flatten = True, usemask = False)
    avg.dtype.names = ('ctr_store_sk', 'ctr_avg')
    print('avg', avg.dtype, avg.itemsize, avg.shape)

    # JOIN
    # u2 = rfn.join_by('ctr_store_sk', df8, avg)
    join_meta = join.prepare_join_float32(df8['ctr_store_sk'])
    df8_idx, avg_idx = join.join_on_table_float32(*join_meta, avg['ctr_store_sk'])
    u2buf = np.empty(len(df8_idx) * (df8.itemsize + avg.itemsize), dtype=np.uint8)
    u2 = join.structured_array_merge(u2buf, df8, avg, df8_idx, avg_idx,
            [n for n in df8.dtype.names],
            [n for n in avg.dtype.names if n != 'ctr_store_sk'])


    u2 = u2[u2['ctr_total_return'] > u2['ctr_avg']]
    print('u2', u2.dtype, u2.itemsize, u2.shape)

    # unique and sort
    final = np.unique(u2['c_customer_id'])
    final = np.sort(final)
    print('final', final.dtype, final.itemsize, final.shape)
    time_table['step8'] = time.time()-t0
    print('time: ')
    print(time_table)
    print('tpcds job finish')

if __name__ == "__main__":
  main()
