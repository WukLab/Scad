import os
import datetime
import pandas as pd
import numpy as np
import base64
import urllib.request
import time

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

def main():
    time_table = {}
    tag_print = "step1"
    print(f"[tpcds] {tag_print}: begin")

    tableurl = "http://localhost:8123/date_dim.csv"
    csv = urllib.request.urlopen(tableurl)

    scheme_in = schemas['step1']
    names = list(scheme_in.keys()) + ['']
    df = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")

    t0 = time.time()
    df_step1 = df[df['d_year']==2000][['d_date_sk']]
    time_table[tag_print] = time.time()-t0

    tag_print = "step2"
    print(f"[tpcds] {tag_print}: begin")

    tableurl = "http://localhost:8123/store_returns.csv"
    csv = urllib.request.urlopen(tableurl)

    scheme_in = schemas['step2']
    names = list(scheme_in.keys()) + ['']
    df_step2 = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")
    t0 = time.time()
    time_table[tag_print] = time.time()-t0

    tag_print = "step3"
    print(f"[tpcds] {tag_print}: begin")

    t0 = time.time()
    df_step3 = df_step1.merge(df_step2, left_on='d_date_sk', right_on='sr_returned_date_sk')
    df_step3.drop('d_date_sk', axis=1, inplace=True)
    time_table[tag_print] = time.time()-t0

    tag_print = "step4"

    print(f"[tpcds] {tag_print}: start reading remote array")

    t0 = time.time()
    df_step4 = df_step3.groupby(['sr_customer_sk', 'sr_store_sk']).agg(
        {'sr_return_amt': 'sum'}).reset_index()
    df_step4.rename(columns={'sr_store_sk': 'ctr_store_sk',
                       'sr_customer_sk': 'ctr_customer_sk',
                       'sr_return_amt': 'ctr_total_return'
                       }, inplace=True)
    time_table[tag_print] = time.time()-t0

    tag_print = "step5"
    print(f"[tpcds] {tag_print}: begin")

    tableurl = "http://localhost:8123/customer.csv"
    csv = urllib.request.urlopen(tableurl)

    scheme_in = schemas['step5']
    names = list(scheme_in.keys()) + ['']
    df = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")

    t0 = time.time()
    df_step5 = df[['c_customer_sk', 'c_customer_id']]
    time_table[tag_print] = time.time()-t0

    tag_print = "step6"
    print(f"[tpcds] {tag_print}: begin")

    t0 = time.time()
    df = df_step4.merge(df_step5, left_on='ctr_customer_sk', right_on='c_customer_sk')
    df_step6 = df[['c_customer_id','ctr_store_sk','ctr_customer_sk','ctr_total_return']]
    time_table[tag_print] = time.time()-t0

    tag_print = "step7"
    print(f"[tpcds] {tag_print}: begin")

    tableurl = "http://localhost:8123/store.csv"
    csv = urllib.request.urlopen(tableurl)

    scheme_in = schemas['step7']
    names = list(scheme_in.keys()) + ['']
    df = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")

    t0 = time.time()
    df = df[['s_state', 's_store_sk']]
    df_step7 = df[df['s_state'] == 'TN'.encode()]
    time_table[tag_print] = time.time()-t0

    tag_print = "step8"
    print(f"[tpcds] {tag_print}: begin")

    d = df_step6
    sr = df_step7

    t0 = time.time()
    merged = d.merge(sr, left_on='ctr_store_sk', right_on='s_store_sk')
    merged_u = merged[['ctr_store_sk', 'c_customer_id',
                       'ctr_total_return', 'ctr_customer_sk']]
  
    r_avg = merged_u.groupby(['ctr_store_sk']).agg(
        {'ctr_total_return': 'mean'}).reset_index()
    r_avg.rename(columns={'ctr_total_return': 'ctr_avg'}, inplace=True)
  
    #print(r_avg)
    #print("aaa")
    #aaa = [d.empty, sr.empty, merged_u.empty, r_avg.empty]
    #print(",".join([str(a) for a in aaa]))
    merged_u2 = merged_u.merge(
        r_avg, left_on='ctr_store_sk', right_on='ctr_store_sk')
    final_merge = merged_u2[merged_u2['ctr_total_return']
                            > merged_u2['ctr_avg']]
    final = final_merge[['c_customer_id']].drop_duplicates().sort_values(by=[
        'c_customer_id'])
    #print(final)
    #print(cc['cc_country'])
    time_table[tag_print] = time.time()-t0
    print('time: ')
    print(time_table)

    merged_u.to_csv('./ref_output_merged_u.csv')
    final.to_csv('./ref_output_final.csv')

    dfs = [df_step1, df_step2, df_step3, df_step4, df_step5, df_step6, df_step7]

    i = 1
    for d in dfs:
      d.to_csv('./steps_reference/ref_output_' + str(i) + '.csv')
      i += 1

if __name__ == "__main__":
  main()