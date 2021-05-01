import os
import datetime
import pandas as pd
import numpy as np
import base64
import urllib.request
import time

schemas = {
    "step1":
    {
        "cs_sold_date_sk": np.dtype(np.float32),
        "cs_sold_time_sk": np.dtype(np.float32),
        "cs_ship_date_sk": np.dtype(np.float32),
        "cs_bill_customer_sk": np.dtype(np.float32),
        "cs_bill_cdemo_sk": np.dtype(np.float32),
        "cs_bill_hdemo_sk": np.dtype(np.float32),
        "cs_bill_addr_sk": np.dtype(np.float32),
        "cs_ship_customer_sk": np.dtype(np.float32),
        "cs_ship_cdemo_sk": np.dtype(np.float32),
        "cs_ship_hdemo_sk": np.dtype(np.float32),
        "cs_ship_addr_sk": np.dtype(np.float32),
        "cs_call_center_sk": np.dtype(np.float32),
        "cs_catalog_page_sk": np.dtype(np.float32),
        "cs_ship_mode_sk": np.dtype(np.float32),
        "cs_warehouse_sk": np.dtype(np.float32),
        "cs_item_sk": np.dtype(np.float32),
        "cs_promo_sk": np.dtype(np.float32),
        "cs_order_number": np.dtype(np.float32),
        "cs_quantity": np.dtype(np.float32),
        "cs_wholesale_cost": np.dtype(np.float32),
        "cs_list_price": np.dtype(np.float32),
        "cs_sales_price": np.dtype(np.float32),
        "cs_ext_discount_amt": np.dtype(np.float32),
        "cs_ext_sales_price": np.dtype(np.float32),
        "cs_ext_wholesale_cost": np.dtype(np.float32),
        "cs_ext_list_price": np.dtype(np.float32),
        "cs_ext_tax": np.dtype(np.float32),
        "cs_coupon_amt": np.dtype(np.float32),
        "cs_ext_ship_cost": np.dtype(np.float32),
        "cs_net_paid": np.dtype(np.float32),
        "cs_net_paid_inc_tax": np.dtype(np.float32),
        "cs_net_paid_inc_ship": np.dtype(np.float32),
        "cs_net_paid_inc_ship_tax": np.dtype(np.float32),
        "cs_net_profit": np.dtype(np.float32),
    }, "step2":
    {
        "cr_returned_date_sk": np.dtype(np.float32),
        "cr_returned_time_sk": np.dtype(np.float32),
        "cr_item_sk": np.dtype(np.float32),
        "cr_refunded_customer_sk": np.dtype(np.float32),
        "cr_refunded_cdemo_sk": np.dtype(np.float32),
        "cr_refunded_hdemo_sk": np.dtype(np.float32),
        "cr_refunded_addr_sk": np.dtype(np.float32),
        "cr_returning_customer_sk": np.dtype(np.float32),
        "cr_returning_cdemo_sk": np.dtype(np.float32),
        "cr_returning_hdemo_sk": np.dtype(np.float32),
        "cr_returning_addr_sk": np.dtype(np.float32),
        "cr_call_center_sk": np.dtype(np.float32),
        "cr_catalog_page_sk": np.dtype(np.float32),
        "cr_ship_mode_sk": np.dtype(np.float32),
        "cr_warehouse_sk": np.dtype(np.float32),
        "cr_reason_sk": np.dtype(np.float32),
        "cr_order_number": np.dtype(np.float32),
        "cr_return_quantity": np.dtype(np.float32),
        "cr_return_amount": np.dtype(np.float32),
        "cr_return_tax": np.dtype(np.float32),
        "cr_return_amt_inc_tax": np.dtype(np.float32),
        "cr_fee": np.dtype(np.float32),
        "cr_return_ship_cost": np.dtype(np.float32),
        "cr_refunded_cash": np.dtype(np.float32),
        "cr_reversed_charge": np.dtype(np.float32),
        "cr_store_credit": np.dtype(np.float32),
        "cr_net_loss": np.dtype(np.float32),
    }, "step3":
    {
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
    }, "step4":
    {
        "ca_address_sk": np.dtype(np.float32),
        "ca_address_id": np.dtype('S16'),
        "ca_street_number": np.dtype('S10'),
        "ca_street_name": np.dtype('S60'),
        "ca_street_type": np.dtype('S15'),
        "ca_suite_number": np.dtype('S10'),
        "ca_city": np.dtype('S60'),
        "ca_county": np.dtype('S30'),
        "ca_state": np.dtype('S2'),
        "ca_zip": np.dtype('S10'),
        "ca_country": np.dtype('S20'),
        "ca_gmt_offset": np.dtype(np.float32),
        "ca_location_type": np.dtype('S20'),
    }, "step5":
    {
        "cc_call_center_sk": np.dtype(np.float32),
        "cc_call_center_id": np.dtype('S16'),
        "cc_rec_start_date": np.dtype('S10'),
        "cc_rec_end_date": np.dtype('S10'),
        "cc_closed_date_sk": np.dtype(np.float32),
        "cc_open_date_sk": np.dtype(np.float32),
        "cc_name": np.dtype('S50'),
        "cc_class": np.dtype('S50'),
        "cc_employees": np.dtype(np.float32),
        "cc_sq_ft": np.dtype(np.float32),
        "cc_hours": np.dtype('S20'),
        "cc_manager": np.dtype('S40'),
        "cc_mkt_id": np.dtype(np.float32),
        "cc_mkt_class": np.dtype('S50'),
        "cc_mkt_desc": np.dtype('S100'),
        "cc_market_manager": np.dtype('S40'),
        "cc_division": np.dtype(np.float32),
        "cc_division_name": np.dtype('S50'),
        "cc_company": np.dtype(np.float32),
        "cc_company_name": np.dtype('S50'),
        "cc_street_number": np.dtype('S10'),
        "cc_street_name": np.dtype('S60'),
        "cc_street_type": np.dtype('S15'),
        "cc_suite_number": np.dtype('S10'),
        "cc_city": np.dtype('S60'),
        "cc_county": np.dtype('S30'),
        "cc_state": np.dtype('S2'),
        "cc_zip": np.dtype('S10'),
        "cc_country": np.dtype('S20'),
        "cc_gmt_offset": np.dtype(np.float32),
        "cc_tax_percentage": np.dtype(np.float32),
    }
}

def main():
    time_table = {}
    tag_print = "step1"
    print(f"[tpcds] {tag_print}: begin")

    tableurl = "http://localhost:8123/catalog_sales.csv"
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

    wanted_columns = ['cs_order_number',
                      'cs_ext_ship_cost',
                      'cs_net_profit',
                      'cs_ship_date_sk',
                      'cs_ship_addr_sk',
                      'cs_call_center_sk',
                      'cs_warehouse_sk']
    df_step1 = df[wanted_columns]
    time_table[tag_print] = time.time()-t0

    tag_print = "step2"
    print(f"[tpcds] {tag_print}: begin")

    tableurl = "http://localhost:8123/catalog_returns.csv"
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

    tableurl = "http://localhost:8123/date_dim.csv"
    csv = urllib.request.urlopen(tableurl)

    scheme_in = schemas['step3']
    names = list(scheme_in.keys()) + ['']
    df = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")

    t0 = time.time()

    cs_succient = df_step1[['cs_order_number', 'cs_warehouse_sk']]
    print('cs_succient: ', cs_succient.dtypes, cs_succient.shape)
    wh_uc = cs_succient.groupby(['cs_order_number']).agg({'cs_warehouse_sk':'nunique'})
    print('wh_uc: ', wh_uc.dtypes,wh_uc.shape)
    target_order_numbers = wh_uc.loc[wh_uc['cs_warehouse_sk'] > 1].index.values

    # with open('/home/jil/serverless/Disagg-Serverless/runtime/test/src/tpcds_16/ref_target_order_numbers.npy', 'wb') as f:
    #     np.save(f, target_order_numbers)

    # print('target_order_numbers: ', target_order_numbers)
    print('target_order_numbers: ', target_order_numbers.size)
    print('df_step1: ', df_step1.dtypes, df_step1.shape)
    cs_sj_f1 = df_step1.loc[df_step1['cs_order_number'].isin(target_order_numbers)]
    print('cs_sj_f1: ', cs_sj_f1.dtypes, cs_sj_f1.shape)

    cs_sj_f2 = cs_sj_f1.loc[cs_sj_f1['cs_order_number'].isin(df_step2.cr_order_number)]
    del cs_sj_f1
    # print('cs_sj_f2: ', cs_sj_f2.dtypes, cs_sj_f2.shape, cs_sj_f2)

    dd = df[['d_date', 'd_date_sk']]
    dd_select = dd[(pd.to_datetime(dd['d_date'].apply(bytes.decode)) > pd.to_datetime('2002-02-01')) & (pd.to_datetime(dd['d_date'].apply(bytes.decode)) < pd.to_datetime('2002-04-01'))]
    dd_filtered = dd_select[['d_date_sk']]
    # print('dd_filtered: ', dd_filtered.dtypes, dd_filtered.shape, dd_filtered)
    
    print('df1: ', cs_sj_f2.dtypes, cs_sj_f2.shape)
    print('df2: ', dd_filtered.dtypes, dd_filtered.shape)
    df_step3 = cs_sj_f2.merge(dd_filtered, left_on='cs_ship_date_sk', right_on='d_date_sk')
    del dd
    del cs_sj_f2
    del dd_select
    del dd_filtered
    df_step3.drop('d_date_sk', axis=1, inplace=True)
    print('df_step3: ', df_step3.dtypes, df_step3.shape)

    time_table[tag_print] = time.time()-t0

    tag_print = "step4"
    print(f"[tpcds] {tag_print}: begin")

    tableurl = "http://localhost:8123/customer_address.csv"
    csv = urllib.request.urlopen(tableurl)

    scheme_in = schemas['step4']
    names = list(scheme_in.keys()) + ['']
    df = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")

    t0 = time.time()

    df_step4 = df[df.ca_state == 'GA'.encode()][['ca_address_sk']]
    print('df_step4: ', df_step4.dtypes, df_step4.shape)

    time_table[tag_print] = time.time()-t0

    tag_print = "step5"
    print(f"[tpcds] {tag_print}: begin")

    tableurl = "http://localhost:8123/call_center.csv"
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

    cs = df_step3
    ca = df_step4
    cc = df

    merged = cs.merge(ca, left_on='cs_ship_addr_sk', right_on='ca_address_sk')
    merged.drop('cs_ship_addr_sk', axis=1, inplace=True)
    
    list_addr = ['Williamson County'.encode(), 'Williamson County'.encode(), 'Williamson County'.encode(), 'Williamson County'.encode(), 'Williamson County'.encode()]
    cc_p = cc[cc.cc_county.isin(list_addr)][['cc_call_center_sk']]
    
    #print(cc['cc_country'])
    merged2 = merged.merge(cc_p, left_on='cs_call_center_sk', right_on='cc_call_center_sk')
    
    df_step5 = merged2[['cs_order_number', 'cs_ext_ship_cost', 'cs_net_profit']]
    print('df_step5: ', df_step5.dtypes, df_step5.shape)

    time_table[tag_print] = time.time()-t0

    tag_print = "step6"
    print(f"[tpcds] {tag_print}: begin")

    t0 = time.time()

    cs = df_step5

    a1 = pd.unique(cs['cs_order_number']).size
    a2 = cs['cs_ext_ship_cost'].sum()
    a3 = cs['cs_net_profit'].sum()

    time_table[tag_print] = time.time()-t0

    print("[tpcds] final query result: ", a1, a2, a3)

    ##############################################

    print('time: ')
    print(time_table)

    # dfs = [df_step1, df_step2, df_step3, df_step4, df_step5]

    # i = 1
    # for d in dfs:
    #   d.reset_index(drop=True).to_csv('./steps_ref/ref_output_' + str(i) + '.csv')
    #   i += 1

if __name__ == "__main__":
  main()
