import os
import datetime
import pandas as pd
import numpy as np
import base64
import urllib.request
import time

# 120,147s/ varchar(\([0-9]*\))/: np.dtype('S\1')/g
schemas = {
    "step1":
    {
        "ws_sold_date_sk": np.dtype(np.float32),
        "ws_sold_time_sk": np.dtype(np.float32),
        "ws_ship_date_sk": np.dtype(np.float32),
        "ws_item_sk": np.dtype(np.float32),
        "ws_bill_customer_sk": np.dtype(np.float32),
        "ws_bill_cdemo_sk": np.dtype(np.float32),
        "ws_bill_hdemo_sk": np.dtype(np.float32),
        "ws_bill_addr_sk": np.dtype(np.float32),
        "ws_ship_customer_sk": np.dtype(np.float32),
        "ws_ship_cdemo_sk": np.dtype(np.float32),
        "ws_ship_hdemo_sk": np.dtype(np.float32),
        "ws_ship_addr_sk": np.dtype(np.float32),
        "ws_web_page_sk": np.dtype(np.float32),
        "ws_web_site_sk": np.dtype(np.float32),
        "ws_ship_mode_sk": np.dtype(np.float32),
        "ws_warehouse_sk": np.dtype(np.float32),
        "ws_promo_sk": np.dtype(np.float32),
        "ws_order_number": np.dtype(np.float32),
        "ws_quantity": np.dtype(np.float32),
        "ws_wholesale_cost": np.dtype(np.float32),
        "ws_list_price": np.dtype(np.float32),
        "ws_sales_price": np.dtype(np.float32),
        "ws_ext_discount_amt": np.dtype(np.float32),
        "ws_ext_sales_price": np.dtype(np.float32),
        "ws_ext_wholesale_cost": np.dtype(np.float32),
        "ws_ext_list_price": np.dtype(np.float32),
        "ws_ext_tax": np.dtype(np.float32),
        "ws_coupon_amt": np.dtype(np.float32),
        "ws_ext_ship_cost": np.dtype(np.float32),
        "ws_net_paid": np.dtype(np.float32),
        "ws_net_paid_inc_tax": np.dtype(np.float32),
        "ws_net_paid_inc_ship": np.dtype(np.float32),
        "ws_net_paid_inc_ship_tax": np.dtype(np.float32),
        "ws_net_profit": np.dtype(np.float32),
    }, "step4":
    {
        "wr_returned_date_sk": np.dtype(np.float32),
        "wr_returned_time_sk": np.dtype(np.float32),
        "wr_item_sk": np.dtype(np.float32),
        "wr_refunded_customer_sk": np.dtype(np.float32),
        "wr_refunded_cdemo_sk": np.dtype(np.float32),
        "wr_refunded_hdemo_sk": np.dtype(np.float32),
        "wr_refunded_addr_sk": np.dtype(np.float32),
        "wr_returning_customer_sk": np.dtype(np.float32),
        "wr_returning_cdemo_sk": np.dtype(np.float32),
        "wr_returning_hdemo_sk": np.dtype(np.float32),
        "wr_returning_addr_sk": np.dtype(np.float32),
        "wr_web_page_sk": np.dtype(np.float32),
        "wr_reason_sk": np.dtype(np.float32),
        "wr_order_number": np.dtype(np.float32),
        "wr_return_quantity": np.dtype(np.float32),
        "wr_return_amt": np.dtype(np.float32),
        "wr_return_tax": np.dtype(np.float32),
        "wr_return_amt_inc_tax": np.dtype(np.float32),
        "wr_fee": np.dtype(np.float32),
        "wr_return_ship_cost": np.dtype(np.float32),
        "wr_refunded_cash": np.dtype(np.float32),
        "wr_reversed_charge": np.dtype(np.float32),
        "wr_account_credit": np.dtype(np.float32),
        "wr_net_loss": np.dtype(np.float32),
    }, "step5": {
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
    }, "step6":
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
    }, "step7": {
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
}

def main():
    time_table = {}
    tag_print = "step1"
    print(f"[tpcds] {tag_print}: begin")

    tableurl = "http://localhost:8123/web_sales.csv"
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

    wanted_columns = ['ws_order_number',
                      'ws_warehouse_sk']

    df_step1 = df[wanted_columns]
    time_table[tag_print] = time.time()-t0
    print('df_step1: ', df_step1.dtypes, df_step1.shape)

    tag_print = "step2"
    print(f"[tpcds] {tag_print}: begin")

    cs = df_step1

    t0 = time.time()

    wh_uc = cs.groupby(['ws_order_number']).agg({'ws_warehouse_sk':'nunique'})
    target_order_numbers = wh_uc.loc[wh_uc['ws_warehouse_sk'] > 1].index.values

    cs_sj_f1 = cs[['ws_order_number']].drop_duplicates()

    df_step2 = cs_sj_f1.loc[cs_sj_f1['ws_order_number'].isin(target_order_numbers)]

    time_table[tag_print] = time.time()-t0
    print('df_step2: ', df_step2.dtypes, df_step2.shape)

    tag_print = "step3"
    print(f"[tpcds] {tag_print}: begin")

    tableurl = "http://localhost:8123/web_sales.csv"
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

    wanted_columns = ['ws_order_number',
                      'ws_ext_ship_cost',
                      'ws_net_profit',
                      'ws_ship_date_sk',
                      'ws_ship_addr_sk',
                      'ws_web_site_sk',
                      'ws_warehouse_sk']

    df_step3 = df[wanted_columns]
    time_table[tag_print] = time.time()-t0
    print('df_step3: ', df_step3.dtypes, df_step3.shape)

    tag_print = "step4"
    print(f"[tpcds] {tag_print}: begin")

    tableurl = "http://localhost:8123/web_returns.csv"
    csv = urllib.request.urlopen(tableurl)

    scheme_in = schemas['step4']
    names = list(scheme_in.keys()) + ['']
    df_step4 = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")
    t0 = time.time()
    time_table[tag_print] = time.time()-t0

    tag_print = "step5"
    print(f"[tpcds] {tag_print}: begin")

    cs = df_step3
    cr = df_step4
    ws_wh = df_step2

    tableurl = "http://localhost:8123/date_dim.csv"
    csv = urllib.request.urlopen(tableurl)

    scheme_in = schemas['step5']
    names = list(scheme_in.keys()) + ['']
    d = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")

    t0 = time.time()

    cs_sj_f1 = cs.loc[cs['ws_order_number'].isin(ws_wh['ws_order_number'])]

    cs_sj_f2 = cs_sj_f1.loc[cs_sj_f1['ws_order_number'].isin(cr.wr_order_number)]    
    
    # join date_dim
    dd = d[['d_date', 'd_date_sk']]
    dd_select = dd[(pd.to_datetime(dd['d_date'].apply(bytes.decode)) > pd.to_datetime('1999-02-01')) & (pd.to_datetime(dd['d_date'].apply(bytes.decode)) < pd.to_datetime('1999-04-01'))]
    dd_filtered = dd_select[['d_date_sk']]
    
    merged = cs_sj_f2.merge(dd_filtered, left_on='ws_ship_date_sk', right_on='d_date_sk')
    del dd
    del cs_sj_f2
    del dd_select
    del dd_filtered
    merged.drop('d_date_sk', axis=1, inplace=True)

    df_step5 = merged

    time_table[tag_print] = time.time()-t0

    tag_print = "step6"
    print(f"[tpcds] {tag_print}: begin")

    tableurl = "http://localhost:8123/customer_address.csv"
    csv = urllib.request.urlopen(tableurl)

    scheme_in = schemas['step6']
    names = list(scheme_in.keys()) + ['']
    df = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")

    t0 = time.time()

    df_step6 = df[df.ca_state == 'IL'.encode()][['ca_address_sk']]

    print('df_step6: ', df_step6.dtypes, df_step6.shape)

    time_table[tag_print] = time.time()-t0

    tag_print = "step7"
    print(f"[tpcds] {tag_print}: begin")

    cs = df_step5
    ca = df_step6

    tableurl = "http://localhost:8123/web_site.csv"
    csv = urllib.request.urlopen(tableurl)

    scheme_in = schemas['step7']
    names = list(scheme_in.keys()) + ['']
    cc = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")

    t0 = time.time()

    merged = cs.merge(ca, left_on='ws_ship_addr_sk', right_on='ca_address_sk')
    merged.drop('ws_ship_addr_sk', axis=1, inplace=True)
    
    cc_p = cc[cc['web_company_name'] == 'pri'.encode()][['web_site_sk']]
    
    #print(cc['cc_country'])
    merged2 = merged.merge(cc_p, left_on='ws_web_site_sk', right_on='web_site_sk')
    
    df_step7 = merged2[['ws_order_number', 'ws_ext_ship_cost', 'ws_net_profit']]

    time_table[tag_print] = time.time()-t0

    print('df_step7: ', df_step7.dtypes, df_step7.shape)

    tag_print = "step8"
    print(f"[tpcds] {tag_print}: begin")

    cs = df_step7

    t0 = time.time()

    a1 = pd.unique(cs['ws_order_number']).size
    a2 = cs['ws_ext_ship_cost'].sum()
    a3 = cs['ws_net_profit'].sum()

    time_table[tag_print] = time.time()-t0

    print("[tpcds] final query result: ", a1, a2, a3)

    ##############################################

    print('time: ')
    print(time_table)

#     dfs = [df_step1, df_step2, df_step3, df_step4, df_step5, df_step6, df_step7]

#     i = 1
#     for d in dfs:
#       d.reset_index(drop=True).to_csv('./steps_ref/ref_output_' + str(i) + '.csv')
#       i += 1

if __name__ == "__main__":
  main()
