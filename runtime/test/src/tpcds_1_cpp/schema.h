#ifndef _SCHEMA_H_
#define _SCHEMA_H_

#include "string.h"

static char fmt[4096];
static char * SCHEMA_FMT(int pairs) {
    int idx = 0;
    fmt[idx++] = '[';
    for (int i = 0; i < pairs; i++) {
        strcpy(fmt+idx, "(s,s)"); idx += 5;
        if (i+1 != pairs) fmt[idx++] = ',';
    }
    fmt[idx++] = ']';
    fmt[idx++] = '\0';
    return fmt;
}

#define STEP1_SCHEMA_SIZE 28
#define STEP1_SCHEMA \
    "d_date_sk",            "f4", \
    "d_date_id",            "S16", \
    "d_date",               "S10", \
    "d_month_seq",          "f4", \
    "d_week_seq",           "f4", \
    "d_quarter_seq",        "f4", \
    "d_year",               "f4", \
    "d_dow",                "f4", \
    "d_moy",                "f4", \
    "d_dom",                "f4", \
    "d_qoy",                "f4", \
    "d_fy_year",            "f4", \
    "d_fy_quarter_seq",     "f4", \
    "d_fy_week_seq",        "f4", \
    "d_day_name",           "S16", \
    "d_quarter_name",       "S8", \
    "d_holiday",            "S1", \
    "d_weekend",            "S1", \
    "d_following_holiday",  "S1", \
    "d_first_dom",          "f4", \
    "d_last_dom",           "f4", \
    "d_same_day_ly",        "f4", \
    "d_same_day_lq",        "f4", \
    "d_current_day",        "S1", \
    "d_current_week",       "S1", \
    "d_current_month",      "S1", \
    "d_current_quarter",    "S1", \
    "d_current_year",       "S1"

#define STEP2_SCHEMA_SIZE 20
#define STEP2_SCHEMA \
    "sr_returned_date_sk",      "f4", \
    "sr_return_time_sk",        "f4", \
    "sr_item_sk",               "f4", \
    "sr_customer_sk",           "f4", \
    "sr_cdemo_sk",              "f4", \
    "sr_hdemo_sk",              "f4", \
    "sr_addr_sk",               "f4", \
    "sr_store_sk",              "f4", \
    "sr_reason_sk",             "f4", \
    "sr_ticket_number",         "f4", \
    "sr_return_quantity",       "f4", \
    "sr_return_amt",            "f4", \
    "sr_return_tax",            "f4", \
    "sr_return_amt_inc_tax",    "f4", \
    "sr_fee",                   "f4", \
    "sr_return_ship_cost",      "f4", \
    "sr_refunded_cash",         "f4", \
    "sr_reversed_charge",       "f4", \
    "sr_store_credit",          "f4", \
    "sr_net_loss",              "f4"

#define DF3_SCHEMA_SIZE 3
#define DF3_SCHEMA \
    "sr_customer_sk", "f4", \
    "sr_store_sk"   , "f4", \
    "sr_return_amt" , "f4"

#endif // _SCHEMA_H_
