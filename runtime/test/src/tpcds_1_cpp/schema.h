#ifndef _SCHEMA_H_
#define _SCHEMA_H_

#include "string.h"

static char fmt[4096];
static char * SCHEMA_FMT_WITH(int pairs, const char *last) {
    int idx = 0;
    fmt[idx++] = '[';
    for (int i = 0; i < pairs; i++) {
        strcpy(fmt+idx, "(s,s)"); idx += 5;
        if (i+1 != pairs) fmt[idx++] = ',';
    }
    fmt[idx++] = ']';
    strcpy(fmt + idx, last);
    return fmt;
}
static char * SCHEMA_FMT(int pairs) {
    return SCHEMA_FMT_WITH(pairs, "");
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

#define DF5_SCHEMA_SIZE 18
#define DF5_SCHEMA \
    "c_customer_sk", "f4", \
    "c_customer_id", "S16", \
    "c_current_cdemo_sk", "f4", \
    "c_current_hdemo_sk", "f4", \
    "c_current_addr_sk", "f4", \
    "c_first_shipto_date_sk", "f4", \
    "c_first_sales_date_sk", "f4", \
    "c_salutation", "S10", \
    "c_first_name", "S20", \
    "c_last_name", "S30", \
    "c_preferred_cust_flag", "S1", \
    "c_birth_day", "f4", \
    "c_birth_month", "f4", \
    "c_birth_year", "f4", \
    "c_birth_country", "S20", \
    "c_login", "S13", \
    "c_email_address", "S50", \
    "c_last_review_date", "S10"

// TODO: confirm df6 schema
#define DF6_SCHEMA_SIZE 4
#define DF6_SCHEMA \
    "ctr_customer_sk", "f4", \
    "ctr_store_sk"   , "f4", \
    "ctr_return_amt" , "f4", \
    "c_customer_id", "S16"

// TODO: confirm type s2 -> s4
#define DF7_SCHEMA_SIZE 29
#define DF7_SCHEMA \
    "s_store_sk", "f4", \
    "s_store_id", "S16", \
    "s_rec_start_date", "S12", \
    "s_rec_end_date", "S12", \
    "s_closed_date_sk", "f4", \
    "s_store_name", "S52", \
    "s_number_employees", "f4", \
    "s_floor_space", "f4", \
    "s_hours", "S20", \
    "s_manager", "S40", \
    "s_market_id", "f4", \
    "s_geography_class", "S100", \
    "s_market_desc", "S100", \
    "s_market_manager", "S40", \
    "s_division_id", "f4", \
    "s_division_name", "S52", \
    "s_company_id", "f4", \
    "s_company_name", "S52", \
    "s_street_number", "S12", \
    "s_street_name", "S60", \
    "s_street_type", "S16", \
    "s_suite_number", "S12", \
    "s_city", "S60", \
    "s_county", "S32", \
    "s_state", "S4", \
    "s_zip", "S12", \
    "s_country", "S20", \
    "s_gmt_offset", "f4", \
    "s_tax_precentage", "f4"

#endif // _SCHEMA_H_
