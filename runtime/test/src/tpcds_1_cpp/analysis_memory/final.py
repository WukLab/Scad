# Calculate the per-symbol lifetime and memory usage.

import argparse 

import pandas as pd

lifetime_df = pd.read_csv('tpcds.lifetime.csv')
line_activity_df = pd.read_csv('tpcds.line_activity.csv', index_col=0)
line_activity_df = line_activity_df.drop(columns=['timestamp'])
line_activity_df = line_activity_df[line_activity_df.event == 'NewHook']
line_activity_df = line_activity_df.drop(columns=['event'])
line_activity_df = line_activity_df.groupby(['line_num']).sum().reset_index()

def human_readable_size(x):
    if x < 1024:
        return f'{x}B'
    elif x < 1024 * 1024:
        return f'{x / 1024:.2f}KB'
    elif x < 1024 * 1024 * 1024:
        return f'{x / 1024 / 1024:.2f}MB'
    else:
        return f'{x / 1024 / 1024 / 1024:.2f}GB'

line_activity_df = line_activity_df.rename(columns={'size': 'alloc_footprint'})
line_activity_df['alloc_footprint'] = line_activity_df['alloc_footprint'].apply(human_readable_size)

df = lifetime_df.merge(line_activity_df, left_on='start_line', right_on='line_num', how='inner')
print(df.to_markdown())