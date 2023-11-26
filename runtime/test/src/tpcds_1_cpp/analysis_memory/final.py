# Calculate the per-symbol lifetime and memory usage.

import argparse 

import pandas as pd

def parse_args():
    args = argparse.ArgumentParser(
        usage='python final.py --lifetime_csv_path tpcds.lifetime.csv --line_activity_csv_path tpcds.line_activity.csv'
    )
    args.add_argument('--lifetime_csv_path', type=str, help='Path to the lifetime CSV file.', required=True)
    args.add_argument('--line_activity_csv_path', type=str, help='Path to the line activity CSV file.', required=True)
    args.add_argument('--display_markdown', action='store_true', help='Print markdown format to stdout.', default=False)
    return args.parse_args()

def human_readable_size(x):
    if x < 1024:
        return f'{x}B'
    elif x < 1024 * 1024:
        return f'{x / 1024:.2f}KB'
    elif x < 1024 * 1024 * 1024:
        return f'{x / 1024 / 1024:.2f}MB'
    else:
        return f'{x / 1024 / 1024 / 1024:.2f}GB'
    
def main():
    args = parse_args()
    lifetime_df = pd.read_csv(args.lifetime_csv_path)
    line_activity_df = pd.read_csv(args.line_activity_csv_path, index_col=0)
    line_activity_df = line_activity_df.drop(columns=['timestamp'])
    line_activity_df = line_activity_df[line_activity_df.event == 'NewHook']
    line_activity_df = line_activity_df.drop(columns=['event'])
    line_activity_df = line_activity_df.groupby(['line_num']).sum().reset_index()

    line_activity_df = line_activity_df.rename(columns={'size': 'alloc_footprint'})
    line_activity_df['alloc_footprint_h'] = line_activity_df['alloc_footprint'].apply(human_readable_size)

    df = lifetime_df.merge(line_activity_df, left_on='start_line', right_on='line_num', how='inner')
    if args.display_markdown:
        print(df.to_markdown())
    else:
        print(df.to_csv())

if __name__ == '__main__':
    main()