import pandas as pd
import argparse
from collections import defaultdict


def parse_args():
    parser = argparse.ArgumentParser(description="CPU alignment")
    parser.add_argument(
        'cpu_trace_csv', type=str,
        help='The CSV stage log file from the program being profiled.'
    )
    parser.add_argument(
        'stage_trace_csv', type=str,
        help='The CPU usage CSV log file from profiler.'
    )
    return parser.parse_args()


def main():
    args = parse_args()

    cpu_trace_csv = args.cpu_trace_csv
    stage_trace_csv = args.stage_trace_csv

    # Prepare the dataframes
    cpu_trace_df = pd.read_csv(cpu_trace_csv)
    cpu_trace_df.columns = ["st", "ed", "cpu"]

    stage_trace_df = pd.read_csv(stage_trace_csv, header=None)
    stage_trace_df.columns = ["stage", "status", "lineno", "ts"]
    stage_trace_df = stage_trace_df[["stage", "ts"]]
    stage_trace_df = stage_trace_df.groupby("stage").agg(["min", "max"])
    stage_trace_df.columns = ["st", "ed"]

    # Prepare a dictionary to record the cpu usage of each sage.
    # For each row, do a range join if the cpu trace is within the stage trace time range.
    cpu_agg_trace = defaultdict(list)
    for i, cpu_row in cpu_trace_df.iterrows():  # row -> cpu trace
        for j, stage_row in stage_trace_df.iterrows():  # row2 -> stage trace
            # if cpu trace is within stage trace for either side, count it as is.
            if stage_row["st"] <= cpu_row["st"] and cpu_row["ed"] <= stage_row["ed"]:
                cpu_usage = cpu_row["cpu"]
                cpu_agg_trace[j].append((i, cpu_usage))

    cpu_agg_trace = {
        k: max(cpu_usage for _, cpu_usage in v)
        for k, v in cpu_agg_trace.items()
    }

    # Make child_list a dataframe
    aggregated_stage_df = pd.DataFrame.from_dict(cpu_agg_trace, orient="index")

    # join aggregated_stage_df with c
    final_df = stage_trace_df.join(aggregated_stage_df)
    final_df.columns = ["st", "ed", "peak_cpu"]
    print(final_df.to_csv())
    return final_df


if __name__ == '__main__':
    final_df = main()
