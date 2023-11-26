import argparse
import json
import re
import subprocess
from pathlib import Path
from typing import TypedDict

import pandas as pd


class MemLogItem(TypedDict):
    event: str
    ptr: str
    timestamp: str
    traceback: 'list[str]'
    pass


# { "event": "DeleteHook", "ptr": "0x5593f8f88010", "timestamp": "785", "traceback": ["/lib/x86_64-linux-gnu/libtcmalloc.so.4(_ZN10MallocHook20InvokeDeleteHookSlowEPKv+0x95) [0x7fbd29080695]", "/lib/x86_64-linux-gnu/libtcmalloc.so.4(+0x35b95) [0x7fbd2908cb95]", "/lib/x86_64-linux-gnu/libc.so.6(+0x3258f) [0x7fbd28c9a58f]", "/lib/x86_64-linux-gnu/libc.so.6(+0x311ce) [0x7fbd28c991ce]", "/lib/x86_64-linux-gnu/libc.so.6(setlocale+0x4c8) [0x7fbd28c98ba8]", "/lib/x86_64-linux-gnu/libpython3.8.so.1.0(_Py_SetLocaleFromEnv+0x12) [0x7fbd29404072]", "/lib/x86_64-linux-gnu/libpython3.8.so.1.0(_PyPreConfig_Read+0x2b7) [0x7fbd29406087]", "/lib/x86_64-linux-gnu/libpython3.8.so.1.0(_Py_PreInitializeFromPyArgv+0xd6) [0x7fbd29406626]", "/lib/x86_64-linux-gnu/libpython3.8.so.1.0(Py_PreInitialize+0x10) [0x7fbd29406680]", "/lib/x86_64-linux-gnu/libpython3.8.so.1.0(_Py_PreInitializeFromConfig+0xfb) [0x7fbd2940678b]", "/lib/x86_64-linux-gnu/libpython3.8.so.1.0(+0x197322) [0x7fbd29408322]", "/lib/x86_64-linux-gnu/libpython3.8.so.1.0(Py_InitializeFromConfig+0x13) [0x7fbd294091a3]", "/lib/x86_64-linux-gnu/libpython3.8.so.1.0(Py_InitializeEx+0x5c) [0x7fbd2940924c]", "./tpcds_1_mem(+0x35db) [0x5593f72c55db]", "./tpcds_1_mem(+0x4288) [0x5593f72c6288]", "/lib/x86_64-linux-gnu/libc.so.6(__libc_start_main+0xf3) [0x7fbd28c8c083]", "./tpcds_1_mem(+0x298e) [0x5593f72c498e]"]}


def parse_traceback(traceback: 'list[str]'):
    result = []
    pattern = re.compile(f'(?P<file_name>.+)\((?P<offset>(\+0x[0-9a-f]*))\)')
    for tb in traceback:
        match = pattern.match(tb)
        if not match:
            continue
        file_name = match.group('file_name')
        offset = match.group('offset')
        result.append((file_name, offset))
        pass
    return result


def translate_memlog(memlog: 'list[MemLogItem]', source_path, exec_path):
    offset_queries = set()
    _exec_path = exec_path.stem
    _source_path = source_path.stem
    df = pd.DataFrame(memlog)
    df['focused_offsets'] = df['traceback'].apply(parse_traceback).apply(
        lambda x: [offset for name, offset in x if _exec_path in name]
    )
    # aggregate the queries
    df['focused_offsets'].agg(lambda x: offset_queries.update(set(x)))

    # Now query the addr2line -e exec_path offset
    offset_queries = list(offset_queries)
    command = ['addr2line', '-e', exec_path] + list(offset_queries)
    proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = proc.stdout.decode('utf-8').split('\n')

    # Get the mapping from offset to the file name and line number
    offset_2_file = {}
    pattern = re.compile(r'(?P<file_name>.+):(?P<line_number>((\d+)|[\?]))(?P<comment>.*)')
    for offset, src_offset in zip(offset_queries, output):
        match = pattern.match(src_offset)
        if match is None:
            continue
        file_name = match.group('file_name')
        line_number = match.group('line_number')
        if file_name in ['??', '?', ''] or line_number in ['??', '?', '']:
            continue
        if _source_path not in file_name:
            continue
        # offset_2_file[offset] = (file_name, line_number)
        # offset_2_file[offset] = (_source_path, int(line_number))
        offset_2_file[offset] = int(line_number)

    def get_final_offset(x):
        for i in x:
            if i in offset_2_file:
                return offset_2_file[i]
        return None

    # Eventually get the "most relevant" file function / offset 
    df['line_num'] = df['focused_offsets'].apply(get_final_offset)
    df = df[['event', 'timestamp', 'size', 'line_num']]
    # Drop all rows where final_offset is NaN
    df = df.dropna(subset=['line_num'])

    # Timestamp base on min
    df['line_num'] = df['line_num'].astype(int)
    return df


def parse_args():
    # file
    args = argparse.ArgumentParser()
    args.add_argument('memlog_path', type=str, help='Path to the memory analysis JSONL log.')
    args.add_argument('--source_path', type=str,
                      help='Path to the source file for analysis. Currently only support one file, but can extend to multiple files (see source code).)',
                      required=True)
    args.add_argument('--exec_path', type=str, help='Path to the executable.', required=True)
    return args.parse_args()


def main():
    args = parse_args()
    # memlog_path = "./tpcds.debug.log"
    # exec_path = Path("../tpcds_1_mem")
    # source_path = Path("../reference_mem.cpp")
    memlog_path = Path(args.memlog_path)
    source_path = Path(args.source_path)
    exec_path = Path(args.exec_path)

    # Read memlog as line json
    memlog = []
    with open(memlog_path, 'r') as f:
        lines = f.readlines()
        for _line in lines:
            item = json.loads(_line)
            memlog.append(item)

    df = translate_memlog(memlog, source_path, exec_path)
    print(df.to_csv())
    pass


if __name__ == '__main__':
    main()
