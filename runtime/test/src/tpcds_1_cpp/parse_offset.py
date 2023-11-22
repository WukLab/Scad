# Parse all offset from the JSONL log file. Reuse in the future.
import pickle
import argparse
import subprocess
import itertools
import json
import re
import sys
from collections import defaultdict
from typing import List, Protocol, cast
import dataclasses


class ParseArgs(Protocol):
    log_file: str
    target_executable: str
    output_srcline_csv_path: str
    output_src_file_pickle_path: str
    dry_run: bool


def parse_arguments(argv) -> ParseArgs:
    # add positional arguments <target-executable> <log_file>
    parser = argparse.ArgumentParser(description='Parse the offset from the JSONL log file.')
    parser.add_argument('target_executable', type=str, help='The target executable.')
    parser.add_argument(
        'log_file', type=str,
        help='The JSONL log file. Each line is a JSON that represents an event where tcmalloc hook gets called.'
    )
    parser.add_argument('output_srcline_csv_path', type=str,
                        help='The CSV file that contains the source code information.')

    parser.add_argument(
        'output_src_file_pickle_path', type=str,
        help='The pickle file that contains the source code information.'
    )
    # add flag --dry-run to print the command instead of executing it
    parser.add_argument('--dry-run', action='store_true', default=False,
                        help='Print the command instead of executing it.')
    return cast(ParseArgs, parser.parse_args(argv[1:]))


def is_user_file(file_path: str, criteria: 'function'):
    return criteria(file_path)


@dataclasses.dataclass
class LineInfo:
    exec_path: 'str'
    offset: 'str'
    file_name: 'str | None' = None
    line_number: 'str | None' = None
    comment: 'str | None' = None

    def csv_str(self):
        return f"{self.exec_path},{self.offset},{self.file_name or ''},{self.line_number or ''},{self.comment or ''}"


def translate_addr2line(exec_path: 'PathLike', offsets: List[str]) -> List[LineInfo]:
    result: List[LineInfo] = []
    command = ['addr2line', '-e', exec_path] + offsets
    # Run the subprocess, but don't let the subprocess inherit the stdout/stderr.
    proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = proc.stdout.decode('utf-8').split('\n')

    # Process each line of output such that it can parse something like:
    # /users/jchen693/DataFrame/include/DataFrame/Vectors/HeteroVector.tcc:66 (discriminator 1)
    # and parse out the
    #   - file_name: /users/jchen693/DataFrame/include/DataFrame/Vectors/HeteroVector.tcc
    #   - line_number: 66
    #   - comment: (anything after the integer number, if any)
    pattern = re.compile(r'(?P<file_name>.+):(?P<line_number>((\d+)|[\?]))(?P<comment>.*)')
    for offset, line in zip(offsets, output):
        match = pattern.match(line)

        if match is None:
            # Usually it means we have ??:? line.
            # Let's just advance in this case.
            result.append(
                LineInfo(
                    exec_path=exec_path,
                    offset=offset,
                    file_name='',
                    line_number='',
                    comment=line
                )
            )
            continue

        file_name = match.group('file_name')
        line_number = match.group('line_number')
        comment = match.group('comment')

        if file_name in ['??', '?', '']:
            file_name = None
        if line_number in ['??', '?', '']:
            line_number = None

        result.append(
            LineInfo(
                exec_path=exec_path,
                offset=offset,
                file_name=file_name,
                line_number=line_number,
                comment=comment
            )
        )

    return result


def main(target_executable: str, log_file: str, output_srcline_csv_path: str, output_src_file_pickle_path: str,
         dry_run: bool = True):
    with open(log_file, 'r') as f:
        log_lines = f.readlines()
    log_items = [json.loads(line.rstrip('\n')) for line in log_lines]
    tracebacks = itertools.chain.from_iterable([item['traceback'] for item in log_items])

    # See: https://regex101.com/r/ZoYwjh/1
    pattern = re.compile(r'(?P<file_path>.+)\(((?P<extsymbol>[^+]+)?\+(?P<offset>.+))?\) \[(?P<address>[x0-9a-f]+)\]')
    result = defaultdict(set)
    for tb in tracebacks:
        match = pattern.match(tb)
        if match is None:
            raise ValueError(f'Cannot parse the traceback: {tb}')
        file_path = match.group('file_path')
        offset = match.group('offset') or match.group('address')
        result[file_path].add(offset)

    line_info_all = []
    for key, items in result.items():
        if dry_run:
            print(f'addr2line -e {key} {" ".join(items)}')
            continue
        # Translate the line information.
        line_infos = translate_addr2line(key, list(items))
        line_info_all.extend(line_infos)

    with open(output_srcline_csv_path, 'w') as f:
        # f.write("exec_path,offset,file_name,line_number,comment\n")
        for line_info in line_info_all:
            f.write(line_info.csv_str() + "\n")

    # Copy all files as a designated dictionary named `exec_path.files.pkl` to the current directory.
    related_files = {}
    files = [info.file_name for info in line_info_all if info.file_name is not None]
    for file in files:
        try:
            with open(file, 'r') as f:
                related_files[file] = f.read()
        except:
            import warnings
            warnings.warn(f'File not found (skipped): {file}.')
            pass

    # Save the dictionary to the current directory.
    with open(output_src_file_pickle_path, 'wb') as f:
        pickle.dump(related_files, f)

    return related_files


if __name__ == '__main__':
    parser = parse_arguments(sys.argv)
    main(
        target_executable=parser.target_executable,
        log_file=parser.log_file,
        output_srcline_csv_path=parser.output_srcline_csv_path,
        output_src_file_pickle_path=parser.output_src_file_pickle_path,
        dry_run=parser.dry_run
    )
