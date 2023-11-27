"""
Annotate time spent for different stages for the program. s
`cpuannotate.py` takes an input of <program> <target_function>, and
only focus on the top-level of that function in the program to understand time spent on each of these stages.
"""
import shutil
import sys
import warnings
from pathlib import Path

import clang
import clang.cindex
from clang.cindex import CursorKind

assert sys.version_info >= (3, 8) and sys.version_info < (3, 9), (
    f"Python version must be 3.8 as of now. "
    f"I do not know how to extend this to other python versions just yet."
)

try:
    # Set library file for clang.cindex
    clang.cindex.Config.set_library_file(
        "/home/junda/.local/lib/python3.8/site-packages/clang/native/libclang-16.so"
    )
except:
    print("Library path already set once.")
    pass


def parse_file(path):
    idx = clang.cindex.Index.create()
    source_path = path
    home_path = Path.home()
    tu = idx.parse(
        path, args=[
            # On macos
            # '-I/usr/lib/clang/11/include',
            # '-std=c++17',
            # '-I/mnt/ssd2/junda/.pyenv/versions/3.10.6/include/python3.10',
            # '-I/home/junda/.pyenv/versions/3.10.6/lib/python3.10/site-packages/numpy/core/include',
            '-I/usr/include/python3.8',
            '-I/usr/lib/llvm-10/lib/clang/10.0.0/include',
            '-I/home/junda/.local/lib/python3.8/site-packages/numpy/core/include',
            '-std=c++17',
        ])
    for diag in tu.diagnostics:
        warnings.warn(str(diag))
    if tu.diagnostics:
        warnings.warn(
            "Compiler error usually cause error to program analysis. If encountered downstream exception, consider look at function `parse_file` and alter the compiler flags.")
    return tu


def describe(node):
    return (node.kind.name, getattr(node.kind, "displayname", ""), node.spelling, node.extent)


def describe_concise(node):
    f = node.extent.start.file.name
    return (node.kind.name, getattr(node.kind, "displayname", ""), node.spelling,
            f"{node.extent.start.line}:{node.extent.start.column}~{node.extent.end.line}:{node.extent.end.column}")


def get_source_file(file_path):
    with open(file_path) as f:
        return f.read().splitlines(True)


def get_source_line(file_path, start, end):
    lines = get_source_file(file_path)
    return '\n'.join(lines[start.line - 1:end.line])


def describe_with_source(node):
    f = node.extent.start.file.name
    return (f"{node.extent.start.line}:{node.extent.start.column}~{node.extent.end.line}:{node.extent.end.column}",
            get_source_line(f, node.extent.start, node.extent.end))


def traverse_children(node: 'clang.cindex.Cursor', depth=0, describe_func=describe, max_depth=None):
    kwargs = dict(locals())
    kwargs.pop('node')
    kwargs.pop('depth')
    if max_depth is not None and depth > max_depth:
        return
    print('  ' * depth, *describe_func(node))
    for child in node.get_children():
        traverse_children(child, **kwargs, depth=depth + 1)


def locate_function_decl(root, name: str):
    """
    Traverse the AST to find the function declaration with the spelling matching the `name`.
    :param root:
    :param name:
    :return:
    """

    def _traverse(node: 'clang.cindex.Cursor'):
        if node.kind == clang.cindex.CursorKind.FUNCTION_DECL and node.spelling == name:
            return node
        for child in node.get_children():
            result = _traverse(child)
            if result is not None:
                return result
        return None

    return _traverse(root)


def get_used_node_types(target):
    node_types = set()

    def get_all_used_node_types(node):
        node_types.add(node.kind)
        for child in node.get_children():
            get_all_used_node_types(child)

    get_all_used_node_types(target)
    return node_types


def traverse_top_ish(node, depth=0, describe_func=describe_concise):
    """Only if the staetment is a compound statement you wil"""
    kwargs = dict(locals())
    kwargs.pop('node')
    kwargs.pop('depth')
    if node.kind not in [CursorKind.RETURN_STMT, CursorKind.PARM_DECL]:
        print('  ' * depth, *describe_func(node))
    for child in node.get_children():
        if node.kind not in [CursorKind.COMPOUND_STMT, CursorKind.FUNCTION_DECL]:
            continue
        traverse_top_ish(child, **kwargs, depth=depth + 1)
    return


def collect_top_ish(root):
    result = []  # Collect the leaves

    def _collect(node):
        if node.kind not in [CursorKind.COMPOUND_STMT, CursorKind.FUNCTION_DECL] + [CursorKind.RETURN_STMT,
                                                                                    CursorKind.PARM_DECL]:
            result.append(node)
        for child in node.get_children():
            if node.kind not in [CursorKind.COMPOUND_STMT, CursorKind.FUNCTION_DECL]:
                continue
            _collect(child)
        return

    _collect(root)
    return result


def describe_with_offset(node):
    f = node.extent.start.file.name
    return (node.kind.name, getattr(node.kind, "displayname", ""), node.spelling,
            f"{node.extent.start.line}:{node.extent.start.column}~{node.extent.end.line}:{node.extent.end.column}",
            f"offset={node.extent.start.offset}~{node.extent.end.offset}")


def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('source_path', type=Path, )
    parser.add_argument('--target_function', type=str, default="main")
    return parser.parse_args()


def get_time_macro_start(name, line):
    return '{' + f'__stgst("{name}", {line});' + '}'


def get_time_macro_end(name, line):
    return '{' + f'__stged("{name}", {line});' + '}'


if __name__ == '__main__':
    args = parse_args()
    path = args.source_path
    tu = parse_file(path)
    root = tu.cursor
    target_func_node = locate_function_decl(root, args.target_function)

    # Debug
    # traverse_top_ish(_main_func, describe_func=describe_with_offset)
    result = collect_top_ish(target_func_node)
    # for i in result:
    #     print(*describe_concise(i))
    # traverse_children(target, describe_func=describe_concise)
    # print(get_used_node_types(target))

    # Now copy the original file, and insert the annotation.
    new_path = path.parent / (path.stem + "_annotated.cpp")
    shutil.copy(path, new_path)

    codes = get_source_file(path)
    codes = [i.strip('\n') for i in codes]

    # Now traverse the `result` and insert the instruction to print time.
    for i, node in enumerate(reversed(result)):
        name_idx = len(result) - i
        # print(node.extent.start.line, node.extent.start.column, node.extent.end.line, node.extent.end.column,
        #       get_source_line(node.extent.start.file.name, node.extent.start, node.extent.end))
        st, ed = node.extent.start.line, node.extent.end.line
        st_macro = get_time_macro_start(name_idx, st)
        codes[st - 1] = st_macro + codes[st - 1]
        # print(st, codes[st - 1])

        ed_macro = get_time_macro_end(name_idx, ed)
        codes[ed - 1] = codes[ed - 1] + ed_macro
        # print(ed, codes[ed - 1])

    # TODO: Probably consider setting up a log file.
    headers = [
        '#include <chrono>',
        ('#define __stgcommon(name, line, status) {'
         'auto t = std::chrono::high_resolution_clock::now(); '
         'std::cerr '
         '<< name << "," << status << "," << line << "," '
         '<< std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count() '
         '<< std::endl;}'),
        '#define __stgst(name, line) {__stgcommon(name, line, "start")}',
        '#define __stged(name, line) {__stgcommon(name, line, "end")}',
    ]
    codes = headers + codes

    codes = [i + '\n' for i in codes]

    with open(new_path, 'w') as f:
        f.writelines(codes)

    # Finally, in the build directory, write your make file and run the following command for compiling.
    # ! make simplemt_annotated.out
    # ! ./simplemt_annotated.out 2>simplemt_annotated.cpulog.csv
    # ! cat simplemt_annotated.cpulog.csv
