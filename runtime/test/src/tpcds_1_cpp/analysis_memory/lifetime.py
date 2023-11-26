"""Variable Lifetime Analysis

Restriction:
- Only works for single function. Assume everything belongs to the same function (except for parameters that passing along).
- No global variable. We could do global variable at some other time, but no necessary for now.
"""

from functools import lru_cache
from pathlib import Path

import clang
import clang.cindex
from clang.cindex import CursorKind

try:
    # Set library file for clang.cindex
    clang.cindex.Config.set_library_file("/home/junda/.local/lib/python3.8/site-packages/clang/native/libclang-16.so")
except:
    pass

NODE_DECL_TYPE = [
    CursorKind.PARM_DECL,
    CursorKind.VAR_DECL,
]

NODE_REF_TYPE = [
    CursorKind.DECL_REF_EXPR,
]

NODE_SCOPE_TYPE = [
    CursorKind.FOR_STMT, CursorKind.WHILE_STMT, CursorKind.IF_STMT,
    CursorKind.COMPOUND_STMT, CursorKind.FUNCTION_DECL,
]


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
        import warnings
        warnings.warn(str(diag))
    if tu.diagnostics:
        warnings.warn(
            "Compiler error usually cause error to program analysis. If encountered downstream exception, consider look at function `parse_file` and alter the compiler flags.")
    return tu


def describe(node):
    return (node.kind.name, getattr(node.kind, "displayname", ""), node.spelling, node.extent)


def accessor(node, *pos):
    for p in pos:
        node = list(node.get_children())[p]
    return node


def traverse_children(node: 'clang.cindex.Cursor', depth=0):
    print('  ' * depth, *describe(node))
    for child in node.get_children():
        traverse_children(child, depth + 1)


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


def get_used_node_types():
    node_types = set()

    def get_all_used_node_types(node):
        node_types.add(node.kind)
        for child in node.get_children():
            get_all_used_node_types(child)

    get_all_used_node_types(_main_func)
    return node_types


def node_is_assignment(node: 'Cursor'):
    if node.kind != CursorKind.BINARY_OPERATOR:
        return False
    tokens = node.get_tokens()
    spellings = [t.spelling for t in tokens]
    if len(spellings) < 2:
        return False
    return spellings[1] == '='


def get_binary_op_lhs(node: 'Cursor'):
    assert node.kind == CursorKind.BINARY_OPERATOR
    first = next(node.get_tokens())
    return first.spelling


def traverse_children_with_life(root: 'clang.cindex.Cursor'):
    var_start = {}
    var_used = {}
    var_assigned = {}

    def _traverse_children_with_life(node: 'clang.cindex.Cursor', depth=0):

        if node.kind in NODE_DECL_TYPE:
            if node.spelling in var_start:
                print("Warning: variable {} is declared multiple times".format(node.spelling))
            var_start[node.spelling] = node

        if node.kind in NODE_REF_TYPE:
            var_used[node.spelling] = node

        if node.kind in [CursorKind.DECL_STMT]:
            pass

        # If node is a call expression, don't include the first element (function name).
        # Here we assume function name is always used.
        for i, child in enumerate(node.get_children()):
            if i == 0 and node.kind in [CursorKind.CALL_EXPR, CursorKind.FOR_STMT]:
                continue
            _traverse_children_with_life(child, depth + 1)
        return

    _traverse_children_with_life(root)
    return var_start, var_used


@lru_cache(maxsize=None)
def get_source_file(file_path):
    with open(file_path) as f:
        return f.read().splitlines(True)


def get_source_line(file_path, start, end):
    lines = get_source_file(file_path)
    return '\n'.join(lines[start.line - 1:end.line])


def describe_with_source(node):
    f = _main_func.extent.start.file.name
    return (f"{node.extent.start.line}:{node.extent.start.column}~{node.extent.end.line}:{node.extent.end.column}",
            get_source_line(f, node.extent.start, node.extent.end))


def find_path(root, target_node):
    # Traverse the root to find the path to the node.
    path = []

    def _traverse(node: 'clang.cindex.Cursor'):
        if node == target_node:
            path.append(node)
            return True
        for child in node.get_children():
            if _traverse(child):
                path.append(node)
                return True
        return False

    _trace = _traverse(root)
    return path[::-1]


def nearest_scope(path):
    # Find the nearest compound statement.
    result = path[0]
    for p in path:
        if p.kind in [CursorKind.FOR_STMT, CursorKind.WHILE_STMT, CursorKind.IF_STMT]:
            break
        if p.kind in [CursorKind.COMPOUND_STMT, CursorKind.FUNCTION_DECL]:
            result = p
            continue
        pass
    return result


def find_after_prefix(prefix_node, path):
    # Find the first node after the prefix.
    for i, p in enumerate(path):
        if p == prefix_node:
            return path[i:]
    else:
        return None


def get_node_enclose(x):
    return (x.line, x.column)


def is_node_enclosed(bigger_node, other):
    node = bigger_node
    return get_node_enclose(node.extent.start) <= get_node_enclose(other.extent.start) and get_node_enclose(
        node.extent.end) >= get_node_enclose(other.extent.end)


def find_next_sibling(scope_node, target_node):
    # Child may not be a direct child of the scope_node.
    # Try to find the node that encapsulate the child, then take the next sibling.
    children = list(scope_node.get_children())
    for i, child in enumerate(children):
        if i == len(children) - 1:
            return None
        if is_node_enclosed(child, target_node):
            return children[i + 1]
    return None


def find_enclosing_at_scope(scope_node, target_node):
    children = list(scope_node.get_children())
    for i, child in enumerate(children):
        if is_node_enclosed(child, target_node):
            return child
    return None


def suggest_delete_line(root, var_start, var_used):
    """Suggest deletion must happen at the enclose of the scope."""
    result = {}

    for var, node in var_start.items():
        if node.kind == CursorKind.PARM_DECL:
            continue
        if var not in var_used:
            # print(f"Unused variable {var}: ", *describe_with_source(node))
            result[var] = ('unused', node.extent.end.line)
            continue

        # Suggest the deletion line of the variable.
        used_node = var_used[var]
        used_path = find_path(root, used_node)
        used_scope = nearest_scope(used_path)
        for i, n in enumerate(used_path):
            if n == used_scope:
                a = used_path[i + 1]
                result[var] = ('last_used', a.extent.end.line)
                break

    return result


def describe_concise(node):
    return (
        node.kind.name, getattr(node.kind, "displayname", ""), node.spelling, node.extent.start.line,
        node.extent.end.line)


next_scope_id = 0


def traverse_children_with_life_scoped(root: 'clang.cindex.Cursor'):
    results = {}

    scope_def = [{}]
    scope_used = [{}]
    assigned_vars = set()

    def __traverse(node: 'clang.cindex.Cursor', depth=0):
        global next_scope_id
        if node.kind in NODE_DECL_TYPE:
            # print(f"Declaration: {node.spelling}. code: ", *describe_with_source(node))
            if node.spelling:
                if node.spelling in scope_def[-1]:
                    print(f"Warning: variable {node.spelling} is declared multiple times.")
                scope_def[-1][node.spelling] = node

        if node.kind == CursorKind.BINARY_OPERATOR:
            if node_is_assignment(node):
                lhs_var = get_binary_op_lhs(node)
                if lhs_var in assigned_vars:
                    print(
                        f"Warning: variable {lhs_var} is assigned multiple times. "
                        f"This time in line {node.extent.start.line}, "
                        f"where previously in line {assigned_vars[lhs_var].extent.start.line}"
                    )
                assigned_vars.add(lhs_var)
                # print(f"Assignment: {lhs_var} = ...")
                # TODO: This is assuming one variable only has one assignment.
                for _scope_id in range(len(scope_def) - 1, -1, -1):
                    if lhs_var in scope_def[_scope_id]:
                        scope_def[_scope_id][lhs_var] = node
                        scope_used[_scope_id][lhs_var] = node
            pass

        if node.kind in NODE_REF_TYPE:
            # Node get used - find the original definition and mark it as used.
            for _scope_id in range(len(scope_def) - 1, -1, -1):
                if node.spelling in scope_def[_scope_id]:
                    scope_used[_scope_id][node.spelling] = node
                    break

        # If node is a call expression, don't include the first element (function name).
        # Here we assume function name is always used.
        for i, child in enumerate(node.get_children()):
            if child.kind in [CursorKind.CALL_EXPR] and i == 0:
                continue
            if child.kind in NODE_SCOPE_TYPE:
                # enter a new scope
                scope_def.append({})
                scope_used.append({})

            __traverse(child, depth + 1)

            if child.kind in NODE_SCOPE_TYPE:
                # exit a scope
                a = scope_def.pop()
                b = scope_used.pop()
                if a or b:
                    results[next_scope_id] = (child, a, b)
                next_scope_id = next_scope_id + 1

        return

    __traverse(root)

    # Organize the result to get var_start, var_used.
    # But this time, `var_start` has to rename its variable with a line number as suffix ("@<line_number>")
    # ---> results # (scope_node_, scope_def_, scope_used_)
    var_start = {}
    var_used = {}
    for scope_id, (scope_node, scope_def, scope_used) in results.items():
        for var, node in scope_def.items():
            name = f"{var}@{node.extent.start.line}"
            var_start[name] = node
            if var in scope_used:
                var_used[name] = scope_used[var]

    return var_start, var_used


def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('source_path', type=Path, )
    parser.add_argument('--target_function', type=str, default="main_")
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    path = Path(args.source_path)
    target_function = args.target_function
    # path = Path("/home/junda/Scad/runtime/test/src/tpcds_1_cpp/reference_mem.cpp")
    # target_function = "main_"

    tu = parse_file(path)
    _main_func = locate_function_decl(tu.cursor, target_function)
    if not _main_func:
        raise ValueError(f"Cannot find function {target_function}")

    var_start, var_used = traverse_children_with_life_scoped(_main_func)
    var_end = suggest_delete_line(_main_func, var_start, var_used)

    # Organize a csv report
    result = []
    for var_name, (reason, end_line) in var_end.items():
        start_line = var_start[var_name].extent.start.line
        result.append((var_name, start_line, end_line))

    result.sort(key=lambda x: x[1])
    print("variable,start_line,end_line")
    for r in result:
        print(','.join(map(str, r)))
