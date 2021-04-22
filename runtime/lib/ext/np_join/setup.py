from setuptools import Extension, setup
from Cython.Build import cythonize
import os

# options
debugging_symbols_requested = False

cur_dir = os.path.dirname(os.path.realpath(__file__))
def p(path):
    return os.path.join(cur_dir, path)

# general settings
extra_compile_args = []
extra_link_args = []
if debugging_symbols_requested:
	extra_compile_args.append("-g")
	extra_compile_args.append("-UNDEBUG")
	extra_compile_args.append("-O0")

# handles PXI inputs
_pxi_dep_template = {
    # "algos": ["algos_common_helper.pxi.in", "algos_take_helper.pxi.in"],
    "hashtable": [
        "hashtable_class_helper.pxi.in",
        "hashtable_func_helper.pxi.in",
        "khash_for_primitive_helper.pxi.in",
    ]
}

_pxifiles = []
_pxi_dep = {}
for module, files in _pxi_dep_template.items():
    pxi_files = [os.path.join("npjoin", x) for x in files]
    _pxifiles.extend(pxi_files)
    _pxi_dep[module] = pxi_files

klib_include = ["npjoin/klib"]

# handles extension
ext_data = {
    "_libs.hashtable": {
        "pyxfile": "_libs/hashtable",
        "include": klib_include,
        "depends": (
            ["pandas/_libs/src/klib/khash_python.h", "pandas/_libs/src/klib/khash.h"]
            + _pxi_dep["hashtable"]
        ),
    },
	"_libs.join": {"pyxfile": "_libs/join", "include": klib_include},
}

extensions = []

for name, data in ext_data.items():
    source_suffix = suffix if suffix == ".pyx" else data.get("suffix", ".c")

    sources = [srcpath(data["pyxfile"], suffix=source_suffix, subdir="")]

    sources.extend(data.get("sources", []))

    include = data.get("include", [])
    include.append(numpy.get_include())

    obj = Extension(
        f"npjoin.{name}",
        sources=sources,
        depends=data.get("depends", []),
        include_dirs=include,
        language=data.get("language", "c"),
        define_macros=data.get("macros", macros),
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
    )

    extensions.append(obj)

# main setup function
setup(
    name = "disagg-python",
    ext_modules = cythonize([Extension("disagg", [p("disagg.pyx")],
            include_dirs = [p('../../include')],
            library_dirs=[p('../..')],
            libraries = ['d', 'nanomsg', 'curl'])],
        language_level=3)
    )

