from Cython import Tempita
from Cython.Build import cythonize
import numpy
from setuptools import (
        Command,
        Extension,
        setup,
        )
from setuptools.command.build_ext import build_ext as _build_ext
import os

# options
debugging_symbols_requested = False

cur_dir = os.path.dirname(os.path.realpath(__file__))
def p(path):
    return os.path.join(cur_dir, path)

# general settings
macros = []

extra_compile_args = []
extra_link_args = []
if debugging_symbols_requested:
	extra_compile_args.append("-g")
	extra_compile_args.append("-UNDEBUG")
	extra_compile_args.append("-O0")

# general helpers

def srcpath(name=None, suffix=".pyx", subdir="src"):
    return os.path.join("npjoin", subdir, name + suffix)

class build_ext(_build_ext):
    @classmethod
    def render_templates(cls, pxifiles):
        for pxifile in pxifiles:
            # build pxifiles first, template extension must be .pxi.in
            print(f"randering file", pxifile)
            assert pxifile.endswith(".pxi.in")
            outfile = pxifile[:-3]

            if (
                os.path.exists(outfile)
                and os.stat(pxifile).st_mtime < os.stat(outfile).st_mtime
            ):
                # if .pxi.in is not updated, no need to output .pxi
                continue

            with open(pxifile) as f:
                tmpl = f.read()
            pyxcontent = Tempita.sub(tmpl)

            with open(outfile, "w") as f:
                f.write(pyxcontent)

    def build_extensions(self):
        # if building from c files, don't need to
        # generate template output
        if _CYTHON_INSTALLED:
            self.render_templates(_pxifiles)

        super().build_extensions()

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
    "hashtable": {
        "pyxfile": "hashtable",
        "include": klib_include,
        "depends": (
            ["npjoin/klib/khash_python.h", "npjoin/src/klib/khash.h"]
            + _pxi_dep["hashtable"]
        ),
    },
	"join": {"pyxfile": "join", "include": klib_include},
}

extensions = []

for name, data in ext_data.items():
    source_suffix = ".pyx"

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

def _cythonize(extensions, *args, **kwargs):
    kwargs["nthreads"] = 16
    build_ext.render_templates(_pxifiles)
    return cythonize(extensions, *args, **kwargs)

# main setup function
setup(
    name = "npjoin",
    ext_modules = _cythonize(extensions, language_level=3)
    )

