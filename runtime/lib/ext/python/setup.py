from setuptools import Extension, setup
from Cython.Build import cythonize
import os

cur_dir = os.path.dirname(os.path.realpath(__file__))
def p(path):
    return os.path.join(cur_dir, path)

setup(
    name = "disagg-python",
    ext_modules = cythonize([Extension("disagg", [p("disagg.pyx")],
            include_dirs = [p('../../include')],
            library_dirs=[p('../..')],
            libraries = ['d', 'nanomsg', 'curl'])],
        language_level=3)
    )

