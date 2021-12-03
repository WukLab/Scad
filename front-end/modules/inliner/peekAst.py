import ast
import astpretty
import utils
import astor
import astunparse
from visitor import *
from symbolHelper import *

path = "test/unitTest/test3.py"
# function which we will not inline
blackListFuncList = ["print", "main"]
mainFuncName = "main"

src = open(path).read()
srcModule = ast.parse(src)
astpretty.pprint(srcModule)

