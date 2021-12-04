import ast
from collections import defaultdict
import astunparse


class ReNameVariableVisitor(ast.NodeTransformer):
    def __init__(self, funcNameList, blackListFuncs):
        self.prefix = ""
        self.funcNameList = funcNameList
        self.blackListFuncs = blackListFuncs
        self.funcNameList.extend(self.blackListFuncs)
        # avoid renaming args since args may have functions, and we will not rename functions
        self.current_args = set()

    def visit_FunctionDef(self, node):
        self.prefix = node.name
        self.current_args = set()
        for argObject in node.args.args:
            self.current_args.add(argObject.arg) 
        if self.prefix not in self.blackListFuncs:
            self.generic_visit(node)
        return node

    def visit_List(self, node):
        for subNode in node.elts:
            if isinstance(subNode, ast.Name):
                self.visit_Name(subNode)
        return node

    def visit_Subscript(self, node):
        self.visit(node.value)
        self.visit(node.slice)
        return node 

    def visit_Call(self, node):
        # function all as a object call its attribute
        if isinstance(node.func, ast.Attribute):
            self.visit(node.func)
        for funcArg in node.args:
            self.visit(funcArg)
        return node

    def visit_Name(self, node):
        if node.id not in self.funcNameList and node.id not in self.current_args:
            node.id = self.prefix + "_" + node.id
        return node

class ReplaceCallNodeVisior(ast.NodeTransformer):
    def __init__(self, nameToRegisterDict):
        self.nameToRegisterDict = nameToRegisterDict

    def visit_Call(self, node):
        funcName = node.func.id
        if (funcName in self.nameToRegisterDict):
            return self.nameToRegisterDict[funcName]
        return node
            

class collectCallNodeVisitor(ast.NodeVisitor):
    def __init__(self, funcBlackList, stmt):
        self.funcBlackList = funcBlackList
        # the defaultdict order keys in insertion order -> is what we need; o.w. trouble
        self.callNodeMap = defaultdict(list)
        self.curStmt = stmt
        # explicitly set for stmt without callNode
        self.callNodeMap[self.curStmt] = []

    def visit_Assign(self, node):
        if self.nodeQualifyCheck(node.value):
            self.callNodeMap[self.curStmt].append(node.value)
        elif isinstance(node.value, ast.BinOp):
            self.visit_Binop(node.value)

    def visit_If(self, node):
        self.curStmt = node
        self.callNodeMap[self.curStmt] = []
        if hasattr(node, "test"):
            self.visit(node.test)
        # Todo assume there is only one if node in orelse; does this assumpytion valid???
        if len(node.orelse):
            if isinstance(node.orelse[0], ast.If):
                self.visit_If(node.orelse[0])

    def visit_Compare(self, node):
        if self.nodeQualifyCheck(node.left):
            self.callNodeMap[self.curStmt].append(node.left)
        for comparator in node.comparators:
            if self.nodeQualifyCheck(comparator):
                self.callNodeMap[self.curStmt].append(comparator)


    def visit_Expr(self, node):
        if self.nodeQualifyCheck(node.value):
            self.callNodeMap[self.curStmt].append(node.value)
    
    def visit_Binop(self, node):
        if self.nodeQualifyCheck(node.left):
            self.callNodeMap[self.curStmt].append(node.left)
        elif isinstance(node.left, ast.BinOp):
            self.visit_Binop(node.left)

        if self.nodeQualifyCheck(node.right):
            self.callNodeMap[self.curStmt].append(node.right)
        elif isinstance(node.right, ast.BinOp):
            self.visit_Binop(node.right)
        
    def getCallNodeMap(self):
        return self.callNodeMap

    def nodeQualifyCheck(self, node):
        # @todo node.func maybe an attrbiute if we call on an object
        return isinstance(node, ast.Call) and hasattr(node.func, "id") and node.func.id not in self.funcBlackList

class ReplaceParamVisitor(ast.NodeTransformer):
    def __init__(self, argMap):
        self.argMap = argMap

    def visit_Name(self, node):
        if node.id in self.argMap:
            return self.argMap[node.id]
        return node

class PrintVisitor(ast.NodeTransformer):
    def __init__(self):
        pass
    def visit_Expr(self, node):
        node = self.generic_visit(node)
        if isinstance(node.value, ast.Assign):
            print(node.value.id)
        return node