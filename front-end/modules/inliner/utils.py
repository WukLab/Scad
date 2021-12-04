import ast
def top_level_functions(body):
    return ([f.name, f] for f in body if isinstance(f, ast.FunctionDef))

def getFuncNameToNodeMap(body):
    res = dict()
    for name, node in top_level_functions(body):
        res[name] = node
    return res

def getExecListWithFuncName(root, funcName):
    bodyList = root.body
    execList = []
    for bodyNode in bodyList:
        if bodyNode.name == funcName:
            execList.append(bodyNode)
        if isinstance(bodyNode, ast.Expr):
            execList.append(bodyNode)
    return execList
