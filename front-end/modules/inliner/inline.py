import ast
import astpretty
import utils
import astor
import astunparse
import copy
from visitor import *
from symbolHelper import *

# function which we will not inline
blackListFuncList = ["main", "append"]
mainFuncName = "main"

# @todo build topo graph, so that we process function with the same order as call graph
# @todo function call as arguments not supported yet
# @todo handling built-in class and related function call

def GetNumOfReturnEle(callFuncDef):
    # assume last stmt is return stmt
    bodyList = callFuncDef.body
    expectReturnNode = bodyList[-1]
    if isinstance(expectReturnNode, ast.Return):
        # currently we assume there is no recursive call happened return 
        if hasattr(expectReturnNode.value, "elts"):
            return len(expectReturnNode.value.elts)
        else:
            return 1
    else:
        return 0

def GetStmtCallNodeMap(stmt):
    global blackListFuncList
    collector = collectCallNodeVisitor(blackListFuncList, stmt)
    collector.visit(stmt)
    return collector.getCallNodeMap()

def CalleeArgProcess(callNode, funcNode):
    global definedFuncNameList
    curFuncNode = copy.deepcopy(funcNode)
    args = curFuncNode.args.args
    argMap = dict()
    for i, argNode in enumerate(args):
        argMap[argNode.arg] = callNode.args[i]

    curFuncNode = ReplaceParamVisitor(argMap).visit(curFuncNode)
    # inlineAgain = False
    # for _, nameObject in argMap.items():
    #     if nameObject.id in definedFuncNameList:
    #         inlineAgain = True
    # if inlineAgain:
    curFuncNode.body = InlineBody(curFuncNode.body)
    return curFuncNode


 # @todo when handling assign node, we assume there is only one stmt;
 # @todo but when handling if stmt, callNodelist involved in many stmts
def IfStmtHandler(stmt, funcNameToNodeMap):
    currentCallNodeMap = GetStmtCallNodeMap(stmt)
    stmtList, stmtMap = procossCallNodeMap(funcNameToNodeMap, currentCallNodeMap, True)
    ifStmtList = list(currentCallNodeMap.keys())
    topStmt = ifStmtList[0]
    topStmt = processIfStmtHelper(topStmt, stmtMap)
    stmtList.append(topStmt)
    return stmtList

def processIfStmtHelper(curIfStmt, stmtMap):
    # if there is call node
    if len(stmtMap[curIfStmt]):
        funcNameToRegister = stmtMap[curIfStmt]
        curIfStmt.test = ReplaceCallNodeVisior(funcNameToRegister).visit(curIfStmt.test)
    if len(curIfStmt.body):
        curIfStmt.body = InlineBody(curIfStmt.body)
    tmpOrELseList = []
    for i in range(len(curIfStmt.orelse)):
        if isinstance(curIfStmt.orelse[i], ast.If):
            tmpOrELseList.append(processIfStmtHelper(curIfStmt.orelse[0], stmtMap))
        else:
            for subOrELseStmt in InlineBody([curIfStmt.orelse[i]]):
                tmpOrELseList.append(subOrELseStmt)
    curIfStmt.orelse = tmpOrELseList
    return curIfStmt

def procossCallNodeMap(funcNameToNodeMap, currentCallNodeMap, isIfStmt=False):
    global symbolSet
    stmtList = []
    ifStmtMap = dict()
    for stmtInMap, currentCallNodeList in currentCallNodeMap.items():
        funcNameToRegister = dict()
        for curCallNode in currentCallNodeList:
            callFuncName = curCallNode.func.id
            callFuncDef = funcNameToNodeMap[callFuncName]
            # process function variable based on call node
            callFuncDef = CalleeArgProcess(curCallNode, callFuncDef)
            # process inner function stmt
            numOfReturnEle = GetNumOfReturnEle(callFuncDef)
            # we should not change the call func stmt when we extend it
            callFuncBodyList = copy.deepcopy(callFuncDef.body)
            if numOfReturnEle > 0:
                curSymbolList, symbolSet = generateSymbol(symbolSet, numOfReturnEle)
                eltsList = []
                for curSymbol in curSymbolList:
                    eltsList.append(ast.Name(id=curSymbol, ctx=ast.Store()))
                returnStmt = callFuncBodyList[-1]
                if numOfReturnEle > 1:
                    curAssignNode = ast.Assign(targets=[(ast.Tuple(elts=eltsList))], value=returnStmt.value)
                    funcNameToRegister[callFuncName] = [(ast.Tuple(elts=eltsList))]
                else:
                    curAssignNode = ast.Assign(targets=[eltsList[0]], value=returnStmt.value)
                    funcNameToRegister[callFuncName] = [eltsList[0]]
                callFuncBodyList[-1] = curAssignNode
            for subStmtInCallFuncBody in callFuncBodyList:
                stmtList.append(subStmtInCallFuncBody)
        if isIfStmt:
            ifStmtMap[stmtInMap] = copy.deepcopy(funcNameToRegister)
        else:
            # function call with no return element, then we should not append original call expr
            if len(funcNameToRegister) == 0 and len(currentCallNodeList) > 0:
                pass
            else:
                stmtList.append(ReplaceCallNodeVisior(funcNameToRegister).visit(stmtInMap))
    if isIfStmt:
        return stmtList, ifStmtMap
    return stmtList

def InlineBody(bodyList):
    global symbolSet
    updateBodyList = []
    for stmt in bodyList:
        if isinstance(stmt, ast.If):
            for subStmt in IfStmtHandler(stmt, funcNameToNodeMap):
                updateBodyList.append(subStmt)
        elif isinstance(stmt, (ast.Assign, ast.AugAssign, ast.AnnAssign)):
            currentCallNodeMap = GetStmtCallNodeMap(stmt)
            for subStmt in procossCallNodeMap(funcNameToNodeMap, currentCallNodeMap):
                updateBodyList.append(subStmt)
        # currently inliner only support inlining body part of with, while, for
        elif isinstance(stmt, (ast.With, ast.For, ast.While)):
            stmt.body = InlineBody(stmt.body)
            updateBodyList.append(stmt)
        elif isinstance(stmt, ast.Expr):
            currentCallNodeMap = GetStmtCallNodeMap(stmt)
            # assert(len(currentCallNodeList) == 1, "Expr as Stmt has more than one call node")
            if len(currentCallNodeMap[stmt]) > 0:
                for subStmt in procossCallNodeMap(funcNameToNodeMap, currentCallNodeMap):
                    updateBodyList.append(subStmt)
            else:
                updateBodyList.append(stmt)
        else:
            updateBodyList.append(stmt)
    return updateBodyList

def InlineModule(module, mainFuncName):
    updateBodyList = []
    for subBody in module.body:
        if isinstance(subBody, ast.FunctionDef):
            if subBody.name == mainFuncName:
                updateBodyList.append(subBody)
        else:
            updateBodyList.append(subBody)
    module.body = updateBodyList

def updateFuncOrderAndBlackList(definedFuncNameList, funcNameToNodeMap, mainFuncName):
    global blackListFuncList
    resList = []
    indegreeMap = dict()
    funcCallMap = dict()
    for curFuncName in definedFuncNameList:
        indegreeMap[curFuncName] = 0
        funcCallMap[curFuncName] = []
    for curFuncName in definedFuncNameList:
        # @todo up to this step -> keep build graph
        # @todo think about generating all function call name
        curFuncDef = funcNameToNodeMap[curFuncName]
        calleeFuncSet = set()
        curBodyList = copy.deepcopy(curFuncDef.body)
        for stmt in curBodyList:
            currentCallNodeMap = GetStmtCallNodeMap(stmt)
            for subStmt, funcList in currentCallNodeMap.items():
                for curCallNode in funcList:
                    calleeFuncSet.add(curCallNode.func.id)
        for calleeFuncName in calleeFuncSet:
            if calleeFuncName in definedFuncNameList:
                funcCallMap[calleeFuncName].append(curFuncName)
                indegreeMap[curFuncName] = indegreeMap[curFuncName] + 1
            else:
                blackListFuncList.append(calleeFuncName)

    # @todo update blacklist function in main
    mainFuncDef = funcNameToNodeMap[mainFuncName]
    calleeFuncSet = set()
    curBodyList = copy.deepcopy(mainFuncDef.body)
    for stmt in curBodyList:
        currentCallNodeMap = GetStmtCallNodeMap(stmt)
        for subStmt, funcList in currentCallNodeMap.items():
            for curCallNode in funcList:
                calleeFuncSet.add(curCallNode.func.id)
    for calleeFuncName in calleeFuncSet:
        if calleeFuncName not in definedFuncNameList:
            blackListFuncList.append(calleeFuncName)

    funcQueue = []
    for funcName, indegree in indegreeMap.items():
        if indegree == 0:
            funcQueue.append(funcName)
    while funcQueue:
        curFuncName = funcQueue.pop(0)
        resList.append(curFuncName)
        for callerFuncName in funcCallMap[curFuncName]:
            indegreeMap[callerFuncName] = indegreeMap[callerFuncName] - 1
            if indegreeMap[callerFuncName] == 0:
                funcQueue.append(callerFuncName)
    # print(blackListFuncList)
    return resList


def inline(source):
    # data strcuture preprocess
    srcModule = ast.parse(source)
    symbolSet = getSymbolSet(path)
    funcNameToNodeMap = utils.getFuncNameToNodeMap(srcModule.body)
    # list of functions that is defined by user except the mainFuncName we will inline
    definedFuncNameList = []
    for definedFuncName in funcNameToNodeMap.keys():
        if definedFuncName != mainFuncName:
            definedFuncNameList.append(definedFuncName)

    # rename variables in definedFuncs
    renameVisitorFuncName = copy.deepcopy(definedFuncNameList)
    srcModule = ReNameVariableVisitor(renameVisitorFuncName, blackListFuncList).visit(srcModule)

    # extend definedFuncs before extend the main functions
    definedFuncNameList = updateFuncOrderAndBlackList(definedFuncNameList, funcNameToNodeMap, mainFuncName)

    # for definedFuncName in definedFuncNameList:
    #     defineFuncDefNode = funcNameToNodeMap[definedFuncName]
    #     defineFuncDefNode.body = InlineBody(defineFuncDefNode.body)
    #     funcNameToNodeMap[definedFuncName] = defineFuncDefNode

    mainFuncDefNode = funcNameToNodeMap[mainFuncName]
    mainFuncDefNode.body = InlineBody(mainFuncDefNode.body)
    InlineModule(srcModule, mainFuncName)
    code = astunparse.unparse(srcModule)
    return code

    # mainFuncDefNode.body = InlineBody(mainFuncDefNode.body)
    # print(mainFuncDefNode.body)
    # code = astor.to_source(mainFuncDefNode)
    # print(code)
    

if __name__ == '__main__':
    # Project Config
    path = "test/lr.py"
    # path = "test/unitTest/lc_crteo.py"
    inline(path)
    
