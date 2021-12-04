import ast
import symtable

seq = 0
prefix = "virtual_return_register_"

def getSymbolSet(path):
    global seq
    seq = 0
    src = open(path).read()
    codeSymbolTable = symtable.symtable(src, '?', 'exec') 
    symbolSet = set()
    for subTable in codeSymbolTable.get_children():
        for symbol in subTable.get_identifiers():
            symbolSet.add(symbol)
    return symbolSet

def generateSymbol(symbolSet, numOfSymbol):
    global seq
    symbolList = []
    for i in range(numOfSymbol):
        curSymbol = prefix + str(seq)
        seq += 1
        symbolList.append(curSymbol)
        symbolSet.add(curSymbol)
    return symbolList, symbolSet


