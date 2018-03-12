#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 29 23:57:57 2018

@author: mitesh
"""
#ChangeLog v4
# 1 Check if Aggregrate Functions are Present
# 2 Check the Common Columns
# 3 Generate Indices for Column Projection
# 4 Solve Aggregate Functions
# 5 Put Distinct Keyword!
# 6 Check for installation of SQL Parse Package

#-----------------------------GET SQL PARSE-----------------------------

import sys
import os
sys.path.insert(0,os.getcwd()+"/sqlparse-0.2.4")

#------------------------------IMPORTS----------------------------------------

import sqlparse
import metadata
import re

#------------------------HELPER FUNCTIONS-------------------------------------

commCols = []
def createHeaders(query_tables):
    qtables = query_tables.split(',')
    header = []
    for table in qtables:
        if table in metadata.tables:
            h1 = metadata.tables[table]
            for col in h1:
                header.append(table+"."+col)
        else:
            return []
    return header

def seperateOperands(op1):
    i = 0
    ans = []
    operator = ""
    while i < len(op1):
        if op1[i] == '>' and op1[i+1] != '=':
            operator = ">"
            i += 1
        elif op1[i] == '<' and op1[i+1] != '=':
            operator = "<"
            i += 1
        elif op1[i] == '>' and op1[i+1] == '=':
            operator = ">="
            i += 1
        elif op1[i] == '<' and op1[i+1] == '=':
            operator = "<="
            i += 1
        elif op1[i] == '=' and (op1[i+1] != '>' or op1[i+1] != '<' or op1[i+1] != '!' or op1[i+1] != '=' or op1[i-1] != '='):
            operator = "="
            i += 1
        elif op1[i] == '!' and op1[i+1] == '=':
            operator = "!="
            i += 1
        i += 1
    ans = op1.split(operator)
    if operator == '=':
        ans.append("==")
    else:
        ans.append(operator)
    return ans     

def findCol(col, colnames):
    ind = -1
    cnt = 0
    for i in range(len(colnames)):
        if colnames[i].upper() == col or colnames[i].endswith("."+col):
            ind = i
            cnt += 1
    if cnt != 1:
        return -1
    return ind

def whereCondition(table,conditions,colnames):
    try:
        connectors = []
        t1 = conditions.lower().split(" ")
        for conn in t1:
            if conn == "and" or conn == "or":
                connectors.append(conn)
        #print connectors
        pattern = "AND|OR"
        operations = re.split(pattern,conditions.upper())
        #print operations
        res = []
        x = 0
        while x < len(operations):
            operations[x] = operations[x].replace(" ","")
            operands = seperateOperands(operations[x])     #Convert to Postfix type O1 O2 Op
            l = findCol(operands[0],colnames)    #Finding the Column Indexes respective to ColumnNames
            r = findCol(operands[1],colnames)    
            if r > -1 and l > -1:
                operands[0]=operands[0].replace(operands[0],"table[x]["+str(l)+"]")
                operands[1]=operands[1].replace(operands[1],"table[x]["+str(r)+"]")
                #print operands
            elif l > -1:
                operands[0]=operands[0].replace(operands[0],"table[x]["+str(l)+"]")
                #print operands
            else:
                print "Error in WHERE Condition!"
                sys.exit(1)
            temp = operands[0],operands[1]
            operations[x]=operands[2].join(temp)        #Back to Infix O1 Op O2
            x += 1
        conditions1 = operations[0] + " "
        i = 0
        for x in range(1,len(operations)):
            conditions1 += connectors[i].lower() + " "
            conditions1 += operations[x] + " "
            i += 1
        for x in range(len(table)):
            if eval(conditions1):
                res.append(table[x])
        return res
    except:
        print "Error in WHERE Condition!"
        sys.exit(1)    
        return []

def checkCommonCols(conditions,colnames):
    global commCols
    try:
        pattern = "AND|OR"
        operations = re.split(pattern,conditions.upper())
        x = 0
        while x < len(operations):
            operations[x] = operations[x].replace(" ","")
            operands = seperateOperands(operations[x])
            if "." in operands[0] and "." in operands[1]:
                L = operands[0].index(".")
                R = operands[0].index(".")
                if operands[0][L:] == operands[1][R:] and operands[2] == '==':
                    p = findCol(operands[0],colnames), findCol(operands[1],colnames)
                    commCols.append(p)
            x += 1
    except Exception:
        print "Error in WHERE Condition!"
        sys.exit(1)
        
def projectCols(cols,tables,headers):
    if len(headers) == 0:
        print "Error in Column Projections!"
        sys.exit(1)
    columns = []
    if cols == "*":
        columns = headers
    else:
        cols = cols.replace(" ","")
        columns = cols.split(",")
    res = []
    for col1 in columns:
        try:
            res.append(headers.index(col1))
        except ValueError:
            #Colunm Name is not in form of <table>.<colname>
            ind = -1
            flag = 0
            for i in range(len(headers)):
                if headers[i].endswith("."+col1):
                    ind = i
                    flag += 1
            if flag == 1:
                res.append(ind)
            else:
                print "Error in Column Projection"
                sys.exit(1)
    #Now Removing the Common Columns
    if len(commCols) > 0:
        for tup in commCols:
            if tup[0] in res and tup[1] in res:
                i1 = res.index(tup[0])
                i2 = res.index(tup[1])
                if i1 < i2:
                    del res[i2]
                else:
                    del res[i1]
    return res
                
def evalAggregate(columns,table,tables,headers):
    res = ""
    col = columns.lower().split(",")
    ans = 0
    for c in col:
        c = c.replace(" ","")
        if c.startswith("max"):
            ind = projectCols(c[4:-1].upper(),tables,headers)
            #print ind
            ans = metadata.MAXFunc(ind[0],table)
        elif c.startswith("min"):
            ind = projectCols(c[4:-1].upper(),tables,headers)
            #print ind
            ans = metadata.MINFunc(ind[0],table)
        elif c.startswith("sum"):
            ind = projectCols(c[4:-1].upper(),tables,headers)
            #print ind
            ans = metadata.SUMFunc(ind[0],table)
        elif c.startswith("avg"):
            ind = projectCols(c[4:-1].upper(),tables,headers)
            #print ind
            ans = metadata.AVGFunc(ind[0],table)
        elif c.startswith("count"):
            ind = projectCols(c[6:-1].upper(),tables,headers)
            #print ind
            ans = metadata.COUNTFunc(ind[0],table)
        res += (str(ans) + ",")
    res = res[:-1]
    return res

#-----------------------------MAIN CODE---------------------------------------

query = sys.argv[1]
if query[-1] == ";":
    query = query[:-1]
parsed = sqlparse.parse(query)
stmt = parsed[0]
tokens = map(str,stmt.tokens)
notokens = len(stmt.tokens)
cols = ""
tables = ""
dist = False
whereclause = False
aggfuncs = 0
#print tokens        #=>Debugging Purposes
i = 0
while i < len(tokens)-1:
    if tokens[i].lower() == "select":
        if tokens[i+2].lower() == "distinct":
            dist = True
            i = i+2
        i = i+2
        cols = tokens[i]
        i = i+2     #Jumps to the FROM clause
    elif tokens[i].lower() == "from":
        i = i+2
        tables = tokens[i]
        i = i+2     #Jump Ahead to WHERE PART
    elif tokens[i] == " ":
        i = i+1
    elif tokens[i].lower() == "where":
        break
    else:
        error = 1
        print "Syntax Error. Wrong Query!"
        sys.exit(1)
#print "Columns: ",cols     #=>Debugging Purposes
#print "Tables: ",tables    #=>Debugging Purposes
conditions = tokens[-1]
cols = cols.replace(" ","")
tables = tables.replace(" ","")
if conditions.lower().startswith("where"):
    whereclause = True
    conditions = conditions[6:]           #WHERE + 1 SPACE
#print "Conditions: ",conditions    #=>Debugging Purposes
header = ""
output = ""
t = []
try:
    t = metadata.getTables(tables)
    #print t
except IOError :
    print "Error Occurred. Table Not Found!"
    sys.exit(1)
headers = createHeaders(tables)
#print headers            #=>Not Needed
aggfuncs = metadata.checkAggregrateFunctions(cols)
if aggfuncs == -1:
    print "Syntax Error: Aggregate Functions"
    sys.exit(1)
if whereclause:
    t = whereCondition(t, conditions, headers)
    #print t
    if aggfuncs > 0:
        header = cols
    checkCommonCols(conditions,headers)        #Check Same Columns in tables
    #print commCols          #Contains the Columns that are the same!
if aggfuncs == 0:
    pColumns = projectCols(cols,tables,headers)
    #print pColumns
    header = metadata.generateHeader(pColumns,headers)
    #print header
    output = metadata.getOutputString(pColumns,t)
    #print output
else:
    try:
        header = cols
        if len(header) > 0:
            output += evalAggregate(cols,t,tables,headers)
        else:
            output = "NULL"
    except IndexError:
        print "Syntax Error: Unidentified Column in Aggregate Functions!"
        sys.exit(1)
if dist:
    output = metadata.distinctRows(output)
if output == "":
    print "No Rows!"
else:
    print header
    print output
