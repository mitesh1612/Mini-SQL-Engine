#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 11:06:57 2018

@author: mitesh
"""
import sys
#Module 1 : Reading the MetaData.
tables = {}
def getMetaData():
    flag = 0
    cols = []
    tablename = ""
    try:
        meta = open("metadata.txt","r")
    except:
        print "ERROR! Couldn't find the Metadata File."
        print "Make Sure its in the same folder as this script!"
        sys.exit(1)
    for l in meta:
        if l.startswith("<begin_table>"):
            cols = []
            flag = 1
            continue
        if l.startswith("<end_table>"):
            tables[tablename] = cols
            continue
        if flag == 1:
            tablename = l[:-2]
            flag = 0
            continue
        cols.append(l[:-2])

#Module 2 : Reading the Table from the CSV File and getting its data.
def getTable(tname):
    tname += ".csv"
    tab = []
    table = open(tname,"r")
    for l in table:
        l = l.replace('"','')
        l = l.replace("'","")
        l = l.replace(" ","")
        l = l[:-2]      #Remove the /r/n from the file.
        colv = l.split(',')
        colvalues = [int(x) for x in colv] #Convert to Integer Data
        tab.append(colvalues)
    return tab

def getTables(tnames):
    names = tnames.split(',')
    if len(names) == 1:
        temp = getTable(names[0])
        return temp
    else:
        t1 = getTable(names[0])
        i = 1
        while i < len(names):
            temp = []
            t2 = getTables(names[i])
            for l in range(len(t2)):
                for m in range(len(t1)):        #Taking Cartesian Products
                    temp.append(t1[m]+t2[l])
            t1 = temp
            i += 1
    return t1

#Module 3 : Aggregate Functions and their Evaluations           
def checkAggregrateFunctions(column):
    columns = column.split(',')
    ans,c = 0,0
    for col in columns:
        col = col.replace(" ","").lower()
        if col.startswith("max") or col.startswith("min") or col.startswith("avg") or col.startswith("sum") or col.startswith("count"):
            ans += 1
        else:
            c += 1
    if (ans > 0 and c == 0) or (ans == 0  and c > 0):
        return ans
    else:
        return -1

def MAXFunc(col, table):
    mx = table[0][col]
    i = 1
    while i < len(table):
        if mx < table[i][col]:
            mx = table[i][col]
        i += 1
    return mx

def MINFunc(col, table):
    mi = table[0][col]
    i = 1
    while i < len(table):
        if mi > table[i][col]:
            mi = table[i][col]
        i += 1
    return mi

def SUMFunc(col, table):
    sum = 0
    for row in table:
        sum += row[col]
    return sum

def AVGFunc(col, table):
    sum = 0
    n = 0
    for row in table:
        sum += row[col]
        n += 1
    ans = sum/n
    return ans

def COUNTFunc(col, table):
    n = 0
    for row in table:
        if row[col]:
            n += 1
    return n

def distinctRows(rows):
    row = rows.split("\n")
    res = []
    for r in row:
        if r not in res:
            res.append(r)
    ans = '\n'.join(res)
    return ans

#Module 4 : For Printing
def generateHeader(cols,headers):
    column = []
    for ind in cols:
        column.append(headers[ind])
    ans = ','.join(column)
    return ans

def getOutputString(cols,table):
    ans = ""
    for row in table:
        for ind in cols:
            ans += (str(row[ind])+",")
        ans = ans[:-1]      #Removing the extra Comma in the End.
        ans += "\n"
    ans = ans[:-1]      #Removing the extra \n in the End.
    return ans
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~READING THE METADATA~~~~~~~~~~~~~~~~~~~~~~~~~        
getMetaData()
