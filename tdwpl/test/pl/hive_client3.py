#!/usr/bin/env python

#server
import hive_client2
from tdw_sql_lib import *
from tdw import HiveServerException

os = __import__("os")

def TDW_PL(tdw, argv=[]):
    # switch on debug/normal mode
    try:
        sql = '''show tables'''
        res = tdw.execute(sql)
        tdw.printf(sql, res)
    except HiveServerException, hsx:
        print "In HiveServerException ..."

    # call functions in hive_client2.py
    hive_client2.func1()
    hive_client2.func2("Hello, world!")

    print os.getpid()
