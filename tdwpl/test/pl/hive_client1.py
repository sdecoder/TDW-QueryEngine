#!/usr/bin/env python

def TDW_PL(tdw, argv=[]):
    sql = '''show tables'''
    res = tdw.execute(sql)
    tdw.printf(sql, res)
    print "Q1 done"

    res = tdw.getschema()
    tdw.printf(sql, res)
    tdw.tdw_raise()

    sql = '''select count(1) from %s''' % ("pokes")
    res = tdw.execute2int(sql)
    tdw.printf(sql, res)
    print "Q2 done"

    sql = '''select count(1) from pokes'''
    res = tdw.execute2str(sql)
    tdw.printf(sql, res)
    print "Q3 done"

    # syntax error here
    aaa = None
    #print aaa + "str"

    tdw.tdw_raise()
