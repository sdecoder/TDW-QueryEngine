#!/usr/bin/env python

def TDW_PL(tdw, argv=[]):
    # switch on debug/normal mode

    sql = '''show tables'''
    res = tdw.execute(sql)
    tdw.printf(sql, res)

    sql = "select count(1) from pokes"
    res = tdw.execute2int(sql)
    tdw.printf(sql, res)

    print "Query 1 is done."

    sql = "select count(1) from pokes"
    res = tdw.execute(sql)
    tdw.printf(sql, res)

    print "Query 2 is done."
    print "All query are done, test raise a exception."

    tdw.tdw_raise()
