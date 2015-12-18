#!/usr/bin/env python

def TDW_PL(tdw, argv=[]):
    # switch on debug/normal mode
    sql = '''show tables'''
    res = tdw.execute(sql)
    tdw.printf(sql, res)

#    tdw.tdw_raise()

    sql = '''select count(1) from pokes'''
    res = tdw.execute2int(sql)
    tdw.printf("%s : " % (sql), res)

    if res == 500:
        notsql = "Good SQL executeion!"
        print notsql
    else:
        print "Error SQL execution!"

