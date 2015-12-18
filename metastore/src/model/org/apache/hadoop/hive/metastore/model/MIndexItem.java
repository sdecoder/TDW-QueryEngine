package org.apache.hadoop.hive.metastore.model;

import java.util.List;

//Implemented By : kontenhong
//Implementation Date : 2010-10-8
public class MIndexItem
{
    private String db;
    private String tbl;
    private String name;
    private String field_list;
    private String location;
    private int type;
    private int status;

    public static int IndexTypePrimary = 0; 
    public static int IndexTypeSecond = 1;
    public static int IndexTypeUnion = 2;
    
    public static int IndexStatusInit = 0; 
    public static int IndexStatusBuilding = 1;
    public static int IndexStatusDone = 2;
    
    public MIndexItem()
    {
    }

    public MIndexItem(String db, String tbl, String name, String fieldList, 
                    String location, int type, int status)
    {
        this.db = db;
        this.tbl = tbl;
        this.name = name;
        this.field_list = fieldList;
        this.location = location;
        this.type = type;        
        this.status = status;
    }

    public void setDb(String db)
    {
        this.db = db;
    }

    public String getDb()
    {
        return this.db;
    }

    public String getTbl()
    {
        return this.tbl;
    }
    
    public void setTbl(String tbl)
    {
        this.tbl = tbl;
    }

    public String getName()
    {
        return this.name;
    }
    
    public void setName(String name)
    {
        this.name = name;
    }
    
    public String getFieldList()
    {
        return this.field_list;
    }

    public void setFieldList(String fieldList)
    {
        this.field_list = fieldList;
    }

    public String getLocation()
    {
        return this.location;
    }

    public void setLocation(String location)
    {
        this.location = location;
    }
    
    public int getType()
    {
        return this.type;
    }

    public void setType(int type)
    {
        this.type = type;
    }

    public int getStatus()
    {
        return this.status;
    }

    public void setStatus(int status)
    {
        this.status = status;
    }
}
