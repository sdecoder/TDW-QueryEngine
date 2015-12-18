package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.List;

public class indexInfoDesc
{
    public String name = "";
    public List<String> fieldList = new ArrayList<String>();
    public int indexType = 1; // 0, primary index; 1, second index; 2, union index;    
     
}
