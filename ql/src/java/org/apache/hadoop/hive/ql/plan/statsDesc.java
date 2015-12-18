package org.apache.hadoop.hive.ql.plan;

import java.awt.print.Book;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class statsDesc implements Serializable {

    int distinctThresholdPercent;
    int mcvTableThresholdValue;
    String tableName;
    java.util.ArrayList<String> _fieldNames;
    java.util.ArrayList<Integer> _fieldStatsSwitch;
    
    private java.util.ArrayList<org.apache.hadoop.hive.ql.plan.exprNodeDesc> colList;

    boolean selStar;

    public statsDesc() {
    }

    public void setSelStar(boolean _para) {
	this.selStar = _para;
    }

    public boolean getSelStar() {
	return selStar;
    }    
   
    public void setFieldNames(ArrayList<String> _para) {
	this._fieldNames = _para;
    }

    public java.util.ArrayList<String> getFieldNames() {
	return this._fieldNames;
    }

    public void setFieldStatsSwitch(ArrayList<Integer> _para) {
	this._fieldStatsSwitch = _para;
    }

    public java.util.ArrayList<Integer> getFieldStatsSwitch() {
	return _fieldStatsSwitch;
    }

    public void setDistinctThresholdPercent(final int _para) {
	this.distinctThresholdPercent = _para;
    }

    public int getDistinctThresholdPercent() {
	distinctThresholdPercent = 40;
	return this.distinctThresholdPercent;
    }

    public void setMcvTableThresholdValue(final int _para) {
	this.mcvTableThresholdValue = _para;
    }

    public int getMcvTableThresholdValue() {
	mcvTableThresholdValue = 256;
	return this.mcvTableThresholdValue;
    }

    public void setTableName(String _para) {
	this.tableName = _para;
    }

    public String getTableName() {
	return this.tableName;
    }

    public java.util.ArrayList<org.apache.hadoop.hive.ql.plan.exprNodeDesc> getColList() {
	return this.colList;
    }

    public void setColList(
	    final java.util.ArrayList<org.apache.hadoop.hive.ql.plan.exprNodeDesc> colList) {
	this.colList = colList;
    }

    // String []strArrRef = jobConf.getStrings(STATISTICS_FIELDS_ATTR);
    /*
     * if (strArrRef == null) { // select * selStar = true; }else{ String
     * statColAttrString = strArrRef[0]; StringTokenizer st = new
     * StringTokenizer(statColAttrString); while (st.hasMoreElements()) { String
     * str = (String) st.nextElement();
     * statFldSwitch.put(str,Integer.valueOf(0)); //denote disable
     * 
     * } }
     */

}
