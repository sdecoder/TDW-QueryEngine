package org.apache.hadoop.hive.ql.parse;

//import org.apache.hadoop.hive.ql.plan.partitionDesc;
import java.util.*;

import org.apache.hadoop.hive.metastore.api.FieldSchema;


public class PartitionDesc {
	/*public enum PartitionType{
		RANGE_PATITION,
		LIST_PARTITION,
		HASH_PARTITION//not use
	};*/
	
	private String dbName;
	private String tableName;
	private Boolean isSub;
	private PartitionType partitionType;
	private FieldSchema partColumn;
	private LinkedHashMap< String,List< String > > partitionSpaces;
	private PartitionDesc subPartition;
	PartitionDesc(){
/*		partitionType = null;
		partitionSpaces = null;
		
		subPartition = null;
	*/	
	}
	
	//this ctor is not recommand ,use follow func
	public PartitionDesc(PartitionType pt,FieldSchema pc,LinkedHashMap< String,List< String > > ps,PartitionDesc subpd){
		partColumn = pc;
		partitionType = pt;
		partitionSpaces = ps;
		subPartition = subpd;
		
	}
	
	public PartitionDesc(String dbname,String tblname,PartitionType pt,FieldSchema pc,LinkedHashMap< String,List< String > > ps,PartitionDesc subpd){
		dbName = dbname;
		tableName = tblname;
		partColumn = pc;
		partitionType = pt;
		partitionSpaces = ps;
		subPartition = subpd;
	
	}
	
	public PartitionType getPartitionType(){
		return partitionType;
	}
	public void setPartitionType(PartitionType pt){
		
		partitionType = pt;
	}
	
	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public LinkedHashMap<String, List<String>> getPartitionSpaces() {
		return partitionSpaces;
	}
	public void setPartitionSpaces(
			LinkedHashMap<String, List<String>> partitionSpaces) {
		this.partitionSpaces = partitionSpaces;
	}
	public PartitionDesc getSubPartition(){
		return subPartition;
	}
	public void setSubPartition(PartitionDesc subpd){
		subPartition = subpd;
	}
	public FieldSchema getPartColumn() {
		return partColumn;
	}
	public void setPartColumn(FieldSchema partColumn) {
		this.partColumn = partColumn;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public Boolean getIsSub() {
		return isSub;
	}
	public void setIsSub(Boolean isSub) {
		this.isSub = isSub;
	}

}
