package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

public class addDefaultPartitionDesc extends ddlDesc implements Serializable{
	private String dbName = null;
	private String tblName = null;
	private boolean isSub = false;
	/**
	 * @param dbName
	 * @param tblName
	 * @param isSub
	 */
	public addDefaultPartitionDesc(String dbName, String tblName, boolean isSub) {
		super();
		this.dbName = dbName;
		this.tblName = tblName;
		this.isSub = isSub;
	}
	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public String getTblName() {
		return tblName;
	}
	public void setTblName(String tblName) {
		this.tblName = tblName;
	}
	public boolean isSub() {
		return isSub;
	}
	public void setSub(boolean isSub) {
		this.isSub = isSub;
	}
	
	
}
