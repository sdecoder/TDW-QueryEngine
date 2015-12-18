/**
 * 
 */
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

/**
 * @author allisonzhao
 *
 */
public class truncatePartitionDesc extends ddlDesc implements Serializable {
	private String dbName;
	private String tblName;
	private String priPartName;
	private String subPartName;
	public truncatePartitionDesc(String dbName, String tblName,
			String priPartName, String subPartNmae) {
		super();
		this.dbName = dbName;
		this.tblName = tblName;
		this.priPartName = priPartName;
		this.subPartName = subPartNmae;
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
	public String getPriPartName() {
		return priPartName;
	}
	public void setPriPartName(String priPartName) {
		this.priPartName = priPartName;
	}
	public String getSubPartName() {
		return subPartName;
	}
	public void setSubPartName(String subPartNmae) {
		this.subPartName = subPartNmae;
	}
	
	

}
