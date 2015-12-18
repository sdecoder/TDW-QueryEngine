package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

public class createDatabaseDesc extends ddlDesc implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String dbname;

	public String getDbname() {
		return dbname;
	}

	public void setDbname(String dbname) {
		this.dbname = dbname;
	}

	public createDatabaseDesc(String dbname) {
		super();
		this.dbname = dbname;
	}
	
	

}
