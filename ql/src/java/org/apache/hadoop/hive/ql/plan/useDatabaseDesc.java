package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

public class useDatabaseDesc extends ddlDesc implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String changeToDB;

	public String getChangeToDB() {
		return changeToDB;
	}

	public void setChangeToDB(String changeToDB) {
		this.changeToDB = changeToDB;
	}

	public useDatabaseDesc(String changeToDB) {
		super();
		this.changeToDB = changeToDB;
	}
	

}
