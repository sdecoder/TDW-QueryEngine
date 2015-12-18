/**
 * 
 */
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

/**
 * @author allisonzhao
 *
 */
public abstract class aclDesc implements Serializable {
	private static final long serialVersionUID = 1L;
	
	String who;
	String DBconnected;
	
	public String getWho() {
		return who;
	}
	public void setWho(String who) {
		this.who = who;
	}
	public String getDBconnected() {
		return DBconnected;
	}
	public void setDBconnected(String dBconnected) {
		DBconnected = dBconnected;
	}
	
}
