/**
 * 
 */
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;


/**
 * @author allisonzhao
 *
 */
public class createUserDesc extends aclDesc implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String userName;
	private String passwd;
	private boolean isDBA = false;
	public createUserDesc(String userName, String passwd, boolean isDBA,String who,String dBconnected) {
		super();
		this.userName = userName;
		this.passwd = passwd;
		this.isDBA = isDBA;
		this.setWho(who);
		this.setDBconnected(DBconnected);
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPasswd() {
		return passwd;
	}
	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}
	public boolean isDBA() {
		return isDBA;
	}
	public void setDBA(boolean isDBA) {
		this.isDBA = isDBA;
	}
	
}
