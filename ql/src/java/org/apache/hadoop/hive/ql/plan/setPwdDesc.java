/**
 * 
 */
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

/**
 * @author allisonzhao
 *
 */
public class setPwdDesc extends aclDesc implements Serializable {
	String passwd;
	String name;
	public setPwdDesc(String passwd, String name,String who,String DBconnected) {
		super();
		this.passwd = passwd;
		this.name = name;
		this.setWho(who);
		this.setDBconnected(DBconnected);
	}
	public String getPasswd() {
		return passwd;
	}
	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
}
