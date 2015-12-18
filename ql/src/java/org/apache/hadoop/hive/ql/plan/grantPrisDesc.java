/**
 * 
 */
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author allisonzhao
 *
 */
public class grantPrisDesc extends aclDesc implements Serializable {
	String user;
	ArrayList<String> privileges;
	String db;
	String table;
	public grantPrisDesc(String user, ArrayList<String> privileges, String db,
			String table,String who,String DBconnected) {
		super();
		this.user = user;
		this.privileges = privileges;
		this.db = db;
		this.table = table;
		this.setWho(who);
		this.setDBconnected(DBconnected);
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public ArrayList<String> getPrivileges() {
		return privileges;
	}
	public void setPrivileges(ArrayList<String> privileges) {
		this.privileges = privileges;
	}
	public String getDb() {
		return db;
	}
	public void setDb(String db) {
		this.db = db;
	}
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	

}
