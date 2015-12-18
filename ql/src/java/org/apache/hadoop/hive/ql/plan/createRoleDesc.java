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
public class createRoleDesc extends aclDesc implements Serializable {
	ArrayList<String> roles;
	boolean asDBA;
	public createRoleDesc(ArrayList<String> roles, boolean asDBA,String who,String DBonnected) {
		super();
		this.roles = roles;
		this.asDBA = asDBA;
		this.setWho(who);
		this.setDBconnected(DBconnected);
	}
	public ArrayList<String> getRoles() {
		return roles;
	}
	public void setRoles(ArrayList<String> roles) {
		this.roles = roles;
	}
	public boolean isAsDBA() {
		return asDBA;
	}
	public void setAsDBA(boolean asDBA) {
		this.asDBA = asDBA;
	}
	
}
