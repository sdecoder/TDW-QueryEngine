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
public class dropRoleDesc  extends aclDesc implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	ArrayList<String> roles;

	public dropRoleDesc(ArrayList<String> roles,String who,String DBconnected) {
		super();
		this.roles = roles;
		this.setWho(who);
		this.setDBconnected(DBconnected);
	}

	public ArrayList<String> getRoles() {
		return roles;
	}

	public void setRoles(ArrayList<String> roles) {
		this.roles = roles;
	}
	
}
