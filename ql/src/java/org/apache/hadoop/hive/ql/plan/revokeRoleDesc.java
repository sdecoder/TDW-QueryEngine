
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author allisonzhao
 *
 */
public class revokeRoleDesc extends aclDesc implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	ArrayList<String> roles;
	ArrayList<String> users;
	public revokeRoleDesc(ArrayList<String> roles, ArrayList<String> users,String who,String DBconnected) {
		super();
		this.roles = roles;
		this.users = users;
		this.setWho(who);
		this.setDBconnected(DBconnected);
	}
	public ArrayList<String> getRoles() {
		return roles;
	}
	public void setRoles(ArrayList<String> roles) {
		this.roles = roles;
	}
	public ArrayList<String> getUsers() {
		return users;
	}
	public void setUsers(ArrayList<String> users) {
		this.users = users;
	}
	

}
