/**
 * 
 */
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

import org.apache.hadoop.fs.Path;

/**
 * @author allisonzhao
 *
 */
public class showRolesDesc extends aclDesc implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String user;
	Path tmpFile;
	private final String schema = "role_name#string";

	public showRolesDesc(String user,Path tmp,String who,String DBconnected) {
		super();
		this.user = user;
		this.tmpFile = tmp;
		this.setWho(who);
		this.setDBconnected(DBconnected);
	}

	public String getSchema() {
		return schema;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public Path getTmpFile() {
		return tmpFile;
	}

	public void setTmpFile(Path tmpFile) {
		this.tmpFile = tmpFile;
	}
	

}
