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
public class showUsersDesc extends aclDesc implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Path tmpFile;

	private final String schema = "user_name#string";
	public showUsersDesc(Path tmp,String who,String DBconnected) {
		super();
		this.tmpFile = tmp;
		this.setWho(who);
		this.setDBconnected(DBconnected);
		// TODO Auto-generated constructor stub
	}

	public String getSchema() {
		return schema;
	}

	public Path getTmpFile() {
		return tmpFile;
	}

	public void setTmpFile(Path tmpFile) {
		this.tmpFile = tmpFile;
	}

}
