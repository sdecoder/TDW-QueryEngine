package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;

public class dropUserDesc extends aclDesc implements Serializable {
	ArrayList<String> userList;

	public dropUserDesc(ArrayList<String> userList,String who,String DBconnected) {
		super();
		this.userList = userList;
		this.setWho(who);
		this.setDBconnected(DBconnected);
	}

	public ArrayList<String> getUserList() {
		return userList;
	}

	public void setUserList(ArrayList<String> userList) {
		this.userList = userList;
	}
	
	
	
}
