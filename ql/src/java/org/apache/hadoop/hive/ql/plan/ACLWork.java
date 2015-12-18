/**
 * 
 */
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

/**
 * @author allisonzhao
 *
 */
public class ACLWork implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	createUserDesc createUser;
	createRoleDesc createRole;
	dropRoleDesc dropRole;
	dropUserDesc dropUser;
	grantPrisDesc grantPris;
	grantRoleDesc grantRole;
	setPwdDesc setPwd;
	showGrantsDesc showGrants;
	showUsersDesc showUsers;
	revokePriDesc revokePri;
	revokeRoleDesc revokeRole;
	showRolesDesc showRoles;
	
	public ACLWork() {
		super();
	}
	
	public showRolesDesc getShowRoles() {
		return showRoles;
	}

	public void setShowRoles(showRolesDesc showRoles) {
		this.showRoles = showRoles;
	}

	public ACLWork(showRolesDesc showRoles) {
		super();
		this.showRoles = showRoles;
	}

	public ACLWork(createUserDesc createUser) {
		super();
		this.createUser = createUser;
	}
	public ACLWork(createRoleDesc createRole) {
		super();
		this.createRole = createRole;
	}
	public ACLWork(dropRoleDesc dropRole) {
		super();
		this.dropRole = dropRole;
	}
	public ACLWork(dropUserDesc dropUser) {
		super();
		this.dropUser = dropUser;
	}
	public ACLWork(grantPrisDesc grantPris) {
		super();
		this.grantPris = grantPris;
	}
	public ACLWork(grantRoleDesc grantRole) {
		super();
		this.grantRole = grantRole;
	}
	public ACLWork(setPwdDesc setPwd) {
		super();
		this.setPwd = setPwd;
	}
	public ACLWork(showGrantsDesc showGrants) {
		super();
		this.showGrants = showGrants;
	}
	public ACLWork(showUsersDesc showUsers) {
		super();
		this.showUsers = showUsers;
	}
	public ACLWork(revokePriDesc revokePri) {
		super();
		this.revokePri = revokePri;
	}
	public ACLWork(revokeRoleDesc revokeRole) {
		super();
		this.revokeRole = revokeRole;
	}
	public createUserDesc getCreateUser() {
		return createUser;
	}
	public void setCreateUser(createUserDesc createUser) {
		this.createUser = createUser;
	}
	public createRoleDesc getCreateRole() {
		return createRole;
	}
	public void setCreateRole(createRoleDesc createRole) {
		this.createRole = createRole;
	}
	public dropRoleDesc getDropRole() {
		return dropRole;
	}
	public void setDropRole(dropRoleDesc dropRole) {
		this.dropRole = dropRole;
	}
	public dropUserDesc getDropUser() {
		return dropUser;
	}
	public void setDropUser(dropUserDesc dropUser) {
		this.dropUser = dropUser;
	}
	public grantPrisDesc getGrantPris() {
		return grantPris;
	}
	public void setGrantPris(grantPrisDesc grantPris) {
		this.grantPris = grantPris;
	}
	public grantRoleDesc getGrantRole() {
		return grantRole;
	}
	public void setGrantRole(grantRoleDesc grantRole) {
		this.grantRole = grantRole;
	}
	public setPwdDesc getSetPwd() {
		return setPwd;
	}
	public void setSetPwd(setPwdDesc setPwd) {
		this.setPwd = setPwd;
	}
	public showGrantsDesc getShowGrants() {
		return showGrants;
	}
	public void setShowGrants(showGrantsDesc showGrants) {
		this.showGrants = showGrants;
	}
	public showUsersDesc getShowUsers() {
		return showUsers;
	}
	public void setShowUsers(showUsersDesc showUsers) {
		this.showUsers = showUsers;
	}
	public revokePriDesc getRevokePri() {
		return revokePri;
	}
	public void setRevokePri(revokePriDesc revokePri) {
		this.revokePri = revokePri;
	}
	public revokeRoleDesc getRevokeRole() {
		return revokeRole;
	}
	public void setRevokeRole(revokeRoleDesc revokeRole) {
		this.revokeRole = revokeRole;
	}
	
	
	
	
	
	
}
