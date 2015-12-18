package org.apache.hadoop.hive.metastore.model;

import java.util.List;

//Implemented By : BrantZhang
//Implementation Date : 2010-04-19
public class MRole {
	
	private String roleName;
	private List<MRole> play_roles;
	
	private boolean select_priv;
	private boolean insert_priv;
	private boolean index_priv;
	private boolean create_priv;
	private boolean drop_priv;
	private boolean delete_priv;
	private boolean alter_priv;
	private boolean update_priv;
	private boolean createview_priv;
	private boolean showview_priv;
	
	private boolean dba_priv;
	
	public MRole() { 
		
	}
	
	public MRole(String roleName){
		this.roleName = roleName;
		this.play_roles = null;
		
		this.select_priv = false;
		this.insert_priv = false;
		this.index_priv = false;
		this.create_priv = false;
		this.drop_priv = false;
		this.delete_priv = false;
		this.alter_priv = false;
		this.update_priv = false;
		this.createview_priv = false;
		this.showview_priv = false;
		
		this.dba_priv = false;
	}
	
	public MRole(String roleName, List<MRole> play_roles, boolean select_priv,
			boolean insert_priv, boolean index_priv, boolean create_priv, boolean drop_priv,
			boolean delete_priv, boolean alter_priv, boolean update_priv, boolean createview_priv,
			boolean showview_priv, boolean dba_priv){
		this.roleName = roleName;
		this.play_roles = play_roles;
		
		this.select_priv = select_priv;
		this.insert_priv = insert_priv;
		this.index_priv = index_priv;
		this.create_priv = create_priv;
		this.drop_priv = drop_priv;
		this.delete_priv = delete_priv;
		this.alter_priv = alter_priv;
		this.update_priv = update_priv;
		this.createview_priv = createview_priv;
		this.showview_priv = showview_priv;
		
		this.dba_priv = dba_priv;
	}
	
	public void setRoleName(String roleName){
		this.roleName = roleName; 
	}
	
	public String getRoleName(){
		return this.roleName;
	}
	
	public void setPlay_roles(List<MRole> play_roles){
		this.play_roles = play_roles;
	}
	
	public List<MRole> getPlay_roles(){
		return this.play_roles;
	}
	
	public void setSelect_priv(boolean select_priv){
		this.select_priv = select_priv;
	}
	
	public boolean getSelect_priv(){
		return this.select_priv;
	}
	
	public void setInsert_priv(boolean insert_priv){
		this.insert_priv = insert_priv;
	}
	
	public boolean getInsert_priv(){
		return this.insert_priv;
	}
	
	public void setIndex_priv(boolean index_priv){
		this.index_priv = index_priv;
	}
	
	public boolean getIndex_priv(){
		return this.index_priv;
	}
	
	public void setCreate_priv(boolean create_priv){
		this.create_priv = create_priv;
	}
	
	public boolean getCreate_priv(){
		return this.create_priv;
	}
	
	public void setDrop_priv(boolean drop_priv){
		this.drop_priv = drop_priv;
	}
	
	public boolean getDrop_priv(){
		return this.drop_priv;
	}
	
	public void setDelete_priv(boolean delete_priv){
		this.delete_priv = delete_priv;
	}
	
	public boolean getDelete_priv(){
		return this.delete_priv;
	}
	
	public void setAlter_priv(boolean alter_priv){
		this.alter_priv = alter_priv;
	}
	
	public boolean getAlter_priv(){
		return this.alter_priv;
	}
	
	public void setUpdate_priv(boolean update_priv){
		this.update_priv = update_priv;
	}
	
	public boolean getUpdate_priv(){
		return this.update_priv;
	}
	
	public void setCreateview_priv(boolean createview_priv){
		this.createview_priv = createview_priv;
	}
	
	public boolean getCreateview_priv(){
		return this.createview_priv;
	}
	
	public void setShowview_priv(boolean showview_priv){
		this.showview_priv = showview_priv;
	}
	
	public boolean getShowview_priv(){
		return this.showview_priv;
	}
	
	public void setDba_priv(boolean dba_priv){
		this.dba_priv = dba_priv;
	}
	
	public boolean getDba_priv(){
		return this.dba_priv;
	}

}
