/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DbPriv;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TblPriv;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.tdw_sys_fields_statistics;
import org.apache.hadoop.hive.metastore.api.tdw_sys_table_statistics;
import org.apache.hadoop.hive.metastore.api.User;

import org.apache.hadoop.hive.metastore.api.IndexItem;

public interface RawStore extends Configurable {

  public abstract void shutdown();

  /**
   * Opens a new one or the one already created
   * Every call of this function must have corresponding commit or rollback function call
   * @return an active transaction
   */

  public abstract boolean openTransaction();

  /**
   * if this is the commit of the first open call then an actual commit is called. 
   * @return true or false
   */
  public abstract boolean commitTransaction();

  /**
   * Rolls back the current transaction if it is active
   */
  public abstract void rollbackTransaction();

  public abstract boolean createDatabase(Database db) throws MetaException;

  public abstract boolean createDatabase(String name) throws MetaException;

  public abstract Database getDatabase(String name) throws NoSuchObjectException;

  public abstract boolean dropDatabase(String dbname);

  public abstract List<String> getDatabases() throws MetaException;

  public abstract boolean createType(Type type);

  public abstract Type getType(String typeName);

  public abstract boolean dropType(String typeName);

  public abstract void createTable(Table tbl) throws InvalidObjectException, MetaException;

  public abstract boolean dropTable(String dbName, String tableName) throws MetaException;

  public abstract Table getTable(String dbName, String tableName) throws MetaException;
  
  // Modified By : guosijie
  // Modified Date : 2010-02-05
  
  // Modification start
 
//  public abstract boolean addPartition(Partition part) throws InvalidObjectException, MetaException;
 
//  public abstract Partition getPartition(String dbName, String tableName, List<String> part_vals)
//      throws MetaException;
//
//  public abstract boolean dropPartition(String dbName, String tableName, List<String> part_vals)
//      throws MetaException;
//
//  public abstract List<Partition> getPartitions(String dbName, String tableName, int max)
//      throws MetaException;
  
  // Note : here just get the partition definition of a specified level
  public abstract Partition getPartition(String dbName, String tableName, int level)
      throws MetaException;
  
  // Note : here just update the partition definition of a specified level
  public abstract void alterPartition(String dbName, String tableName, Partition new_part)
      throws InvalidObjectException, MetaException;
 
  // Modification end

  public abstract void alterTable(String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException;

  public List<String> getTables(String dbName, String pattern) throws MetaException;

  // Modification By : guosijie
  // Modification Date : 2010-03-09
  
  // Modification start
  
//  public abstract List<String> listPartitionNames(String db_name, String tbl_name, short max_parts)
//    throws MetaException;

//  public abstract void alterPartition(String db_name, String tbl_name,
//      Partition new_part) throws InvalidObjectException, MetaException;
  
  // Modification end
  
  // joeyli added for statics information collection begin
  
  public abstract boolean add_table_statistics(tdw_sys_table_statistics new_table_statistics) throws MetaException;

  public abstract boolean delete_table_statistics(String table_statistics_name)  throws MetaException;

  public abstract tdw_sys_table_statistics get_table_statistics(String table_statistics_name) throws MetaException;

  public abstract List<tdw_sys_table_statistics> get_table_statistics_multi(int max) throws MetaException;

  public abstract List<String> get_table_statistics_names(int max) throws MetaException;

  public abstract boolean add_fields_statistics(tdw_sys_fields_statistics new_fields_statistics) throws MetaException;

  public abstract boolean delete_fields_statistics(String table_statistics_name,String fields_statistics_name)  throws MetaException;

  public abstract tdw_sys_fields_statistics get_fields_statistics(String table_statistics_name,String fields_statistics_name) throws MetaException;

  public abstract List<tdw_sys_fields_statistics> get_fields_statistics_multi(String table_statistics_name,int max) throws MetaException;

  public abstract List<String> get_fields_statistics_names(String table_statistics_name,int max) throws MetaException;
  
  // joeyli added for statics information collection end

  
// added by BrantZhang for authorization begin
  
  //for user table
  public abstract boolean       createUser(String user, String passwd) throws AlreadyExistsException;
  
  public abstract boolean       dropUser(String userName) throws NoSuchObjectException;
  
  public abstract User          getUser(String userName) throws NoSuchObjectException;
  
  public abstract List<String>  getUsersAll();
  									  
  public abstract boolean       setPasswd(String userName, String newPasswd) throws NoSuchObjectException;
  									  
  public abstract boolean       isAUser(String userName, String passwd);
  
  public abstract boolean       grantAuthSys(String userName, List<String> privileges) throws NoSuchObjectException, InvalidObjectException;
  
  public abstract boolean       grantRoleToUser(String userName, List<String> roles) throws NoSuchObjectException, InvalidObjectException;
  
  public abstract boolean       revokeAuthSys(String userName, List<String> privileges) throws NoSuchObjectException, InvalidObjectException;
  
  public abstract boolean       revokeRoleFromUser(String userName, List<String> roles) throws NoSuchObjectException, InvalidObjectException;
  
  
  //for role table
  public abstract boolean       isARole(String roleName);
  
  public abstract boolean       createRole(String roleName) throws AlreadyExistsException;
  
  public abstract boolean       dropRole(String roleName) throws NoSuchObjectException;
  
  public abstract boolean       grantRoleToRole(String roleName, List<String> roles) throws NoSuchObjectException, InvalidObjectException;
  
  public abstract boolean       revokeRoleFromRole(String roleName, List<String> roles) throws NoSuchObjectException, InvalidObjectException;
  
  public abstract Role          getRole(String roleName) throws NoSuchObjectException;
  
  public abstract List<String>  getRolesAll();
  
  public abstract boolean       grantAuthRoleSys(String role, List<String> privileges) throws NoSuchObjectException, InvalidObjectException;
  
  public abstract boolean       revokeAuthRoleSys(String role, List<String> privileges) throws NoSuchObjectException, InvalidObjectException;									  
  
  
  //for dbpriv table
  public abstract boolean       grantAuthOnDb(String forWho, List<String> privileges, String db) throws NoSuchObjectException, InvalidObjectException;
  									   
  public abstract DbPriv        getAuthOnDb(String who, String db);
  
  public abstract List<DbPriv>  getDbAuth(String db);
  
  public abstract List<DbPriv>  getAuthOnDbs(String who);
  
  public abstract List<DbPriv>  getDbAuthAll();
  
  public abstract boolean       revokeAuthOnDb(String who, List<String> privileges, String db) throws NoSuchObjectException, InvalidObjectException;
  
  public abstract boolean       dropAuthOnDb(String who, String db);
  
  public abstract boolean       dropAuthInDb(String who);
  
  
  //for tblpriv table
  public abstract boolean       grantAuthOnTbl(String forWho, List<String> privileges, String db, String tbl) throws NoSuchObjectException, InvalidObjectException;
						   
  public abstract TblPriv       getAuthOnTbl(String who, String db, String tbl);
  									   
  public abstract List<TblPriv> getTblAuth(String db, String tbl);
  
  public abstract List<TblPriv> getAuthOnTbls(String who);
  									   
  public abstract List<TblPriv> getTblAuthAll();
 
  public abstract boolean       revokeAuthOnTbl(String who, List<String> privileges, String db, String tbl) throws NoSuchObjectException, InvalidObjectException;
  
  public abstract boolean       dropAuthOnTbl(String who, String db, String tbl);
  
  public abstract boolean       dropAuthInTbl(String who);
 				   
  // added by BrantZhang for authorization end
  
  // add by konten for index begin
  public abstract boolean createIndex(IndexItem index) throws MetaException;
  public abstract boolean dropIndex(String db, String table, String name)throws MetaException;
  public abstract int  getIndexNum(String db, String table)throws MetaException;
  public abstract int  getIndexType(String db, String table, String name)throws MetaException;
  public abstract String getIndexField(String db, String table, String name)throws MetaException;
  public abstract String getIndexLocation(String db, String table, String name)throws MetaException;
  public abstract boolean setIndexLocation(String db, String table, String name, String location)throws MetaException;
  public abstract boolean setIndexStatus(String db, String table, String name, int status)throws MetaException;
  public abstract List<IndexItem>  getAllIndexTable(String db, String table)throws MetaException;
  public abstract IndexItem getIndexInfo(String db, String table, String name)throws MetaException;  
  public abstract List<IndexItem> getAllIndexSys()throws MetaException;
  //add by konten for index end
}
