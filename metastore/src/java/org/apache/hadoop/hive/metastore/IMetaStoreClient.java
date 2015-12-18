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

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.DbPriv;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.IndexItem;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TblPriv;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.tdw_sys_fields_statistics;
import org.apache.hadoop.hive.metastore.api.tdw_sys_table_statistics;
import org.apache.hadoop.hive.metastore.api.User;

import org.apache.thrift.TException;

/**
 * TODO Unnecessary when the server sides for both dbstore and filestore are merged
 */
public interface IMetaStoreClient {

  public void close();

  public List<String> getTables(String dbName, String tablePattern) throws MetaException, UnknownTableException, TException,
      UnknownDBException;

  /**
   * Drop the table.
   * @param tableName The table to drop
   * @param deleteData Should we delete the underlying data
   * @throws MetaException Could not drop table properly.
   * @throws UnknownTableException The table wasn't found.
   * @throws TException A thrift communication error occurred
   * @throws NoSuchObjectException The table wasn't found.
   */
  public void dropTable(String tableName, boolean deleteData) 
    throws MetaException, UnknownTableException, TException, NoSuchObjectException;

  /**
   * Drop the table.
   * @param dbname The database for this table
   * @param tableName The table to drop
   * @throws MetaException Could not drop table properly.
   * @throws NoSuchObjectException The table wasn't found.
   * @throws TException A thrift communication error occurred
   * @throws ExistingDependentsException
   */
  public void dropTable(String dbname, String tableName, boolean deleteData, 
      boolean ignoreUknownTab) throws  
      MetaException, TException, NoSuchObjectException;

  //public void createTable(String tableName, Properties schema) throws MetaException, UnknownTableException,
    //  TException;

  public boolean tableExists(String tableName) throws MetaException, TException, UnknownDBException;

  /**
   * Get a table object. 
   * @param tableName Name of the table to fetch.
   * @return An object representing the table.
   * @throws MetaException Could not fetch the table
   * @throws TException A thrift communication error occurred 
   * @throws NoSuchObjectException In case the table wasn't found.
   */
  public Table getTable(String tableName) throws MetaException, 
    TException, NoSuchObjectException;
  
  /**
   * Get a table object. 
   * @param dbName The database the table is located in.
   * @param tableName Name of the table to fetch.
   * @return An object representing the table.
   * @throws MetaException Could not fetch the table
   * @throws TException A thrift communication error occurred 
   * @throws NoSuchObjectException In case the table wasn't found.
   */
  public Table getTable(String dbName, String tableName) 
    throws MetaException, TException, NoSuchObjectException;

//  /**
//   * @param tableName
//   * @param dbName
//   * @param partVals
//   * @return the partition object
//   * @throws InvalidObjectException
//   * @throws AlreadyExistsException
//   * @throws MetaException
//   * @throws TException
//   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#append_partition(java.lang.String, java.lang.String, java.util.List)
//   */
//  public Partition appendPartition(String tableName, String dbName, List<String> partVals)
//      throws InvalidObjectException, AlreadyExistsException, MetaException, TException;
  
//  /**
//   * Add a partition to the table.
//   * @param partition The partition to add
//   * @return The partition added
//   * @throws InvalidObjectException Could not find table to add to
//   * @throws AlreadyExistsException Partition already exists
//   * @throws MetaException Could not add partition
//   * @throws TException Thrift exception
//   */
//  public Partition add_partition(Partition partition) 
//    throws InvalidObjectException, AlreadyExistsException, 
//      MetaException, TException;

  /**
   * @param tblName the table name
   * @param dbName the database name
   * @return the primary partition definition object
   * @throws MetaException
   * @throws TException
   */
  public Partition getPriPartition(String dbName, String tblName)
    throws MetaException, TException;
  
  /**
   * Set the primary partition
   * @param tblName
   * @param dbName
   * @param priPart
   * @throws MetaException
   * @throws TException
   */
  public void setPriPartition(String dbName, String tblName, Partition priPart)
    throws MetaException, TException, InvalidOperationException;
  
  /**
   * @param tblName the table name
   * @param dbName the database name
   * @return the sub partition definition object
   * @throws MetaException
   * @throws TException
   */
  public Partition getSubPartition(String dbName, String tblName)
    throws MetaException, TException;
  
  /**
   * Set the sub partition
   * @param tblName
   * @param dbName
   * @param sbuPart
   * @throws MetaException
   * @throws TException
   */
  public void setSubPartition(String dbName, String tblName, Partition subPart)
    throws MetaException, TException, InvalidOperationException;
  
  /**
   * Get the primary & sub partition
   * @param tblName
   * @param dbName
   * @return the primary & sub partition
   * @throws MetaException
   * @throws TException
   */
  public List<Partition> getPartitions(String dbName, String tblName)
    throws MetaException, TException, NoSuchObjectException;
  
  /**
   * Set the primary & sub partition
   * @param tblName
   * @param dbName
   * @param partitions
   * @throws MetaException
   * @throws TException
   */
  public void setParititions(String dbName, String tblName, List<Partition> partitions)
    throws MetaException, TException, InvalidOperationException;
  
//  /**
//   * @param tblName
//   * @param dbName
//   * @param partVals
//   * @return the partition object
//   * @throws MetaException
//   * @throws TException
//   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_partition(java.lang.String, java.lang.String, java.util.List)
//   */
//  public Partition getPartition(String tblName, String dbName, List<String> partVals)
//      throws MetaException, TException ;
//  
//  /**
//   * @param tbl_name
//   * @param db_name
//   * @param max_parts
//   * @return the list of partitions
//   * @throws NoSuchObjectException
//   * @throws MetaException
//   * @throws TException
//   */
//  public List<Partition> listPartitions(String db_name, String tbl_name, short max_parts)
//      throws NoSuchObjectException, MetaException, TException;
//
//  public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts)
//    throws  MetaException, TException;
  /**
   * @param tbl
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_table(org.apache.hadoop.hive.metastore.api.Table)
   */
  public void createTable(Table tbl) throws AlreadyExistsException, InvalidObjectException,
      MetaException, NoSuchObjectException, TException ;

  public void alter_table(String defaultDatabaseName, String tblName, Table table) throws InvalidOperationException, MetaException, TException;
  public boolean createDatabase(String name, String location_uri) throws AlreadyExistsException, MetaException, TException;
  public boolean dropDatabase(String name) throws MetaException, TException;

//  /**
//   * @param db_name
//   * @param tbl_name
//   * @param part_vals
//   * @param deleteData delete the underlying data or just delete the table in metadata
//   * @return true or false
//   * @throws NoSuchObjectException
//   * @throws MetaException
//   * @throws TException
//   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_partition(java.lang.String, java.lang.String, java.util.List, boolean)
//   */
//  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData)
//      throws NoSuchObjectException, MetaException, TException;

//  /**
//   * updates a partition to new partition
//   * @param dbName database of the old partition
//   * @param tblName table name of the old partition
//   * @param newPart new partition
//   * @throws InvalidOperationException if the old partition does not exist
//   * @throws MetaException if error in updating metadata
//   * @throws TException if error in communicating with metastore server
//   */
//  public void alter_partition(String dbName, String tblName,
//      Partition newPart) throws InvalidOperationException, MetaException,
//      TException;
  
  /**
   * @param db
   * @param tableName
   * @throws UnknownTableException
   * @throws UnknownDBException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_fields(java.lang.String, java.lang.String)
   */
  public List<FieldSchema> getFields(String db, String tableName) 
      throws MetaException, TException, UnknownTableException, UnknownDBException;
  /**
   * @param db
   * @param tableName
   * @throws UnknownTableException
   * @throws UnknownDBException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_schema(java.lang.String, java.lang.String)
   */
  public List<FieldSchema> getSchema(String db, String tableName) 
      throws MetaException, TException, UnknownTableException, UnknownDBException;
  
  // joeyli added for statics information collection begin
  
  public tdw_sys_table_statistics add_table_statistics(tdw_sys_table_statistics new_table_statistics) throws InvalidObjectException, AlreadyExistsException, MetaException, TException;

  public boolean delete_table_statistics(String table_statistics_name) throws NoSuchObjectException, MetaException, TException;

  public tdw_sys_table_statistics get_table_statistics(String table_statistics_name) throws MetaException, TException;

  public List<tdw_sys_table_statistics> get_table_statistics_multi(int max_parts) throws NoSuchObjectException, MetaException, TException;

  public List<String> get_table_statistics_names(int max_parts) throws NoSuchObjectException, MetaException, TException;

  public tdw_sys_fields_statistics add_fields_statistics(tdw_sys_fields_statistics new_fields_statistics) throws InvalidObjectException, AlreadyExistsException, MetaException, TException;

  public boolean delete_fields_statistics(String table_statistics_name,String fields_name) throws NoSuchObjectException, MetaException, TException;

  public tdw_sys_fields_statistics get_fields_statistics(String table_statistics_name,String fields_name) throws MetaException, TException;

  public List<tdw_sys_fields_statistics> get_fields_statistics_multi(String table_statistics_name,int max_parts) throws NoSuchObjectException, MetaException, TException;

  public List<String> get_fields_statistics_names(String table_statistics_name,int max_parts) throws NoSuchObjectException, MetaException, TException;
  
  // joeyli added for statics information collection end

  
//added by BrantZhang for authorization begin
  
  public boolean create_user(String byWho, String newUser, String passwd) throws AlreadyExistsException, TException, MetaException;

  public boolean drop_user(String byWho, String userName) throws NoSuchObjectException, TException, MetaException;

  public User get_user(String byWho, String userName) throws NoSuchObjectException, TException, MetaException;

  public List<String> get_users_all(String byWho) throws TException, MetaException;

  public boolean set_passwd(String byWho, String forWho, String newPasswd) throws NoSuchObjectException, TException, MetaException;

  public boolean is_a_user(String userName, String passwd) throws TException, MetaException;

  public boolean is_a_role(String roleName) throws TException, MetaException;

  public boolean create_role(String byWho, String roleName) throws AlreadyExistsException, TException, MetaException;

  public boolean drop_role(String byWho, String roleName) throws NoSuchObjectException, TException, MetaException;

  public Role get_role(String byWho, String roleName) throws NoSuchObjectException, TException, MetaException;

  public List<String> get_roles_all(String byWho) throws TException, MetaException;

  public boolean grant_auth_sys(String byWho, String userName, List<String> privileges) throws NoSuchObjectException, TException, InvalidObjectException, MetaException;

  public boolean grant_auth_role_sys(String byWho, String roleName, List<String> privileges) throws NoSuchObjectException, TException, InvalidObjectException, MetaException;

  public boolean grant_role_to_user(String byWho, String userName, List<String> roleNames) throws NoSuchObjectException, TException, InvalidObjectException, MetaException;

  public boolean grant_role_to_role(String byWho, String roleName, List<String> roleNames) throws NoSuchObjectException, TException, InvalidObjectException, MetaException;

  public boolean grant_auth_on_db(String byWho, String forWho, List<String> privileges, String db) throws NoSuchObjectException, TException, MetaException, InvalidObjectException;

  public boolean grant_auth_on_tbl(String byWho, String forWho, List<String> privileges, String db, String tbl) throws NoSuchObjectException, TException, InvalidObjectException, MetaException;

  public DbPriv get_auth_on_db(String byWho, String who, String db) throws NoSuchObjectException, TException, MetaException;

  public List<DbPriv> get_db_auth(String byWho, String db) throws NoSuchObjectException, TException, MetaException;

  public List<DbPriv> get_db_auth_all(String byWho) throws NoSuchObjectException, TException, MetaException;

  public TblPriv get_auth_on_tbl(String byWho, String who, String db, String tbl) throws NoSuchObjectException, TException, MetaException;

  public List<TblPriv> get_tbl_auth(String byWho, String db, String tbl) throws NoSuchObjectException, TException, MetaException;

  public List<TblPriv> get_tbl_auth_all(String byWho) throws NoSuchObjectException, TException, MetaException;

  public boolean revoke_auth_sys(String byWho, String userName, List<String> privileges) throws NoSuchObjectException, TException, InvalidObjectException, MetaException;

  public boolean revoke_auth_role_sys(String byWho, String roleName, List<String> privileges) throws NoSuchObjectException, TException, InvalidObjectException, MetaException;

  public boolean revoke_role_from_user(String byWho, String userName, List<String> roleNames) throws NoSuchObjectException, TException, InvalidObjectException, MetaException;

  public boolean revoke_role_from_role(String byWho, String roleName, List<String> roleNames) throws NoSuchObjectException, TException, InvalidObjectException, MetaException;

  public boolean revoke_auth_on_db(String byWho, String who, List<String> privileges, String db) throws NoSuchObjectException, TException, InvalidObjectException, MetaException;

  public boolean revoke_auth_on_tbl(String byWho, String who, List<String> privileges, String db, String tbl) throws NoSuchObjectException, TException, InvalidObjectException, MetaException;
  
  public boolean drop_auth_on_db(String byWho, String forWho, String db) throws TException, MetaException;
  
  public boolean drop_auth_in_db(String byWho, String forWho) throws TException, MetaException;
  
  public boolean drop_auth_on_tbl(String byWho, String forWho, String db, String tbl) throws TException, MetaException;
  
  public boolean drop_auth_in_tbl(String byWho, String forWho) throws TException, MetaException;
  
//added by BrantZhang for authorization end
  
//add by konten for index begin
  public boolean create_index(IndexItem index)throws MetaException, TException;
  
  public boolean drop_index(String db, String table, String name)throws MetaException, TException;
    
  public int get_index_num(String db, String table)throws MetaException, TException;
  
  public int  get_index_type(String db, String table, String name)throws MetaException, TException;
  
  public String get_index_field(String db, String table, String name)throws MetaException, TException;
  
  public String get_index_location(String db, String table, String name)throws MetaException, TException;
  
  public boolean set_index_location(String db, String table, String name, String location)throws MetaException, TException;
  
  public boolean set_index_status(String db, String table, String name, int status)throws MetaException, TException;
  
  public List<IndexItem>  get_all_index_table(String db, String table)throws MetaException, TException;
  
  public IndexItem get_index_info(String db, String table, String name)throws MetaException, TException;
  
  public List<IndexItem> get_all_index_sys()throws MetaException, TException;
  
  //add by konten for index end
}
