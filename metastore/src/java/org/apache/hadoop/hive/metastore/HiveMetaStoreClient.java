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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
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
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.tdw_sys_fields_statistics;
import org.apache.hadoop.hive.metastore.api.tdw_sys_table_statistics;
import org.apache.hadoop.hive.metastore.api.User;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Hive Metastore Client.
 */
public class HiveMetaStoreClient implements IMetaStoreClient {
  ThriftHiveMetastore.Iface client = null;
  private TTransport transport = null;
  private boolean open = false;
  private URI metastoreUris[];
  private boolean standAloneClient = false;

  // for thrift connects
  private int retries = 5;

  static final private Log LOG = LogFactory.getLog("hive.metastore");


  public HiveMetaStoreClient(HiveConf conf) throws MetaException {
    if(conf == null) {
      conf = new HiveConf(HiveMetaStoreClient.class);
    }
    
    boolean localMetaStore = conf.getBoolean("hive.metastore.local", false);
    if(localMetaStore) {
      // instantiate the metastore server handler directly instead of connecting through the network
      client = new HiveMetaStore.HMSHandler("hive client", conf);
      this.open = true;
      return;
    }
    
    // get the number retries
    retries = conf.getInt("hive.metastore.connect.retries", 5);

    // user wants file store based configuration
    if(conf.getVar(HiveConf.ConfVars.METASTOREURIS) != null) {
      String metastoreUrisString []= conf.getVar(HiveConf.ConfVars.METASTOREURIS).split(",");
      this.metastoreUris = new URI[metastoreUrisString.length];
      try {
        int i = 0;
        for(String s: metastoreUrisString) {
          URI tmpUri = new URI(s);
          if(tmpUri.getScheme() == null) {
            throw new IllegalArgumentException("URI: "+s+" does not have a scheme");
          }
          this.metastoreUris[i++]= tmpUri;

        }
      } catch (IllegalArgumentException e) {
        throw (e);
      } catch(Exception e) {
        MetaStoreUtils.logAndThrowMetaException(e);
      }
    } else if(conf.getVar(HiveConf.ConfVars.METASTOREDIRECTORY) != null) {
      this.metastoreUris = new URI[1];
      try {
        this.metastoreUris[0] = new URI(conf.getVar(HiveConf.ConfVars.METASTOREDIRECTORY));
      } catch(URISyntaxException e) {
        MetaStoreUtils.logAndThrowMetaException(e);
      }
    } else {
      LOG.error("NOT getting uris from conf");
      throw new MetaException("MetaStoreURIs not found in conf file");
    }
    // finally open the store
    this.open();
  }
  
  /**
   * @param dbname
   * @param tbl_name
   * @param new_tbl
   * @throws InvalidOperationException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#alter_table(java.lang.String, java.lang.String, org.apache.hadoop.hive.metastore.api.Table)
   */
  public void alter_table(String dbname, String tbl_name, Table new_tbl)
      throws InvalidOperationException, MetaException, TException {
    client.alter_table(dbname, tbl_name, new_tbl);
  }

  private void open() throws MetaException {
    for(URI store: this.metastoreUris) {
      LOG.info("Trying to connect to metastore with URI " + store);
      try {
        openStore(store);
      } catch (MetaException e) {
        LOG.warn(e.getStackTrace());
        LOG.warn("Unable to connect metastore with URI " + store);
      }
      if (open) {
        break;
      }
    }
    if(!open) {
      throw new MetaException("Could not connect to meta store using any of the URIs provided");
    }
    LOG.info("Connected to metastore.");
  }
 
  private void openStore(URI store) throws MetaException {
    open = false;
    transport = new TSocket(store.getHost(), store.getPort());
    ((TSocket)transport).setTimeout(20000);
    TProtocol protocol = new TBinaryProtocol(transport);
    client = new ThriftHiveMetastore.Client(protocol);

    for(int i = 0; i < retries && !this.open; ++i) {
      try {
        transport.open();
        open = true;
      } catch(TTransportException e) {
        LOG.warn("failed to connect to MetaStore, re-trying...");
        try {
          Thread.sleep(1000);
        } catch(InterruptedException ignore) { }
      }
    }
    if(!open) {
      throw new MetaException("could not connect to meta store");
    }
  }
  
  public void close() {
    open = false;
    if((transport != null) && transport.isOpen()) {
      transport.close();
    }
    if(standAloneClient) {
      try {
        client.shutdown();
      } catch (TException e) {
        //TODO:pc cleanup the exceptions
        LOG.error("Unable to shutdown local metastore client");
        LOG.error(e.getStackTrace());
        //throw new RuntimeException(e.getMessage());
      }
    }
  }

  public void dropTable(String tableName, boolean deleteData) throws MetaException, NoSuchObjectException {
    // assume that it is default database
    try {
      this.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName, deleteData, false);
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
  }
  
//  /**
//   * @param new_part
//   * @return the added partition
//   * @throws InvalidObjectException
//   * @throws AlreadyExistsException
//   * @throws MetaException
//   * @throws TException
//   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#add_partition(org.apache.hadoop.hive.metastore.api.Partition)
//   */
//  public Partition add_partition(Partition new_part) throws InvalidObjectException,
//      AlreadyExistsException, MetaException, TException {
//    return client.add_partition(new_part);
//  }

//  /**
//   * @param table_name
//   * @param db_name
//   * @param part_vals
//   * @return the appended partition
//   * @throws InvalidObjectException
//   * @throws AlreadyExistsException
//   * @throws MetaException
//   * @throws TException
//   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#append_partition(java.lang.String, java.lang.String, java.util.List)
//   */
//  public Partition appendPartition(String db_name, String table_name, List<String> part_vals)
//      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
//    return client.append_partition(db_name, table_name, part_vals);
//  }

  /**
   * @param name
   * @param location_uri
   * @return true or false
   * @throws AlreadyExistsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_database(java.lang.String, java.lang.String)
   */
  public boolean createDatabase(String name, String location_uri) throws AlreadyExistsException,
      MetaException, TException {
    return client.create_database(name, location_uri);
  }

  /**
   * @param tbl
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_table(org.apache.hadoop.hive.metastore.api.Table)
   */
  public void createTable(Table tbl) throws AlreadyExistsException, InvalidObjectException,
  MetaException, NoSuchObjectException, TException {
    client.create_table(tbl);
  }

  /**
   * @param type
   * @return true or false
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_type(org.apache.hadoop.hive.metastore.api.Type)
   */
  public boolean createType(Type type) throws AlreadyExistsException, InvalidObjectException,
      MetaException, TException {
    return client.create_type(type);
  }

  /**
   * @param name
   * @return true or false
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_database(java.lang.String)
   */
  public boolean dropDatabase(String name) throws MetaException, TException {
    return client.drop_database(name);
  }

//  /**
//   * @param tbl_name
//   * @param db_name
//   * @param part_vals
//   * @return true or false
//   * @throws NoSuchObjectException
//   * @throws MetaException
//   * @throws TException
//   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_partition(java.lang.String, java.lang.String, java.util.List, boolean)
//   */
//  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals)
//      throws NoSuchObjectException, MetaException, TException {
//        return dropPartition(db_name, tbl_name, part_vals, true);
//      }

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
//      throws NoSuchObjectException, MetaException, TException {
//    return client.drop_partition(db_name, tbl_name, part_vals, deleteData);
//  }
  
  /**
   * @param name
   * @param dbname
   * @throws NoSuchObjectException
   * @throws ExistingDependentsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_table(java.lang.String, java.lang.String, boolean)
   */
  public void dropTable(String dbname, String name) throws NoSuchObjectException,
      MetaException, TException {
        dropTable(dbname, name, true, true);
      }

  /**
   * @param dbname
   * @param name
   * @param deleteData delete the underlying data or just delete the table in metadata
   * @throws NoSuchObjectException
   * @throws ExistingDependentsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_table(java.lang.String, java.lang.String, boolean)
   */
  public void dropTable(String dbname, String name, boolean deleteData, boolean ignoreUknownTab) throws 
      MetaException, TException, NoSuchObjectException {
    try {
      client.drop_table(dbname, name, deleteData);
    } catch (NoSuchObjectException e) {
      if(!ignoreUknownTab) {
        throw e;
      }
    }
  }

  /**
   * @param type
   * @return true if the type is dropped
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_type(java.lang.String)
   */
  public boolean dropType(String type) throws MetaException, TException {
    return client.drop_type(type);
  }

  /**
   * @param name
   * @return map of types
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_type_all(java.lang.String)
   */
  public Map<String, Type> getTypeAll(String name) throws MetaException, TException {
    return client.get_type_all(name);
  }

  /**
   * @return the list of databases
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_databases()
   */
  public List<String> getDatabases() throws MetaException, TException {
    return client.get_databases();
  }

//  /**
//   * @param tbl_name
//   * @param db_name
//   * @param max_parts
//   * @return list of partitions
//   * @throws NoSuchObjectException
//   * @throws MetaException
//   * @throws TException
//   */
//  public List<Partition> listPartitions(String db_name, String tbl_name, short max_parts)
//      throws NoSuchObjectException, MetaException, TException {
//    return client.get_partitions(db_name, tbl_name, max_parts);
//  }

  /**
   * @param name
   * @return the database
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_database(java.lang.String)
   */
  public Database getDatabase(String name) throws NoSuchObjectException, MetaException,
      TException {
    return client.get_database(name);
  }

//  /**
//   * @param tbl_name
//   * @param db_name
//   * @param part_vals
//   * @return the partition
//   * @throws MetaException
//   * @throws TException
//   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_partition(java.lang.String, java.lang.String, java.util.List)
//   */
//  public Partition getPartition(String db_name, String tbl_name, List<String> part_vals)
//      throws MetaException, TException {
//    return client.get_partition(db_name, tbl_name, part_vals);
//  }
  
  /**
   * @param name
   * @param dbname
   * @return the table
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @throws NoSuchObjectException 
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_table(java.lang.String, java.lang.String)
   */
  public Table getTable(String dbname, String name) throws MetaException, TException, NoSuchObjectException {
    return client.get_table(dbname, name);
  }

  /**
   * @param name
   * @return the type
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_type(java.lang.String)
   */
  public Type getType(String name) throws MetaException, TException {
    return client.get_type(name);
  }
  
  public List<String> getTables(String dbname, String tablePattern) throws MetaException {
    try {
      return client.get_tables(dbname, tablePattern);
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null; 
  }
  
  public List<String> getTables(String tablePattern) throws MetaException {
    String dbname = MetaStoreUtils.DEFAULT_DATABASE_NAME;
    return this.getTables(dbname, tablePattern); 
  }

  public boolean tableExists(String tableName) throws MetaException, TException,
  UnknownDBException {
    try {
      client.get_table(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
    } catch (NoSuchObjectException e) {
      return false;
    }
    return true;
  }

  public Table getTable(String tableName) throws MetaException, TException, NoSuchObjectException {
    return getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
  }

//  public List<String> listPartitionNames(String dbName, String tblName, short max)
//      throws MetaException, TException {
//    return client.get_partition_names(dbName, tblName, max);
//  }
//
//  public void alter_partition(String dbName, String tblName,
//      Partition newPart) throws InvalidOperationException, MetaException,
//      TException {
//    client.alter_partition(dbName, tblName, newPart);
//  }
  
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
      throws MetaException, TException, UnknownTableException, UnknownDBException {
    return client.get_fields(db, tableName);
  }

  public List<Partition> getPartitions(String dbName, String tblName)
      throws MetaException, TException, NoSuchObjectException {
    return client.get_partitions(dbName, tblName);
  }

  @Override
  public Partition getPriPartition(String dbName, String tblName)
      throws MetaException, TException {
    return client.get_partition(dbName, tblName, 0);
  }

  @Override
  public Partition getSubPartition(String dbName, String tblName)
      throws MetaException, TException {
    return client.get_partition(dbName, tblName, 1);
  }

  @Override
  public void setParititions(String dbName, String tblName,
      List<Partition> partitions) throws MetaException, TException, InvalidOperationException {
    for (Partition part : partitions) {
      client.alter_partition(dbName, tblName, part);
    }
  }

  @Override
  public void setPriPartition(String dbName, String tblName, Partition priPart)
      throws MetaException, TException, InvalidOperationException {
    client.alter_partition(dbName, tblName, priPart);
  }

  @Override
  public void setSubPartition(String dbName, String tblName, Partition subPart)
      throws MetaException, TException, InvalidOperationException {
    client.alter_partition(dbName, tblName, subPart);
  }

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
      throws MetaException, TException, UnknownTableException, UnknownDBException {
    return client.get_schema(db, tableName);
  }
  
  public Warehouse getWarehouse(){
	  return ((HiveMetaStore.HMSHandler)client).getWarehouse();
  }

  // joeyli added for statics information collection begin
  
  public tdw_sys_table_statistics add_table_statistics(tdw_sys_table_statistics new_table_statistics) throws AlreadyExistsException, MetaException, TException{
	  return client.add_table_statistics(new_table_statistics);
  }

  public boolean delete_table_statistics(String table_statistics_name) throws NoSuchObjectException, MetaException, TException{
	  return client.delete_table_statistics(table_statistics_name);
  }

  public tdw_sys_table_statistics get_table_statistics(String table_statistics_name) throws MetaException, TException{
	  return client.get_table_statistics(table_statistics_name);
  }

  public List<tdw_sys_table_statistics> get_table_statistics_multi(int max_parts) throws NoSuchObjectException, MetaException, TException{
	  return client.get_table_statistics_multi(max_parts);
  }

  public List<String> get_table_statistics_names(int max_parts) throws NoSuchObjectException, MetaException, TException{
	  return client.get_table_statistics_names(max_parts);
  }

  public tdw_sys_fields_statistics add_fields_statistics(tdw_sys_fields_statistics new_fields_statistics) throws AlreadyExistsException, MetaException, TException{
	  return client.add_fields_statistics(new_fields_statistics);
  }

  public boolean delete_fields_statistics(String table_name,String field_name ) throws NoSuchObjectException, MetaException, TException{
	  return client.delete_fields_statistics(table_name,field_name);
  }

  public tdw_sys_fields_statistics get_fields_statistics(String table_name,String field_name) throws MetaException, TException{
	  return client.get_fields_statistics(table_name,field_name);
  }

  public List<tdw_sys_fields_statistics> get_fields_statistics_multi(String table_name,int max_parts) throws NoSuchObjectException, MetaException, TException{
	  return client.get_fields_statistics_multi(table_name,max_parts);
  }

  public List<String> get_fields_statistics_names(String table_name,int max_parts) throws NoSuchObjectException, MetaException, TException{
	  return client.get_fields_statistics_names(table_name,max_parts);
  }
  
  // joeyli added for statics information collection end

//added by BrantZhang for authorization begin
  
  public boolean create_role(String byWho, String roleName)
		throws AlreadyExistsException, TException, MetaException {
	return client.create_role(byWho, roleName);
  }

  public boolean create_user(String byWho, String newUser, String passwd)
		throws AlreadyExistsException, TException, MetaException {
	return client.create_user(byWho, newUser, passwd);
  }

  public boolean drop_role(String byWho, String roleName)
		throws NoSuchObjectException, TException, MetaException {
	return client.drop_role(byWho, roleName);
  }

  public boolean drop_user(String byWho, String userName)
		throws NoSuchObjectException, TException, MetaException {
	return client.drop_user(byWho, userName);
  }

  public DbPriv get_auth_on_db(String byWho, String who, String db)
		throws NoSuchObjectException, TException, MetaException {
	return client.get_auth_on_db(byWho, who, db);
  }
  
  public List<DbPriv> get_auth_on_dbs(String byWho, String who)
	    throws NoSuchObjectException, TException, MetaException {
	return client.get_auth_on_dbs(byWho, who);
  }

  public TblPriv get_auth_on_tbl(String byWho, String who, String db, String tbl)
		throws NoSuchObjectException, TException, MetaException {
	return client.get_auth_on_tbl(byWho, who, db, tbl);
  }
  
  public List<TblPriv> get_auth_on_tbls(String byWho, String who)
		throws NoSuchObjectException, TException, MetaException {
	return client.get_auth_on_tbls(byWho, who);
  }

  public List<DbPriv> get_db_auth(String byWho, String db)
		throws NoSuchObjectException, TException, MetaException {
	return client.get_db_auth(byWho, db);
  }

  public List<DbPriv> get_db_auth_all(String byWho) throws NoSuchObjectException,
		TException, MetaException {
	return client.get_db_auth_all(byWho);
  }

  public Role get_role(String byWho, String roleName)
		throws NoSuchObjectException, TException, MetaException {
	return client.get_role(byWho, roleName);
  }

  public List<String> get_roles_all(String byWho) throws TException,
		MetaException {
	return client.get_roles_all(byWho);
  }

  public List<TblPriv> get_tbl_auth(String byWho, String db, String tbl)
		throws NoSuchObjectException, TException, MetaException {
	return client.get_tbl_auth(byWho, db, tbl);
  }

  public List<TblPriv> get_tbl_auth_all(String byWho)
		throws NoSuchObjectException, TException, MetaException {
	return client.get_tbl_auth_all(byWho);
  }

  public User get_user(String byWho, String userName)
		throws NoSuchObjectException, TException, MetaException {
	return client.get_user(byWho, userName);
  }

  public List<String> get_users_all(String byWho) throws TException,
		MetaException {
	return client.get_users_all(byWho);
  }

  public boolean grant_auth_on_db(String byWho, String forWho,
		List<String> privileges, String db) throws NoSuchObjectException,
		TException, MetaException, InvalidObjectException {
	return client.grant_auth_on_db(byWho, forWho, privileges, db);
  }

  public boolean grant_auth_on_tbl(String byWho, String forWho,
		List<String> privileges, String db, String tbl)
		throws NoSuchObjectException, TException, InvalidObjectException,
		MetaException {
	return client.grant_auth_on_tbl(byWho, forWho, privileges, db, tbl);
  }

  public boolean grant_auth_role_sys(String byWho, String roleName,
		List<String> privileges) throws NoSuchObjectException, TException,
		InvalidObjectException, MetaException {
	return client.grant_auth_role_sys(byWho, roleName, privileges);
  }

  public boolean grant_auth_sys(String byWho, String userName,
		List<String> privileges) throws NoSuchObjectException, TException,
		InvalidObjectException, MetaException {
	return client.grant_auth_sys(byWho, userName, privileges);
  }

  public boolean grant_role_to_role(String byWho, String roleName,
		List<String> roleNames) throws NoSuchObjectException, TException,
		InvalidObjectException, MetaException {
	return client.grant_role_to_role(byWho, roleName, roleNames);
  }

  public boolean grant_role_to_user(String byWho, String userName,
		List<String> roleNames) throws NoSuchObjectException, TException,
		InvalidObjectException, MetaException {
	return client.grant_role_to_user(byWho, userName, roleNames);
  }

  public boolean is_a_role(String roleName) throws TException, MetaException {
	return client.is_a_role(roleName);
  }

  public boolean is_a_user(String userName, String passwd) throws TException,
		MetaException {
	return client.is_a_user(userName, passwd);
  }

  public boolean revoke_auth_on_db(String byWho, String who,
		List<String> privileges, String db) throws NoSuchObjectException,
		TException, InvalidObjectException, MetaException {
	return client.revoke_auth_on_db(byWho, who, privileges, db);
  }

  public boolean revoke_auth_on_tbl(String byWho, String who,
		List<String> privileges, String db, String tbl)
		throws NoSuchObjectException, TException, InvalidObjectException,
		MetaException {
	return client.revoke_auth_on_tbl(byWho, who, privileges, db, tbl);
  }

  public boolean revoke_auth_role_sys(String byWho, String roleName,
		List<String> privileges) throws NoSuchObjectException, TException,
		InvalidObjectException, MetaException {
	return client.revoke_auth_role_sys(byWho, roleName, privileges);
  }

  public boolean revoke_auth_sys(String byWho, String userName,
		List<String> privileges) throws NoSuchObjectException, TException,
		InvalidObjectException, MetaException {
	return client.revoke_auth_sys(byWho, userName, privileges);
  }

  public boolean revoke_role_from_role(String byWho, String roleName,
		List<String> roleNames) throws NoSuchObjectException, TException,
		InvalidObjectException, MetaException {
	return client.revoke_role_from_role(byWho, roleName, roleNames);
  }

  public boolean revoke_role_from_user(String byWho, String userName,
		List<String> roleNames) throws NoSuchObjectException, TException,
		InvalidObjectException, MetaException {
	return client.revoke_role_from_user(byWho, userName, roleNames);
  }

  public boolean set_passwd(String byWho, String forWho, String newPasswd)
		throws NoSuchObjectException, TException, MetaException {
	return client.set_passwd(byWho, forWho, newPasswd);
  }

@Override
public boolean drop_auth_in_db(String byWho, String forWho) throws TException,
		MetaException {
	return client.drop_auth_in_db(byWho, forWho);
}

@Override
public boolean drop_auth_in_tbl(String byWho, String forWho) throws TException,
		MetaException {
	return client.drop_auth_in_tbl(byWho, forWho);
}

@Override
public boolean drop_auth_on_db(String byWho, String forWho, String db)
		throws TException, MetaException {
	return client.drop_auth_on_db(byWho, forWho, db);
}

@Override
public boolean drop_auth_on_tbl(String byWho, String forWho, String db,
		String tbl) throws TException, MetaException {
	return client.drop_auth_on_tbl(byWho, forWho, db, tbl);
}

//added by BrantZhang for authorization end


//add by konten for index begin
public boolean create_index(IndexItem index) throws MetaException, TException
{
    return client.create_index(index);
}   

public boolean drop_index(String db, String table, String name) throws MetaException, TException
{
    return client.drop_index(db, table, name);
}

public int get_index_num(String db, String table) throws MetaException, TException
{    
    return client.get_index_num(db, table);
}

public int  get_index_type(String db, String table, String name) throws MetaException, TException
{   
    return client.get_index_type(db, table, name);
}

public String get_index_field(String db, String table, String name) throws MetaException, TException
{    
    return client.get_index_field(db, table, name);
}

public String get_index_location(String db, String table, String name) throws MetaException, TException
{
    return client.get_index_location(db, table, name);
}

public boolean set_index_location(String db, String table, String name, String location) throws MetaException, TException
{    
    return client.set_index_location(db, table, name, location);
}

public boolean set_index_status(String db, String table, String name, int status) throws MetaException, TException
{    
    return client.set_index_status(db, table, name, status);
}

public List<IndexItem>  get_all_index_table(String db, String table) throws MetaException, TException
{    
    return client.get_all_index_table(db, table);
}

public IndexItem get_index_info(String db, String table, String name) throws MetaException, TException
{    
    return client.get_index_info(db, table, name);
}

public List<IndexItem> get_all_index_sys() throws MetaException, TException
{    
    return client.get_all_index_sys();
}
//add by konten for index end
}
