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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.DataStoreCache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DbPriv;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.IndexItem;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TblPriv;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.tdw_sys_fields_statistics;
import org.apache.hadoop.hive.metastore.api.tdw_sys_table_statistics;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MDbPriv;
import org.apache.hadoop.hive.metastore.model.MFieldSchema;
import org.apache.hadoop.hive.metastore.model.MIndexItem;
import org.apache.hadoop.hive.metastore.model.MOrder;
import org.apache.hadoop.hive.metastore.model.MPartSpace;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MRole;
import org.apache.hadoop.hive.metastore.model.MSerDeInfo;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MTblPriv;
import org.apache.hadoop.hive.metastore.model.MType;
import org.apache.hadoop.hive.metastore.model.Mtdw_sys_fields_statistics;
import org.apache.hadoop.hive.metastore.model.Mtdw_sys_table_statistics;
import org.apache.hadoop.hive.metastore.model.MUser;
import org.apache.hadoop.util.StringUtils;

/**
 * This class is the interface between the application logic and the database store that
 * contains the objects. 
 * Refrain putting any logic in mode.M* objects or in this file as former could be auto
 * generated and this class would need to be made into a interface that can read both
 * from a database and a filestore.
 */
public class ObjectStore implements RawStore, Configurable {
  @SuppressWarnings("nls")
  private static final String JPOX_CONFIG = "jpox.properties";
  private static Properties prop = null;
  private static PersistenceManagerFactory pmf = null;
  private static final Log LOG = LogFactory.getLog(ObjectStore.class.getName());
  private static enum TXN_STATUS {
    NO_STATE,
    OPEN,
    COMMITED,
    ROLLBACK
  }
  private boolean isInitialized = false;
  private PersistenceManager pm = null;
  private Configuration hiveConf;
  private int openTrasactionCalls = 0;
  private Transaction currentTransaction = null;
  private TXN_STATUS transactionStatus = TXN_STATUS.NO_STATE;
  
  public ObjectStore() {}

  public Configuration getConf() {
    return hiveConf;
  }

  @SuppressWarnings("nls")
  public void setConf(Configuration conf) {
    this.hiveConf = conf;
    if(isInitialized) {
      return;
    } else {
      initialize();
    }
    if(!isInitialized) {
      throw new RuntimeException("Unable to create persistence manager. Check dss.log for details");
    } else {
      LOG.info("Initialized ObjectStore");
    }
  }

  private ClassLoader classLoader;
  {
    this.classLoader = Thread.currentThread().getContextClassLoader();
    if (this.classLoader == null) {
      this.classLoader = ObjectStore.class.getClassLoader();
    }
  }

  @SuppressWarnings("nls")
  private void initialize() {
    LOG.info("ObjectStore, initialize called");
    initDataSourceProps();
    pm = getPersistenceManager();
    if(pm != null)
      isInitialized = true;
    return;
  }

  /**
   * Properties specified in hive-default.xml override the properties specified in
   * jpox.properties.
   */
  @SuppressWarnings("nls")
  private void initDataSourceProps() {
    if(prop != null) {
      return;
    }
    prop = new Properties();
    
    Iterator<Map.Entry<String, String>> iter = hiveConf.iterator();
    while(iter.hasNext()) {
      Map.Entry<String, String> e = iter.next();
      if(e.getKey().contains("datanucleus") || e.getKey().contains("jdo")) {
        Object prevVal = prop.setProperty(e.getKey(), e.getValue());
        if(LOG.isDebugEnabled() && !e.getKey().equals(HiveConf.ConfVars.METASTOREPWD.varname)) {
          LOG.debug("Overriding " + e.getKey() + " value " + prevVal 
              + " from  jpox.properties with " + e.getValue());
        }
      }
    }

    if(LOG.isDebugEnabled()) {
      for (Entry<Object, Object> e: prop.entrySet()) {
        if(!e.getKey().equals(HiveConf.ConfVars.METASTOREPWD.varname))
          LOG.debug(e.getKey() + " = " + e.getValue());
      }
    }
  }

  private static PersistenceManagerFactory getPMF() {
    if(pmf == null) {
      pmf = JDOHelper.getPersistenceManagerFactory(prop);
      DataStoreCache dsc = pmf.getDataStoreCache();
      if(dsc != null) {
        dsc.pinAll(true, MTable.class);
        dsc.pinAll(true, MStorageDescriptor.class);
        dsc.pinAll(true, MSerDeInfo.class);
        dsc.pinAll(true, MPartition.class);
        dsc.pinAll(true, MDatabase.class);
        dsc.pinAll(true, MType.class);
        dsc.pinAll(true, MFieldSchema.class);
        dsc.pinAll(true, MOrder.class);
      }
    }
    return pmf;
  }
  
  private PersistenceManager getPersistenceManager() {
    return getPMF().getPersistenceManager();
  }
  
  public void shutdown() {
    if(pm != null) {
      pm.close();
    }
  }

  /**
   * Opens a new one or the one already created
   * Every call of this function must have corresponding commit or rollback function call
   * @return an active transaction
   */
  
  public boolean openTransaction() {
    this.openTrasactionCalls++;
    if(this.openTrasactionCalls == 1) {
      currentTransaction = pm.currentTransaction();
      currentTransaction.begin();
      transactionStatus = TXN_STATUS.OPEN;
    } else {
      // something is wrong since openTransactionCalls is greater than 1 but currentTransaction is not active
      assert((currentTransaction != null) && (currentTransaction.isActive()));
    }
    return currentTransaction.isActive();
  }
  
  /**
   * if this is the commit of the first open call then an actual commit is called. 
   * @return Always returns true
   */
  @SuppressWarnings("nls")
  public boolean commitTransaction() {
    assert(this.openTrasactionCalls >= 1);
    if(!currentTransaction.isActive()) {
      throw new RuntimeException("Commit is called, but transaction is not active. Either there are" +
          "mismatching open and close calls or rollback was called in the same trasaction");
    }
    this.openTrasactionCalls--;
    if ((this.openTrasactionCalls == 0) && currentTransaction.isActive()) {
      transactionStatus = TXN_STATUS.COMMITED;
      currentTransaction.commit();
    }
    return true;
  }
  
  /**
   * @return true if there is an active transaction. If the current transaction is either
   * committed or rolled back it returns false
   */
  public boolean isActiveTransaction() {
    if(currentTransaction == null)
      return false;
    return currentTransaction.isActive();
  }
  
  /**
   * Rolls back the current transaction if it is active
   */
  public void rollbackTransaction() {
    if(this.openTrasactionCalls < 1) {
      return;
    }
    this.openTrasactionCalls = 0;
    if(currentTransaction.isActive() && transactionStatus != TXN_STATUS.ROLLBACK) {
      transactionStatus = TXN_STATUS.ROLLBACK;
       // could already be rolled back
      currentTransaction.rollback();
    }
  }

  public boolean createDatabase(Database db) {
    boolean success = false;
    boolean commited = false;
    MDatabase mdb = new MDatabase(db.getName().toLowerCase(), db.getDescription());
    try {
      openTransaction();
      pm.makePersistent(mdb);
      success = true;
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return success;
  }
  
  public boolean createDatabase(String name) {
    // TODO: get default path
    Database db = new Database(name, "default_path");
    return this.createDatabase(db);
  }
  
  @SuppressWarnings("nls")
  private MDatabase getMDatabase(String name) throws NoSuchObjectException {
    MDatabase db = null;
    boolean commited = false;
    try {
      openTransaction();
      name = name.toLowerCase();
      Query query = pm.newQuery(MDatabase.class, "name == dbname");
      query.declareParameters("java.lang.String dbname");
      query.setUnique(true);
      db = (MDatabase) query.execute(name.trim());
      pm.retrieve(db);
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    if(db == null) {
      throw new NoSuchObjectException("There is no database named " + name);
    }
    return db;
  }
  public Database getDatabase(String name) throws NoSuchObjectException {
    MDatabase db = null;
    boolean commited = false;
    try {
      openTransaction();
      db = getMDatabase(name);
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return new Database(db.getName(), db.getDescription());
  }

  public boolean dropDatabase(String dbname) {
    
    boolean success = false;
    boolean commited = false;
    try {
      openTransaction();
      dbname = dbname.toLowerCase();
      //added by brantzhang for auth begin
	  //first drop lines in dbpriv table
      LOG.info("Dropping all lines in dbpriv table for db: " + dbname);
      Query q2 = pm.newQuery(MDbPriv.class, "db.name == dbName");
      q2.declareParameters("java.lang.String dbName");
      List<MDbPriv> dbprivs = (List<MDbPriv>) q2.execute(dbname.trim());
      if(dbprivs != null)
    	  pm.deletePersistentAll(dbprivs);
      
      //second drop lines in tblpriv table
      LOG.info("Dropping all lines in tblpriv table for db: " + dbname);
      Query q3 = pm.newQuery(MTblPriv.class, "db.name == dbName");
      q3.declareParameters("java.lang.String dbName");
      List<MTblPriv> mtblprivs = (List<MTblPriv>) q3.execute(dbname.trim());
      if(mtblprivs != null)
    	  pm.deletePersistentAll(mtblprivs);
  	  //added by brantzhang for auth end
      
      // first drop tables
      
      LOG.info("Dropping database along with all tables " + dbname);
      Query q1 = pm.newQuery(MTable.class, "database.name == dbName");
      q1.declareParameters("java.lang.String dbName");
      List<MTable> mtbls = (List<MTable>) q1.execute(dbname.trim());
      pm.deletePersistentAll(mtbls);

      // then drop the database
      Query query = pm.newQuery(MDatabase.class, "name == dbName"); 
      query.declareParameters("java.lang.String dbName"); 
      query.setUnique(true); 
      MDatabase db = (MDatabase) query.execute(dbname.trim()); 
      pm.retrieve(db);
      
      //StringIdentity id = new StringIdentity(MDatabase.class, dbname);
      //MDatabase db = (MDatabase) pm.getObjectById(id);
      if(db != null)
        pm.deletePersistent(db);
      commited = commitTransaction();
      success = true;
    } catch (JDOObjectNotFoundException e) {
      LOG.debug("database not found " + dbname,e);
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return success;
  }

  public List<String> getDatabases() {
    List dbs = null;
    boolean commited = false;
    try {
      openTransaction();
      Query query = pm.newQuery(MDatabase.class);
      query.setResult("name");
      query.setResultClass(String.class);
      query.setOrdering("name asc");
      dbs = (List) query.execute();
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return dbs;
  }
  
  private MType getMType(Type type) {
    List<MFieldSchema> fields = new ArrayList<MFieldSchema>();
    if(type.getFields() != null) {
      for (FieldSchema field : type.getFields()) {
        fields.add(new MFieldSchema(field.getName(), field.getType(), field.getComment()));
      }
    }
    return new MType(type.getName(), type.getType1(), type.getType2(), fields);
  }

  private Type getType(MType mtype) {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    if(mtype.getFields() != null) {
      for (MFieldSchema field : mtype.getFields()) {
        fields.add(new FieldSchema(field.getName(), field.getType(), field.getComment()));
      }
    }
    return new Type(mtype.getName(), mtype.getType1(), mtype.getType2(), fields);
  }

  public boolean createType(Type type) {
    boolean success = false;
    MType mtype = getMType(type);
    boolean commited = false;
    try {
      openTransaction();
      pm.makePersistent(mtype);
      commited = commitTransaction();
      success = true;
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return success;
  }

  public Type getType(String typeName) {
    Type type = null;
    boolean commited = false;
    try {
      openTransaction();
      Query query = pm.newQuery(MType.class, "name == typeName"); 
      query.declareParameters("java.lang.String typeName"); 
      query.setUnique(true); 
      MType mtype = (MType) query.execute(typeName.trim()); 
      pm.retrieve(type);
      if(mtype != null) {
        type = getType(mtype);
      }
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return type;
  }

  public boolean dropType(String typeName) {
    
    boolean success = false;
    boolean commited = false;
    try {
      openTransaction();
      Query query = pm.newQuery(MType.class, "name == typeName"); 
      query.declareParameters("java.lang.String typeName"); 
      query.setUnique(true); 
      MType type = (MType) query.execute(typeName.trim()); 
      pm.retrieve(type);
      pm.deletePersistent(type);
      commited = commitTransaction();
      success = true;
    } catch (JDOObjectNotFoundException e) {
      commited = commitTransaction();
      LOG.debug("type not found " + typeName, e);
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return success;
  }

  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MTable mtbl = convertToMTable(tbl);
      pm.makePersistent(mtbl);
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
  }
  
  public boolean dropTable(String dbName, String tableName) {
    
    boolean success = false;
    try {
      openTransaction();
      MTable tbl = getMTable(dbName, tableName); 
      pm.retrieve(tbl);
      if(tbl != null) {
    	//added by brantzhang for auth begin
    	  String tablename = tableName.toLowerCase();
    	  dbName = dbName.toLowerCase();
    	//first drop lines in tblpriv table
          LOG.info("Dropping all lines in tblpriv table for table: " + tablename + " in db: " + dbName);
          Query q1 = pm.newQuery(MTblPriv.class, "db.name == dbName && table.tableName == tablename");
          q1.declareParameters("java.lang.String dbName, java.lang.String tablename");
          List<MTblPriv> mtblprivs = (List<MTblPriv>) q1.execute(dbName.trim(), tablename.trim());
          if(mtblprivs != null)
        	  pm.deletePersistentAll(mtblprivs);  
    	//added by brantzhang for auth end
    	  
        // first remove the table
        pm.deletePersistent(tbl);
        List<MPartition> partitions = listMPartitions(dbName, tableName);
        /**
        for (MPartition part : partitions) {
          pm.deletePersistentAll(listMPartSpaces(part));
          pm.deletePersistentAll(listMStorageDescriptor(part));
        }
        /**/
        // then remove all the partitions
        pm.deletePersistentAll(partitions);
        // pm.deletePersistent(tbl);
      }
      success = commitTransaction();
    } finally {
      if(!success) {
        rollbackTransaction();
      }
    }
    return success;
  }
  
  private List<MPartSpace> listMPartSpaces(MPartition partition) {
    List<MPartSpace> partSpaces = new ArrayList<MPartSpace>();
    partSpaces.addAll(partition.getPartSpaceMap().values());
    return partSpaces;
  }

  public Table getTable(String dbName, String tableName) throws MetaException {
    boolean commited = false;
    Table tbl = null;
    try {
      openTransaction();
      tbl = convertToTable(getMTable(dbName, tableName));
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return tbl;
  }
  
  public List<String> getTables(String dbName, String pattern) throws MetaException {
    boolean commited = false;
    List<String> tbls = null;
    try {
      openTransaction();
      dbName = dbName.toLowerCase();
      // Take the pattern and split it on the | to get all the composing patterns
      String [] subpatterns = pattern.trim().split("\\|");
      String query = "select tableName from org.apache.hadoop.hive.metastore.model.MTable where database.name == dbName && (";
      boolean first = true;
      for(String subpattern: subpatterns) {
        subpattern = "(?i)" + subpattern.replaceAll("\\*", ".*");
        if (!first) {
          query = query + " || ";
        }
        query = query + " tableName.matches(\"" + subpattern + "\")";
        first = false;
      }
      query = query + ")";

      Query q = pm.newQuery(query);
      q.declareParameters("java.lang.String dbName");
      q.setResult("tableName");
      Collection names = (Collection) q.execute(dbName.trim());
      tbls = new ArrayList<String>(); 
      for (Iterator i = names.iterator (); i.hasNext ();) {
          tbls.add((String) i.next ()); 
      }
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return tbls;
  }
  
  private MTable getMTable(String db, String table) {
    MTable mtbl = null;
    boolean commited = false;
    try {
      openTransaction();
      db = db.toLowerCase();
      table = table.toLowerCase();
      Query query = pm.newQuery(MTable.class, "tableName == table && database.name == db"); 
      query.declareParameters("java.lang.String table, java.lang.String db"); 
      query.setUnique(true); 
      mtbl = (MTable) query.execute(table.trim(), db.trim()); 
      pm.retrieve(mtbl);
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return mtbl;
  }

  private Table convertToTable(MTable mtbl) throws MetaException {
    if(mtbl == null) return null;
    // Modified by : guosijie
    // Modified Date : 2010-02-05
    //   since the structure of partition is changed, we need to change this method
    
    // Modification start
    
//    return new Table(mtbl.getTableName(),
//        mtbl.getDatabase().getName(),
//        mtbl.getOwner(),
//        mtbl.getCreateTime(),
//        mtbl.getLastAccessTime(),
//        mtbl.getRetention(),
//        convertToStorageDescriptor(mtbl.getSd()),
//        convertToFieldSchemas(mtbl.getPartitionKeys()),
//        mtbl.getParameters());
    return new Table(mtbl.getTableName(),
        mtbl.getDatabase().getName(),
        mtbl.getOwner(),
        mtbl.getCreateTime(),
        mtbl.getLastAccessTime(),
        mtbl.getRetention(),
        convertToStorageDescriptor(mtbl.getSd()),
        convertToPart(mtbl.getPriPartition()),
        convertToPart(mtbl.getSubPartition()),
        mtbl.getParameters());
    
    // Modificiation end
  }
  
  private MTable convertToMTable(Table tbl) throws InvalidObjectException, MetaException {
    if(tbl == null) return null;
    MDatabase mdb = null;
    try {
      mdb = this.getMDatabase(tbl.getDbName());
    } catch (NoSuchObjectException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new InvalidObjectException("Database " + tbl.getDbName() + " doesn't exsit.");
    }
    
    // Modified by : guosijie
    // Modified Date : 2010-02-05
    //   since the structure of partition is changed, we need to change this method
    
    // Modification start
    
    return new MTable(tbl.getTableName().toLowerCase(),
        mdb,
        convertToMStorageDescriptor(tbl.getSd()),
        tbl.getOwner(),
        tbl.getCreateTime(),
        tbl.getLastAccessTime(),
        tbl.getRetention(),
        convertToMPart(tbl.getPriPartition()),
        convertToMPart(tbl.getSubPartition()),
        // convertToMFieldSchemas(tbl.getPartitionKeys()),
        tbl.getParameters());
    
    // Modification end
  }
  
  // Added by : guosijie
  // Added date : 2010-02-05
  //   add a method to convert the structures between MFieldSchema and FieldSchema
  
  private MFieldSchema convertToMFieldSchema(FieldSchema key) {
    return new MFieldSchema(key.getName().toLowerCase(), key.getType(), key.getComment());
  }
  
  private FieldSchema convertToFieldSchema(MFieldSchema key) {
    return new FieldSchema(key.getName().toLowerCase(), key.getType(), key.getComment());
  }
  
  // Added End
  
  private List<MFieldSchema> convertToMFieldSchemas(List<FieldSchema> keys) {
    List<MFieldSchema> mkeys = null;
    if(keys != null) {
      mkeys = new ArrayList<MFieldSchema>(keys.size());
      for (FieldSchema part : keys) {
        mkeys.add(new MFieldSchema(part.getName().toLowerCase(), part.getType(), part.getComment()));
      }
    }
    return mkeys;
  } 
  
  private List<FieldSchema> convertToFieldSchemas(List<MFieldSchema> mkeys) {
    List<FieldSchema> keys = null;
    if(mkeys != null) {
      keys = new ArrayList<FieldSchema>(mkeys.size());
      for (MFieldSchema part : mkeys) {
        keys.add(new FieldSchema(part.getName(), part.getType(), part.getComment()));
      }
    }
    return keys;
  }
  
  private List<MOrder> convertToMOrders(List<Order> keys) {
    List<MOrder> mkeys = null;
    if(keys != null) {
      mkeys = new ArrayList<MOrder>(keys.size());
      for (Order part : keys) {
        mkeys.add(new MOrder(part.getCol().toLowerCase(), part.getOrder()));
      }
    }
    return mkeys;
  } 
  
  private List<Order> convertToOrders(List<MOrder> mkeys) {
    List<Order> keys = null;
    if(mkeys != null) {
      keys = new ArrayList<Order>();
      for (MOrder part : mkeys) {
        keys.add(new Order(part.getCol(), part.getOrder()));
      }
    }
    return keys;
  }
  
  private SerDeInfo converToSerDeInfo(MSerDeInfo ms) throws MetaException {
   if(ms == null) throw new MetaException("Invalid SerDeInfo object");
   return new SerDeInfo(ms.getName(),
       ms.getSerializationLib(),
       ms.getParameters()); 
  }
  
  private MSerDeInfo converToMSerDeInfo(SerDeInfo ms) throws MetaException {
    if(ms == null) throw new MetaException("Invalid SerDeInfo object");
    return new MSerDeInfo(ms.getName(),
        ms.getSerializationLib(),
        ms.getParameters()); 
   }
  
  // Added by : guosijie
  // Added date : 2010-02-05
  //   add a common method to convert the data structures between msds and sds.
  
  private Map<String, StorageDescriptor> convertToStorageDescriptors(
      Map<String, MStorageDescriptor> msds)
  throws MetaException {
    Map<String, StorageDescriptor> sds = null;
    if (msds == null) return null;
    
    sds = new HashMap<String, StorageDescriptor>();
    for (String key : msds.keySet()) {
      MStorageDescriptor msd = msds.get(key);
      if (msd == null)
        continue;
      
      sds.put(key, convertToStorageDescriptor(msd));
    }
    return sds;
  }
  
  private Map<String, MStorageDescriptor> convertToMStorageDescriptors(
      Map<String, StorageDescriptor> sds)
  throws MetaException {
    Map<String, MStorageDescriptor> msds = null;
    if (sds == null) return null;
    
    msds = new HashMap<String, MStorageDescriptor>();
    for (String key : sds.keySet()) {
      StorageDescriptor sd = sds.get(key);
      if (sd == null)
        continue;
      
      msds.put(key, convertToMStorageDescriptor(sd));
    }
    return msds;
  }
  
  // Added end
  
  // MSD and SD should be same objects. Not sure how to make then same right now
  // MSerdeInfo *& SerdeInfo should be same as well
  private StorageDescriptor convertToStorageDescriptor(MStorageDescriptor msd) throws MetaException {
    if(msd == null) return null;
    return new StorageDescriptor(
        convertToFieldSchemas(msd.getCols()),
        msd.getLocation(),
        msd.getInputFormat(),
        msd.getOutputFormat(),
        msd.isCompressed(),
        msd.getNumBuckets(),
        converToSerDeInfo(msd.getSerDeInfo()),
        msd.getBucketCols(),
        convertToOrders(msd.getSortCols()),
        msd.getParameters());
  }
  
  private MStorageDescriptor convertToMStorageDescriptor(StorageDescriptor sd) throws MetaException {
    if(sd == null) return null;
    return new MStorageDescriptor(
        convertToMFieldSchemas(sd.getCols()),
        sd.getLocation(),
        sd.getInputFormat(),
        sd.getOutputFormat(),
        sd.isCompressed(),
        sd.getNumBuckets(),
        converToMSerDeInfo(sd.getSerdeInfo()),
        sd.getBucketCols(),
        convertToMOrders(sd.getSortCols()),
        sd.getParameters());
  }
  
  // Modified by : guosijie
  // Modified date : 2010-03-09
  //   we do not need addPartition any more, use alterPartition for replace
  
//  public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
//    boolean success = false;
//    boolean commited = false;
//    try {
//      openTransaction();
//      MPartition mpart = convertToMPart(part);
//      pm.makePersistent(mpart);
//      commited = commitTransaction();
//      success = true;
//    } finally {
//      if(!commited) {
//        rollbackTransaction();
//      }
//    }
//    return success;
//  }
  
  // Modification End
  
  // Modified by : guosijie
  // Modified date : 2010-02-08
  //   change the rawstore interfaces, so we need to change its implementation
  
  // Modification start
  
  @Override
  public Partition getPartition(String dbName, String tableName, int level)
      throws MetaException {
    this.openTransaction();
    Partition part = convertToPart(
        this.getMPartition(dbName, tableName, level));
    this.commitTransaction();
    return part;
  }
  
  /**
   * Get a partition.
   * 
   * 1) if subPartName is null & priPartName is not null (means that we retrieve a 
   *    target top-level partition;), the returned partition object's level is 1.
   * 2) if subPartName is not null & priPartName is not null (means that we retrieve
   *    a bottom-level partition;), the returned partition object's level is 2.
   * 3) if priPartName is null & subPartName is not null (means that we retrieve
   *    the whole bottom-level partitions use the same bottom name;), the returned 
   *    objects are the level 2 partitions. 
   * 4) if priPartName is null & subPartName is null, throws MetaException;
   * 
   *    
   * @param dbName database name
   * @param tableName table name
   * @param priPartName primary partition name
   * @param subPartName sub parttiion name
   * @return the target partition object
   * @throws MetaException
   */
  private MPartition getMPartition(String db, String table, int level)
  throws MetaException {
    MPartition mpart = null;
    boolean commited = false;
    try {
      openTransaction();
      db= db.toLowerCase();
      table = table.toLowerCase();
      /**
      MTable mtbl = this.getMTable(dbName, tableName);
      if (mtbl == null) {
        commited = commitTransaction();
        return null;
      }
      
      if (level == 0) {
        mpart = mtbl.getPriPartition();
      } else if (level == 1) {
        mpart = mtbl.getSubPartition();
      } else {
        throw new MetaException("Unknown partition level.");
      } **/
      Query query = pm.newQuery(MPartition.class, "level == l && tableName == table && dbName == db"); 
      query.declareParameters("java.lang.Integer l, java.lang.String table, java.lang.String db"); 
      query.setUnique(true); 
      mpart = (MPartition) query.execute(level, table.trim(), db.trim()); 
      pm.retrieve(mpart);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return mpart;
  }
  
  /** 
  public Partition getPartition(String dbName, String tableName, List<String> part_vals) throws MetaException {
    this.openTransaction();
    Partition part = convertToPart(this.getMPartition(dbName, tableName, part_vals));
    this.commitTransaction();
    return part;
  }

  private MPartition getMPartition(String dbName, String tableName, List<String> part_vals) throws MetaException {
    MPartition mpart = null;
    boolean commited = false;
    try {
      openTransaction();
      dbName = dbName.toLowerCase();
      tableName = tableName.toLowerCase();
      MTable mtbl = this.getMTable(dbName, tableName);
      if(mtbl == null) {
        commited = commitTransaction();
        return null;
      }
      // Change the query to use part_vals instead of the name which is redundant
      String name = Warehouse.makePartName(convertToFieldSchemas(mtbl.getPartitionKeys()), part_vals);
      Query query = pm.newQuery(MPartition.class, "table.tableName == t1 && table.database.name == t2 && partitionName == t3"); 
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3"); 
      query.setUnique(true); 
      mpart = (MPartition) query.execute(tableName.trim(), dbName.trim(), name); 
      pm.retrieve(mpart);
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return mpart;
  }
  **/
  
  // Modification end
  
  private MPartition convertToMPart(Partition part) throws InvalidObjectException, MetaException {
    if(part == null) {
      return null;
    }
//    MTable mt = getMTable(part.getDbName(), part.getTableName());
//    if(mt == null) {
//      throw new InvalidObjectException("Partition doesn't have a valid table or database name");
//    }
    // Modified by : guosijie
    // Modified date : 2010-02-05
    //  return the new partition structure
    
    // Modification start
    
    return new MPartition(
        part.getDbName().toLowerCase(),
        part.getTableName().toLowerCase(),
        part.getLevel(),
        part.getParType(),
        convertToMFieldSchema(part.getParKey()),
        part.getParSpaces()
        );
//    return new MPartition(
//        Warehouse.makePartName(convertToFieldSchemas(mt.getPartitionKeys()), part.getValues()),
//        mt,
//        part.getValues(),
//        part.getCreateTime(),
//        part.getLastAccessTime(),
//        convertToMStorageDescriptor(part.getSd()),
//        part.getParameters());
    
    // Modification end
  }
  
  private Partition convertToPart(MPartition mpart) throws MetaException {
    if(mpart == null) {
      return null;
    }
    
    // Modified by : guosijie
    // Modified date : 2010-02-05
    //  return the new partition structure
    
    // Modification start
    
    return new Partition(
        mpart.getDBName(),
        mpart.getTableName(),
        mpart.getLevel(),
        mpart.getParType(),
        convertToFieldSchema(mpart.getParKey()),
        mpart.getParSpaces()
        );
//    return new Partition(
//        mpart.getValues(),
//        mpart.getTable().getDatabase().getName(),
//        mpart.getTable().getTableName(),
//        mpart.getCreateTime(),
//        mpart.getLastAccessTime(),
//        convertToStorageDescriptor(mpart.getSd()),
//        mpart.getParameters());
    
    // Modification end
  }
  
  // Modified by : guosijie 
  // Modified date : 2010-02-08
  
  // Modfication start
  
  /**
  public boolean dropPartition(String dbName, String tableName, List<String> part_vals) throws MetaException {
    boolean success = false;
    try {
      openTransaction();
      MPartition part = this.getMPartition(dbName, tableName, part_vals); 
      if(part != null)
        pm.deletePersistent(part);
      success = commitTransaction();
    } finally {
      if(!success) {
        rollbackTransaction();
      }
    }
    return success;
  }
  **/
  
  // Modification end
  
  // Modification By : guosijie
  // Modification Date : 2010-03-09
  //   we do not need to list partitions any more , we just return the partition definition by getPartition
  
  // Modification start

//  public List<Partition> getPartitions(String dbName, String tableName, int max) throws MetaException {
//    this.openTransaction();
//    List<Partition> parts = convertToParts(this.listMPartitions(dbName, tableName, max));
//    this.commitTransaction();
//    return parts;
//  }
  
  // Modification end
  
  private List<Partition> convertToParts(List<MPartition> mparts) throws MetaException {
    List<Partition> parts = new ArrayList<Partition>(mparts.size());
    for (MPartition mp : mparts) {
      parts.add(this.convertToPart(mp));
    }
    return parts;
  }

  // Modification By : guosijie
  // Modification Date : 2010-03-09
  //   we do not need to list partitions any more.

  //TODO:pc implement max
  
  /**
  public List<String> listPartitionNames(String dbName, String tableName, short max) throws MetaException {
    List<String> pns = new ArrayList<String>();
    boolean success = false;
    try {
      openTransaction();
      LOG.debug("Executing getPartitionNames");
      dbName = dbName.toLowerCase();
      tableName = tableName.toLowerCase();
      Query q = pm.newQuery("select partitionName from org.apache.hadoop.hive.metastore.model.MPartition where table.database.name == t1 && table.tableName == t2 order by partitionName asc");
      q.declareParameters("java.lang.String t1, java.lang.String t2");
      q.setResult("partitionName");
      Collection names = (Collection) q.execute(dbName.trim(), tableName.trim());
      pns = new ArrayList<String>(); 
      for (Iterator i = names.iterator (); i.hasNext ();) {
          pns.add((String) i.next ()); 
      }
      success = commitTransaction();
    } finally {
      if(!success) {
        rollbackTransaction();
      }
    }
    return pns;
  }
  **/
  
  // Modification end
  
  // TODO:pc implement max
  private List<MPartition> listMPartitions(String dbName, String tableName) {
    boolean success = false;
    List<MPartition> mparts = null;
    try {
      openTransaction();
      LOG.debug("Executing listMPartitions");
      dbName = dbName.toLowerCase();
      tableName = tableName.toLowerCase();
      Query query = pm.newQuery(MPartition.class, "tableName == t1 && dbName == t2"); 
      query.declareParameters("java.lang.String t1, java.lang.String t2"); 
      mparts = (List<MPartition>) query.execute(tableName.trim(), dbName.trim()); 
      LOG.debug("Done executing query for listMPartitions");
      pm.retrieveAll(mparts);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listMPartitions");
    } finally {
      if(!success) {
        rollbackTransaction();
      }
    }
    return mparts;
  }

  public void alterTable(String dbname, String name, Table newTable) throws InvalidObjectException, MetaException {
    boolean success = false;
    try {
      openTransaction();
      name = name.toLowerCase();
      dbname = dbname.toLowerCase();
      MTable newt = convertToMTable(newTable);
      if(newt == null) {
        throw new InvalidObjectException("new table is invalid");
      }
      
      MTable oldt = this.getMTable(dbname, name);
      if(oldt == null) {
        throw new MetaException("table " + name + " doesn't exist");
      }
      
      // For now only alter name, owner, paramters, cols, bucketcols are allowed
      oldt.setTableName(newt.getTableName().toLowerCase());
      oldt.setParameters(newt.getParameters());
      oldt.setOwner(newt.getOwner());
      oldt.setSd(newt.getSd());
      oldt.setDatabase(newt.getDatabase());
      oldt.setRetention(newt.getRetention());
      // Modification by : guosijie
      // Modification Date : 2010-03-09
      //   as the partition interfaces are changed, so we changed here
      
      // oldt.setPartitionKeys(newt.getPartitionKeys()); 
      oldt.setPriPartition(newt.getPriPartition());
      oldt.setSubPartition(newt.getSubPartition());
      
      // Modification end
      
      // commit the changes
      success = commitTransaction();
    } finally {
      if(!success) {
        rollbackTransaction();
      }
    }
  }

  // Modification By : guosijie
  // Modification Date : 2010-03-10
  
  // Modification start
  
  public void alterPartition(String dbname, String name, Partition newPart) 
  throws InvalidObjectException, MetaException {
    boolean success = false;
    try {
      openTransaction();
      name = name.toLowerCase();
      dbname = dbname.toLowerCase();
      LOG.info("get old partition...DB:" + dbname + ",table:" + name + ",level:" + newPart.getLevel() );
      MPartition oldp = getMPartition(dbname, name, newPart.getLevel());
      if(oldp == null){
    	  throw new InvalidObjectException("Partition does not exist." + "DB: " + dbname + ",table: " + name + ",level: " + newPart.getLevel() );
      }
      MPartition newp = convertToMPart(newPart);
      if (newp == null) {
        throw new InvalidObjectException("new Partition does not exist.");
      }
      oldp.setParKey(newp.getParKey());
      oldp.setParSpaces(newp.getParSpaces());
      oldp.setParType(newp.getParType());
      oldp.setDBName(newp.getDBName());
      oldp.setTableName(newp.getTableName());
      
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }
  
  /**
  public void alterPartition(String dbname, String name, Partition newPart)
  throws InvalidObjectException, MetaException {
    boolean success = false;
    try {
      openTransaction();
      name = name.toLowerCase();
      dbname = dbname.toLowerCase();
      MPartition oldp = getMPartition(dbname, name, newPart.getValues());
      MPartition newp = convertToMPart(newPart);
      if (oldp == null || newp == null) {
        throw new InvalidObjectException("partition does not exist.");
      }
      oldp.setParameters(newPart.getParameters());
      copyMSD(newp.getSd(), oldp.getSd());
      if (newp.getCreateTime() != oldp.getCreateTime())
        oldp.setCreateTime(newp.getCreateTime());
      if (newp.getLastAccessTime() != oldp.getLastAccessTime())
        oldp.setLastAccessTime(newp.getLastAccessTime());
      // commit the changes
      success = commitTransaction();
    } finally {
      if(!success) {
        rollbackTransaction();
      }
    }
  }
  **/
  
  // Modification end

  private void copyMSD(MStorageDescriptor newSd, MStorageDescriptor oldSd) {
    oldSd.setLocation(newSd.getLocation());
    oldSd.setCols(newSd.getCols());
    oldSd.setBucketCols(newSd.getBucketCols());
    oldSd.setCompressed(newSd.isCompressed());
    oldSd.setInputFormat(newSd.getInputFormat());
    oldSd.setOutputFormat(newSd.getOutputFormat());
    oldSd.setNumBuckets(newSd.getNumBuckets());
    oldSd.getSerDeInfo().setName(newSd.getSerDeInfo().getName());
    oldSd.getSerDeInfo().setSerializationLib(newSd.getSerDeInfo().getSerializationLib());
    oldSd.getSerDeInfo().setParameters(newSd.getSerDeInfo().getParameters());
  }
  
  // joeyli added for statics information collection begin

  private tdw_sys_table_statistics convertToTable_statistics(Mtdw_sys_table_statistics mtable_statistics) throws MetaException {
	    if(mtable_statistics == null) {
	      return null;
	    }
	    return new tdw_sys_table_statistics(
	    		mtable_statistics.getStat_table_name(),
	    		mtable_statistics.getStat_num_records(),
	    		mtable_statistics.getStat_num_units(),
	    		mtable_statistics.getStat_total_size(),
	    		mtable_statistics.getStat_num_files(),
	    		mtable_statistics.getStat_num_blocks());

  }
  
  private Mtdw_sys_table_statistics convertToMTable_statistics(tdw_sys_table_statistics table_statistics) throws MetaException {
	    if(table_statistics == null) {
	      return null;
	    }
	    return new Mtdw_sys_table_statistics(
	    		table_statistics.getStat_table_name(),
	    		table_statistics.getStat_num_records(),
	    		table_statistics.getStat_num_units(),
	    		table_statistics.getStat_total_size(),
	    		table_statistics.getStat_num_files(),
	    		table_statistics.getStat_num_blocks());

}

  private List<tdw_sys_table_statistics> convertToTable_statisticses(List<Mtdw_sys_table_statistics> mtable_statisticses) throws MetaException {
	    List<tdw_sys_table_statistics> table_statisticses = new ArrayList<tdw_sys_table_statistics>(mtable_statisticses.size());
	    for (Mtdw_sys_table_statistics mp : mtable_statisticses) {
	    	table_statisticses.add(this.convertToTable_statistics(mp));
	    }
	    return table_statisticses;
	  }
  
  public boolean add_table_statistics(tdw_sys_table_statistics new_table_statistics) throws  MetaException{
	    boolean success = false;
	    boolean commited = false;
	    try {
	      openTransaction();
	      Mtdw_sys_table_statistics mtable_statistics = convertToMTable_statistics(new_table_statistics);
	      pm.makePersistent(mtable_statistics);
	      commited = commitTransaction();
	      success = true;
	    } finally {
	      if(!commited) {
	        rollbackTransaction();
	      }
	    }
	    return success;
  }

  public boolean delete_table_statistics(String table_statistics_name)  throws MetaException{
	    boolean success = false;
	    try {
	      openTransaction();
	      Mtdw_sys_table_statistics mtable_statistics = this.get_Mtable_statistics(table_statistics_name); 
	      if(mtable_statistics != null)
	        pm.deletePersistent(mtable_statistics);
	      success = commitTransaction();
	    } finally {
	      if(!success) {
	        rollbackTransaction();
	      }
	    }
	    return success;
  }

  public Mtdw_sys_table_statistics get_Mtable_statistics(String table_statistics_name) throws MetaException{
	    
	    Mtdw_sys_table_statistics mtable_statistics = null;
	    boolean commited = false;
	    try {
	      openTransaction();
	      table_statistics_name = table_statistics_name.toLowerCase();
	      Query query = pm.newQuery(Mtdw_sys_table_statistics.class, "stat_table_name == table_statistics_name"); 
	      query.declareParameters("java.lang.String table_statistics_name"); 
	      query.setUnique(true); 
	      mtable_statistics = (Mtdw_sys_table_statistics) query.execute(table_statistics_name.trim()); 
	      pm.retrieve(mtable_statistics);
	      commited = commitTransaction();
	    } finally {
	      if(!commited) {
	        rollbackTransaction();
	      }
	    }
	    return mtable_statistics;	  
  }
  
  public tdw_sys_table_statistics get_table_statistics(String table_statistics_name) throws MetaException{
	    boolean commited = false;
	    tdw_sys_table_statistics  table_statistics = null;
	    try {
	      openTransaction();
	      table_statistics = convertToTable_statistics(get_Mtable_statistics(table_statistics_name));
	      commited = commitTransaction();
	    } finally {
	      if(!commited) {
	        rollbackTransaction();
	      }
	    }
	    return table_statistics;	  
  }

  private List<Mtdw_sys_table_statistics> listMtable_statistics(int max) {
	    boolean success = false;
	    List<Mtdw_sys_table_statistics> mtable_statisticses = null;
	    try {
	      openTransaction();
	      LOG.debug("Executing listMtable_statistics");
	      Query query = pm.newQuery(Mtdw_sys_table_statistics.class, ""); 
	      mtable_statisticses = (List<Mtdw_sys_table_statistics>) query.execute(); 
	      LOG.debug("Done executing query for listMtable_statistics");
	      pm.retrieveAll(mtable_statisticses);
	      success = commitTransaction();
	      LOG.debug("Done retrieving all objects for listMtable_statistics");
	    } finally {
	      if(!success) {
	        rollbackTransaction();
	      }
	    }
	    return mtable_statisticses;
	  }
  
  public List<tdw_sys_table_statistics> get_table_statistics_multi(int max) throws MetaException{
	    this.openTransaction();
	    List<tdw_sys_table_statistics> table_statisticses = convertToTable_statisticses(this.listMtable_statistics(max));
	    this.commitTransaction();
	    return table_statisticses;
  }

  public List<String> get_table_statistics_names(int max) throws MetaException{
	    List<String> statistics_names = new ArrayList<String>();
	    boolean success = false;
	    try {
	      openTransaction();
	      LOG.debug("Executing get_table_statistics_names");
	      Query q = pm.newQuery("select stat_table_name from org.apache.hadoop.hive.metastore.model.Mtdw_sys_table_statistics order by stat_table_name asc");
	      q.setResult("stat_table_name");
	      Collection names = (Collection) q.execute();
	      statistics_names = new ArrayList<String>(); 
	      for (Iterator i = names.iterator (); i.hasNext ();) {
	    	  statistics_names.add((String) i.next ()); 
	      }
	      success = commitTransaction();
	    } finally {
	      if(!success) {
	        rollbackTransaction();
	      }
	    }
	    return statistics_names;	  
  }

  private tdw_sys_fields_statistics convertToFields_statistics(Mtdw_sys_fields_statistics mfields_statistics) throws MetaException {
	    if(mfields_statistics == null) {
	      return null;
	    }
	    return new tdw_sys_fields_statistics(
	    		mfields_statistics.getStat_table_name(),
	    		mfields_statistics.getStat_field_name(),
	    		mfields_statistics.getStat_nullfac(),
	    		mfields_statistics.getStat_avg_field_width(),
	    		mfields_statistics.getStat_distinct_values(),
	    		mfields_statistics.getStat_values_1(),
	    		mfields_statistics.getStat_numbers_1(),
	    		mfields_statistics.getStat_values_2(),
	    		mfields_statistics.getStat_numbers_2(),
	    		mfields_statistics.getStat_values_3(),
	    		mfields_statistics.getStat_numbers_3(),
	    		mfields_statistics.getStat_number_1_type(),
	    		mfields_statistics.getStat_number_2_type(),
	    		mfields_statistics.getStat_number_3_type());

}

private Mtdw_sys_fields_statistics convertToMFields_statistics(tdw_sys_fields_statistics fields_statistics) throws MetaException {
	    if(fields_statistics == null) {
	      return null;
	    }
	    return new Mtdw_sys_fields_statistics(
	    		fields_statistics.getStat_table_name(),
	    		fields_statistics.getStat_field_name(),
	    		fields_statistics.getStat_nullfac(),
	    		fields_statistics.getStat_avg_field_width(),
	    		fields_statistics.getStat_distinct_values(),
	    		fields_statistics.getStat_values_1(),
	    		fields_statistics.getStat_numbers_1(),
	    		fields_statistics.getStat_values_2(),
	    		fields_statistics.getStat_numbers_2(),
	    		fields_statistics.getStat_values_3(),
	    		fields_statistics.getStat_numbers_3(),
	    		fields_statistics.getStat_number_1_type(),
	    		fields_statistics.getStat_number_2_type(),
	    		fields_statistics.getStat_number_3_type());

}

private List<tdw_sys_fields_statistics> convertToFields_statisticses(List<Mtdw_sys_fields_statistics> mfields_statisticses) throws MetaException {
	    List<tdw_sys_fields_statistics> fields_statisticses = new ArrayList<tdw_sys_fields_statistics>(mfields_statisticses.size());
	    for (Mtdw_sys_fields_statistics mp : mfields_statisticses) {
	    	fields_statisticses.add(this.convertToFields_statistics(mp));
	    }
	    return fields_statisticses;
	  }

public boolean add_fields_statistics(tdw_sys_fields_statistics new_fields_statistics) throws  MetaException{
	    boolean success = false;
	    boolean commited = false;
	    try {
	      openTransaction();
	      Mtdw_sys_fields_statistics mfields_statistics = convertToMFields_statistics(new_fields_statistics);
	      pm.makePersistent(mfields_statistics);
	      commited = commitTransaction();
	      success = true;
	    } finally {
	      if(!commited) {
	        rollbackTransaction();
	      }
	    }
	    return success;
}

public boolean delete_fields_statistics(String table_statistics_name,String fields_statistics_name)  throws MetaException{
	    boolean success = false;
	    try {
	      openTransaction();
	      Mtdw_sys_fields_statistics mfields_statistics = this.get_MFields_statistics(table_statistics_name,fields_statistics_name); 
	      if(mfields_statistics != null)
	        pm.deletePersistent(mfields_statistics);
	      success = commitTransaction();
	    } finally {
	      if(!success) {
	        rollbackTransaction();
	      }
	    }
	    return success;
}

public Mtdw_sys_fields_statistics get_MFields_statistics(String table_statistics_name,String fields_statistics_name) throws MetaException{
	    
	    Mtdw_sys_fields_statistics mfields_statistics = null;
	    boolean commited = false;
	    try {
	      openTransaction();
	      table_statistics_name = table_statistics_name.toLowerCase();
	      fields_statistics_name = fields_statistics_name.toLowerCase();	      
	      Query query = pm.newQuery(Mtdw_sys_fields_statistics.class, "stat_table_name == table_statistics_name && stat_field_name == fields_statistics_name"); 
	      query.declareParameters("java.lang.String table_statistics_name,java.lang.String fields_statistics_name"); 
	      query.setUnique(true); 
	      mfields_statistics = (Mtdw_sys_fields_statistics) query.execute(table_statistics_name.trim(),fields_statistics_name.trim()); 
	      pm.retrieve(mfields_statistics);
	      commited = commitTransaction();
	    } finally {
	      if(!commited) {
	        rollbackTransaction();
	      }
	    }
	    return mfields_statistics;	  
}

public tdw_sys_fields_statistics get_fields_statistics(String table_statistics_name,String fields_statistics_name) throws MetaException{
	    boolean commited = false;
	    tdw_sys_fields_statistics  fields_statistics = null;
	    try {
	      openTransaction();
	      fields_statistics = convertToFields_statistics(get_MFields_statistics(table_statistics_name,fields_statistics_name));
	      commited = commitTransaction();
	    } finally {
	      if(!commited) {
	        rollbackTransaction();
	      }
	    }
	    return fields_statistics;	  
}

private List<Mtdw_sys_fields_statistics> listMfields_statistics(String table_statistics_name,int max) {
	    boolean success = false;
	    List<Mtdw_sys_fields_statistics> mfields_statisticses = null;
	    try {
	      openTransaction();
	      LOG.debug("Executing listMfields_statistics");
	      if(table_statistics_name.trim().equals("*"))
	      {
		      Query query = pm.newQuery(Mtdw_sys_fields_statistics.class, ""); 
		      mfields_statisticses = (List<Mtdw_sys_fields_statistics>) query.execute(); 
	      }
	      else
	      {
		      Query query = pm.newQuery(Mtdw_sys_fields_statistics.class, "stat_table_name == table_statistics_name"); 
		      query.declareParameters("java.lang.String table_statistics_name"); 
		      mfields_statisticses = (List<Mtdw_sys_fields_statistics>) query.execute(table_statistics_name.trim()); 	    	  
	      }

	      LOG.debug("Done executing query for listMfields_statistics");
	      pm.retrieveAll(mfields_statisticses);
	      success = commitTransaction();
	      LOG.debug("Done retrieving all objects for listMfields_statistics");
	    } finally {
	      if(!success) {
	        rollbackTransaction();
	      }
	    }
	    return mfields_statisticses;
	  }

public List<tdw_sys_fields_statistics> get_fields_statistics_multi(String table_statistics_name,int max) throws MetaException{
	    this.openTransaction();
	    List<tdw_sys_fields_statistics> fields_statisticses = convertToFields_statisticses(this.listMfields_statistics(table_statistics_name,max));
	    this.commitTransaction();
	    return fields_statisticses;
}

public List<String> get_fields_statistics_names(String table_statistics_name,int max) throws MetaException{
	    List<String> statistics_names = new ArrayList<String>();
	    boolean success = false;
	    try {
	      openTransaction();
	      LOG.debug("Executing get_fields_statistics_names");
	      Query q = pm.newQuery("select stat_field_name from org.apache.hadoop.hive.metastore.model.Mtdw_sys_fields_statistics where stat_table_name == t1 order by stat_field_name asc");
	      q.declareParameters("java.lang.String t1");
	      q.setResult("stat_field_name");
	      Collection names = (Collection) q.execute(table_statistics_name.trim());
	      statistics_names = new ArrayList<String>(); 
	      for (Iterator i = names.iterator (); i.hasNext ();) {
	    	  statistics_names.add((String) i.next ()); 
	      }
	      success = commitTransaction();
	    } finally {
	      if(!success) {
	        rollbackTransaction();
	      }
	    }
	    return statistics_names;	  
}

  // joeyli added for statics information collection end


//added by BrantZhang for authorization begin
  
  private MRole getMRole(String role) {
	    MRole mrole = null;
	    boolean commited = false;
	    try {
	      openTransaction();
	      Query query = pm.newQuery(MRole.class, "roleName == role"); 
	      query.declareParameters("java.lang.String role"); 
	      query.setUnique(true); 
	      mrole = (MRole) query.execute(role); 
	      if(mrole!= null)
	    	  pm.retrieve(mrole);
	      commited = commitTransaction();
	    } finally {
	      if(!commited) {
	        rollbackTransaction();
	      }
	    }
	    return mrole;
	  }


  //create a role with no privilege
  public boolean createRole(String rolename) throws AlreadyExistsException {
	boolean success = false;
    boolean commited = false;
    MRole mrole = getMRole(rolename);
    
    if(mrole != null)
    	throw new AlreadyExistsException("The role already existed!");
    
    mrole = new MRole(rolename);
    try {
      openTransaction();
      pm.makePersistent(mrole);
      success = true;
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return success;
  }

  private MUser getMUser(String username) {
    MUser muser = null;
    boolean commited = false;
    try {
      openTransaction();
      Query query = pm.newQuery(MUser.class, "userName == username"); 
      query.declareParameters("java.lang.String username"); 
      query.setUnique(true); 
      muser = (MUser) query.execute(username); 
      if(muser!=null)
    	  pm.retrieve(muser);
      commited = commitTransaction();
    } finally {
      if(!commited) {
        rollbackTransaction();
      }
    }
    return muser;
  }

  	//create a user with no privilege
  	public boolean createUser(String username, String passwd) throws AlreadyExistsException{
  		boolean success = false;
  		boolean commited = false;
  		MUser muser = getMUser(username);
  	    
  		if(muser != null)
  			throw new AlreadyExistsException("The user already existed!");
    
  		muser = new MUser(username, passwd);
  		try {
  			openTransaction();
  			pm.makePersistent(muser);
  			success = true;
  			commited = commitTransaction();
  		} finally {
  			if(!commited) {
  				rollbackTransaction();
  			}
  		}
  		return success;
  	}

  	//drop all lines of the user in DbPriv table
  	public boolean dropAuthInDb(String who){
  		boolean commited = false;
  		List<MDbPriv> mdbps = null;
  		 
  		try {
  			openTransaction();
  			Query query = pm.newQuery(MDbPriv.class,"user == who");
  			query.declareParameters("java.lang.String who"); 
  			//query.setUnique(true);
  			mdbps = (List<MDbPriv>) query.execute(who); 
  			
  			if(mdbps == null) return true;
  			//pm.retrieveAll(mdbps);
  			pm.deletePersistentAll(mdbps);
  			
  			commited = commitTransaction();
  		} finally {
  			if(!commited) {
  				rollbackTransaction();
  			}
  		}
  		return true;
  	}

  	//drop all lines of the user in TblPriv table
  	public boolean dropAuthInTbl(String who){
  		boolean commited = false;
  		List<MTblPriv> mtblps = null;
  		 
  		try {
  			openTransaction();
  			Query query = pm.newQuery(MTblPriv.class,"user == who");
  			query.declareParameters("java.lang.String who"); 
  			//query.setUnique(true);
  			mtblps = (List<MTblPriv>) query.execute(who); 
  			
  			if(mtblps == null) return true;
  			//pm.retrieveAll(mtblps);
  			pm.deletePersistentAll(mtblps);
  			
  			commited = commitTransaction();
  		} finally {
  			if(!commited) {
  			rollbackTransaction();
  			}
  		}
  		return true;
  	}

    //drop auth line in DbPriv
	public boolean dropAuthOnDb(String who, String dbName){
		boolean success = false;
		MDbPriv mdbp = null;
		try {
			openTransaction();
			
			Query query = pm.newQuery(MDbPriv.class, "user == who && db.name == dbName"); 
		    query.declareParameters("java.lang.String who, java.lang.String dbName"); 
		    query.setUnique(true); 
		    mdbp = (MDbPriv) query.execute(who, dbName); 
		    if(mdbp != null){
		    	//pm.retrieve(mdbp);
		    	pm.deletePersistent(mdbp);
		    }
		    
		    success = commitTransaction();
		} finally {
			if (!success) {
				rollbackTransaction();
			}
		}
		return success;
	}

	//drop auth line in TblPriv
	public boolean dropAuthOnTbl(String who, String dbName, String tbl){
		boolean success = false;
		MTblPriv mtblp = null;
		
		try {
			openTransaction();
			
			Query query = pm.newQuery(MTblPriv.class, "user == who && db.name == dbName && table.tableName == tbl"); 
			query.declareParameters("java.lang.String who, java.lang.String dbName, java.lang.String tbl"); 
			query.setUnique(true); 
			mtblp = (MTblPriv) query.execute(who, dbName, tbl); 
			if(mtblp != null){
				//pm.retrieve(mtblp);
				pm.deletePersistent(mtblp);
			}
			
			success = commitTransaction();
		} finally {
			if (!success) {
				rollbackTransaction();
			}
		}
		return success;
	}

	//drop role and its privileges
	public boolean dropRole(String rolename) throws NoSuchObjectException {
	
		boolean success = false;
		boolean commited = false;
		 
		try {
			openTransaction();
			
			Query query = pm.newQuery(MRole.class, "roleName == rolename"); 
		    query.declareParameters("java.lang.String rolename"); 
		    query.setUnique(true); 
		    MRole  mrole = (MRole) query.execute(rolename); 
		    
			if(mrole == null)
				throw new NoSuchObjectException("The role doesn't exist:" + rolename);
			else pm.retrieve(mrole);
			
			//first drop table privileges of the role
			Query q1 = pm.newQuery(MTblPriv.class, "user == rolename");
			q1.declareParameters("java.lang.String rolename");
			List<MTblPriv> mtblp = (List<MTblPriv>) q1.execute(rolename);
			
			if(mtblp != null){
				//pm.retrieveAll(mtblp);
				pm.deletePersistentAll(mtblp);
			}
				
			//second drop db privileges of the role
			Query q2 = pm.newQuery(MDbPriv.class, "user == rolename");
			q2.declareParameters("java.lang.String rolename");
			List<MDbPriv> mdbp = (List<MDbPriv>) q2.execute(rolename);
			
			if(mdbp != null){
				//pm.retrieveAll(mdbp);
				pm.deletePersistentAll(mdbp);
			}
			
			//3rd Modifying all users playing role
		    LOG.info("Modifying all users playing role: " + rolename);
		    Query q3 = pm.newQuery(MUser.class);
		    List<MUser> musers = (List<MUser>) q3.execute();
		    List<MRole> play_roles;
		    for(MUser user: musers){
		    	play_roles = user.getPlay_roles();
		    	for(MRole role: play_roles){
		    		if(role.getRoleName().equals(rolename))
		    			play_roles.remove(role);
		    	}
		    	
		    }
		      
		    //4th Modifying all roles playing role
		    LOG.info("Modifying all roles playing role: " + rolename);
		    Query q4 = pm.newQuery(MRole.class);
		    List<MRole> mroles = (List<MRole>) q4.execute();
		    for(MRole m_role: mroles){
		    	play_roles = m_role.getPlay_roles();
		    	for(MRole role: play_roles){
		    		if(role.getRoleName().equals(rolename))
		    			play_roles.remove(role);
		    	}
		    }
		  	
		    //last drop the role
			pm.deletePersistent(mrole);
			//LOG.info("Dropping role along with all privileges in DbPriv table and TblPriv:" + rolename);
      
			commited = commitTransaction();
			success = true;
		} catch (JDOObjectNotFoundException e) {
			//LOG.debug("role not found " + rolename,e);
			commited = commitTransaction();
		} finally {
			if(!commited) {
				rollbackTransaction();
			}
		}
		return success;
	}

	//drop user and his privileges
	public boolean dropUser(String username) throws NoSuchObjectException {
		
		boolean success = false;
		boolean commited = false;
		
		try {
			openTransaction();
			
			Query query = pm.newQuery(MUser.class, "userName == username"); 
		    query.declareParameters("java.lang.String username"); 
		    query.setUnique(true); 
		    MUser muser = (MUser) query.execute(username); 
		    
			if(muser == null)
				throw new NoSuchObjectException("The user doesn't exist: " + username);
			else pm.retrieve(muser);
			
			//first drop the user
			pm.deletePersistent(muser);
			//LOG.info("Dropping user along with all privileges in DbPriv table and TblPriv:" + username);
      
			//then drop table privileges of the user
			Query q1 = pm.newQuery(MTblPriv.class, "user == username");
			q1.declareParameters("java.lang.String username");
			List<MTblPriv> mtblp = (List<MTblPriv>) q1.execute(username);
			
			if(mtblp != null){
				//pm.retrieveAll(mtblp);
				pm.deletePersistentAll(mtblp);
			}
				
			//last drop db privileges of the user
			Query q2 = pm.newQuery(MDbPriv.class, "user == username");
			q2.declareParameters("java.lang.String username");
			List<MDbPriv> mdbp = (List<MDbPriv>) q2.execute(username);
			
			if(mdbp != null){
				//pm.retrieveAll(mdbp);
				pm.deletePersistentAll(mdbp);
			}
			
			commited = commitTransaction();
			success = true;
		} catch (JDOObjectNotFoundException e) {
			//LOG.debug("user not found " + username,e);
			commited = commitTransaction();
		} /*catch (Throwable e) { 
			LOG.info(StringUtils.stringifyException(e));
			throw new RuntimeException(e) ;
		}*/finally {
			if(!commited) {
				rollbackTransaction();
			}
		}
		return success;
	}
	
	//get the db privilege of a user on some db
	public DbPriv getAuthOnDb(String who, String db){
		MDbPriv mdbp = getMDbPriv(who, db);
		
		if(mdbp == null)
			return null;
		return new DbPriv(db, who, mdbp.getSelect_priv(),
				mdbp.getInsert_priv(),mdbp.getIndex_priv(),mdbp.getCreate_priv(),
				mdbp.getDrop_priv(),mdbp.getDelete_priv(),mdbp.getAlter_priv(),
				mdbp.getUpdate_priv(),mdbp.getCreateview_priv(),mdbp.getShowview_priv());
	}
	
	//get the db privilege of a user on all dbs
	public List<DbPriv> getAuthOnDbs(String who){
		List<MDbPriv> mdbps = getMDbPrivs(who);
		List<DbPriv> dbps = new ArrayList<DbPriv>();
		
		if(mdbps == null)
			return null;
		for(MDbPriv mdbp: mdbps)
			dbps.add(new DbPriv(mdbp.getDb().getName(), who, mdbp.getSelect_priv(),
				mdbp.getInsert_priv(),mdbp.getIndex_priv(),mdbp.getCreate_priv(),
				mdbp.getDrop_priv(),mdbp.getDelete_priv(),mdbp.getAlter_priv(),
				mdbp.getUpdate_priv(),mdbp.getCreateview_priv(),mdbp.getShowview_priv()));
		
		return dbps;
	}

	//get the table privileges of a user on some table
	public TblPriv getAuthOnTbl(String who, String db, String tbl){
		MTblPriv mtblp = getMTblPriv(who, db, tbl);
		
		if(mtblp == null)
			return null;
		return new TblPriv(db, tbl, who, mtblp.getSelect_priv(),
				mtblp.getInsert_priv(),mtblp.getIndex_priv(),mtblp.getCreate_priv(),
				mtblp.getDrop_priv(),mtblp.getDelete_priv(),mtblp.getAlter_priv(),
				mtblp.getUpdate_priv());
	}
	
	//get the table privileges of a user on all tables
	public List<TblPriv> getAuthOnTbls(String who){
		List<MTblPriv> mtblps = getMTblPrivs(who);
		List<TblPriv> tblps = new ArrayList<TblPriv>();
		
		if(mtblps == null)
			return null;
		for(MTblPriv mtblp: mtblps)
			tblps.add(new TblPriv(mtblp.getDb().getName(), mtblp.getTable().getTableName(), who, mtblp.getSelect_priv(),
					mtblp.getInsert_priv(),mtblp.getIndex_priv(),mtblp.getCreate_priv(),
					mtblp.getDrop_priv(),mtblp.getDelete_priv(),mtblp.getAlter_priv(),
					mtblp.getUpdate_priv()));
		return tblps;
	}

	//get the privileges on a db
	public List<DbPriv> getDbAuth(String dbName){
		 boolean commited = false;
		 List<MDbPriv> mdbps = null;
		 List<DbPriv> dbps = new ArrayList();
		 
		 try {
		      openTransaction();
		      Query query = pm.newQuery(MDbPriv.class,"db.name == dbName");
		      query.declareParameters("java.lang.String dbName"); 
		      //query.setUnique(true);
		      mdbps = (List<MDbPriv>) query.execute(dbName); 
		      if(mdbps != null)
		    	  pm.retrieveAll(mdbps);
		      commited = commitTransaction();
		    } finally {
		      if(!commited) {
		        rollbackTransaction();
		      }
		    }
		  if(mdbps == null) return null;
		  for(MDbPriv mdbp: mdbps){
			  dbps.add(new DbPriv(dbName, mdbp.getUser(), mdbp.getSelect_priv(),
				mdbp.getInsert_priv(),mdbp.getIndex_priv(),mdbp.getCreate_priv(),
				mdbp.getDrop_priv(),mdbp.getDelete_priv(),mdbp.getAlter_priv(),
				mdbp.getUpdate_priv(),mdbp.getCreateview_priv(),mdbp.getShowview_priv()));
		  }
		  return dbps;
		    		 
	}

	//get all lines in DbPriv table
	public List<DbPriv> getDbAuthAll(){
		List<DbPriv>  dbps = null;
		List<MDbPriv> mdbps = null;
	    boolean commited = false;
	    try {
	      openTransaction();
	      Query query = pm.newQuery(MDbPriv.class);
	      
	      mdbps = (List<MDbPriv>) query.execute();
	      commited = commitTransaction();
	    } finally {
	      if(!commited) {
	        rollbackTransaction();
	      }
	    }
	    if(mdbps == null) return null;
		for(MDbPriv mdbp: mdbps){
			dbps.add(new DbPriv(mdbp.getDb().getName(), mdbp.getUser(), mdbp.getSelect_priv(),
			mdbp.getInsert_priv(),mdbp.getIndex_priv(),mdbp.getCreate_priv(),
			mdbp.getDrop_priv(),mdbp.getDelete_priv(),mdbp.getAlter_priv(),
			mdbp.getUpdate_priv(),mdbp.getCreateview_priv(),mdbp.getShowview_priv()));
		}
		return dbps;
	}

	private Role convertToRole(MRole mrole){
		List<MRole> mrl = mrole.getPlay_roles();
		List<String> roles = new ArrayList<String>();
		for(MRole mr: mrl)
			roles.add(mr.getRoleName());
		 
		return new Role(mrole.getRoleName(), roles, mrole.getSelect_priv(),
				mrole.getInsert_priv(),mrole.getIndex_priv(),mrole.getCreate_priv(),
				mrole.getDrop_priv(),mrole.getDelete_priv(),mrole.getAlter_priv(),
				mrole.getUpdate_priv(),mrole.getCreateview_priv(),mrole.getShowview_priv(),
				mrole.getDba_priv());
		
	}
	
	//get a role
	public Role getRole(String roleName) throws NoSuchObjectException{
		MRole mrole = getMRole(roleName);
		
		if(mrole == null)
			throw new NoSuchObjectException("Role does not exist: " + roleName);;
		return convertToRole(mrole);
	}

	//get all roles' names
	public List<String> getRolesAll() {
		List<String> roles = null;
	    boolean commited = false;
	    try {
	      openTransaction();
	      Query query = pm.newQuery(MRole.class);
	      query.setResult("roleName");
	      query.setResultClass(String.class);
	      query.setOrdering("roleName asc");
	      roles = (List) query.execute();
	      commited = commitTransaction();
	    } finally {
	      if(!commited) {
	        rollbackTransaction();
	      }
	    }
	    return roles;
	}

	//return all users' privileges on some table
	public List<TblPriv> getTblAuth(String dbName, String tbl){
		 boolean commited = false;
		 List<MTblPriv> mtblps = null;
		 List<TblPriv> tblps = new ArrayList();
		 
		 try {
		      openTransaction();
		      Query query = pm.newQuery(MTblPriv.class,"db.name == dbName && table.tableName == tbl");
		      query.declareParameters("java.lang.String dbName, java.lang.String tbl"); 
		      //query.setUnique(true);
		      mtblps = (List<MTblPriv>) query.execute(dbName, tbl); 
		      if(mtblps != null)
		    	  pm.retrieveAll(mtblps);
		      commited = commitTransaction();
		    } finally {
		      if(!commited) {
		        rollbackTransaction();
		      }
		    }
		  if(mtblps == null) return null;
		  for(MTblPriv mtblp: mtblps){
			  tblps.add(new TblPriv(dbName, tbl, mtblp.getUser(), mtblp.getSelect_priv(),
					  mtblp.getInsert_priv(),mtblp.getIndex_priv(),mtblp.getCreate_priv(),
					  mtblp.getDrop_priv(),mtblp.getDelete_priv(),mtblp.getAlter_priv(),
					  mtblp.getUpdate_priv()));
		  }
		  return tblps;
	}

	//get all user and roles' privileges on tables
	public List<TblPriv> getTblAuthAll(){
		List<TblPriv>  tblps = null;
		List<MTblPriv> mtblps = null;
	    boolean commited = false;
	    try {
	      openTransaction();
	      Query query = pm.newQuery(MTblPriv.class);
	      
	      mtblps = (List<MTblPriv>) query.execute();
	      commited = commitTransaction();
	    } finally {
	      if(!commited) {
	        rollbackTransaction();
	      }
	    }
	    if(mtblps == null) return null;
		for(MTblPriv mtblp: mtblps){
			tblps.add(new TblPriv(mtblp.getDb().getName(), mtblp.getTable().getTableName(), 
					mtblp.getUser(), mtblp.getSelect_priv(),
					mtblp.getInsert_priv(),mtblp.getIndex_priv(),mtblp.getCreate_priv(),
					mtblp.getDrop_priv(),mtblp.getDelete_priv(),mtblp.getAlter_priv(),
					mtblp.getUpdate_priv()));
		}
		return tblps;
	}

	private User convertToUser(MUser muser){
		List<MRole> mrl = muser.getPlay_roles();
		List<String> roles = new ArrayList<String>();
		for(MRole mr: mrl)
			roles.add(mr.getRoleName());
		 
		return new User(muser.getUserName(), roles, muser.getSelect_priv(),
				muser.getInsert_priv(),muser.getIndex_priv(),muser.getCreate_priv(),
				muser.getDrop_priv(),muser.getDelete_priv(),muser.getAlter_priv(),
				muser.getUpdate_priv(),muser.getCreateview_priv(),muser.getShowview_priv(),
				muser.getDba_priv());
		
	}
	
	//get the user
	public User getUser(String username) throws NoSuchObjectException {
		MUser muser = getMUser(username);
		
		if(muser == null)
			throw new NoSuchObjectException("User does not exist: " + username);
		return convertToUser(muser);
		
	}

	//list all users's names
	public List<String> getUsersAll() {
		List<String> users = null;
	    boolean commited = false;
	    try {
	      openTransaction();
	      Query query = pm.newQuery(MUser.class);
	      query.setResult("userName");
	      query.setResultClass(String.class);
	      query.setOrdering("userName asc");
	      users = (List) query.execute();
	      commited = commitTransaction();
	    } finally {
	      if(!commited) {
	        rollbackTransaction();
	      }
	    }
	    return users;
	}

	//get the privileges of a user or role on db
	private MDbPriv getMDbPriv(String who, String dbName) {
	    MDbPriv mdbp = null;
	    boolean commited = false;
	    try {
	      openTransaction();
	      
	      Query query = pm.newQuery(MDbPriv.class, "user == who && db.name == dbName"); 
	      query.declareParameters("java.lang.String who, java.lang.String dbName"); 
	      query.setUnique(true); 
	      mdbp = (MDbPriv) query.execute(who, dbName); 
	      if(mdbp != null)
	    	  pm.retrieve(mdbp);
	      commited = commitTransaction();
	    }/*catch (Throwable e) { 
			LOG.info(StringUtils.stringifyException(e));
			throw new RuntimeException(e) ;
		}*/ finally {
	      if(!commited) {
	        rollbackTransaction();
	      }
	    }
	    return mdbp;
	  }
	
	//get the privileges of a user or role on all dbs
	private List<MDbPriv> getMDbPrivs(String who) {
	    List<MDbPriv> mdbps = null;
	    boolean commited = false;
	    try {
	      openTransaction();
	      
	      Query query = pm.newQuery(MDbPriv.class, "user == who"); 
	      query.declareParameters("java.lang.String who"); 
	      //query.setUnique(true); 
	      try{
	    	  mdbps = (List<MDbPriv>) query.execute(who);
	      }catch(Exception e){
	    	  MDbPriv mdbp = (MDbPriv) query.execute(who);
	    	  mdbps = new ArrayList<MDbPriv>();
	    	  mdbps.add(mdbp);
	      }
	      	      
	      if(mdbps!=null)
	    	  pm.retrieveAll(mdbps);
	      commited = commitTransaction();
	    }/*catch (Throwable e) { 
			LOG.info(StringUtils.stringifyException(e));
			throw new RuntimeException(e) ;
	    }*/finally {
	      if(!commited) {
	        rollbackTransaction();
	      }
	    }
	    return mdbps;
	  }
	
	//create a user or role with no privilege in DbPriv table
	  private MDbPriv createMDbPriv(String who, String db) throws NoSuchObjectException {
		boolean commited = false;
	    
	    MDatabase mdb = getMDatabase(db);
	    if(mdb == null){
			throw new NoSuchObjectException("Db does not exist: " + db);
		}
	    	    
	    MDbPriv mdbp = new MDbPriv(mdb, who);
	    try {
	      openTransaction();
	      pm.makePersistent(mdbp);
	      commited = commitTransaction();
	    } finally {
	      if(!commited) {
	        rollbackTransaction();
	        mdbp = null;
	      }
	    }
	    return mdbp;
	  }
	
	//grant privileges for user or role on db in DbPriv table
	public boolean grantAuthOnDb(String who, List<String> privileges,
		String dbName) throws NoSuchObjectException, InvalidObjectException {
		
		boolean success = false;
		try {
			openTransaction();
			Query query = pm.newQuery(MDbPriv.class, "user == who && db.name == dbName"); 
		    query.declareParameters("java.lang.String who, java.lang.String dbName"); 
		    query.setUnique(true); 
		    MDbPriv mdbp = (MDbPriv) query.execute(who, dbName); 
		     
			if(mdbp == null){
				try{
					mdbp = createMDbPriv(who, dbName);
				}catch(NoSuchObjectException e){
				throw e;
				}
				
				if(mdbp == null){
					return false;
				}
			}else
				pm.retrieve(mdbp);
			
			if(privileges == null)
				throw new InvalidObjectException("No privileges are given!");
			
			for(String priv: privileges){
				if(priv.equals("TOK_SELECT_PRI")) mdbp.setSelect_priv(true);
				else if(priv.equals("TOK_INSERT_PRI")) mdbp.setInsert_priv(true);
				else if(priv.equals("TOK_CREATE_PRI")) mdbp.setCreate_priv(true);
				else if(priv.equals("TOK_DROP_PRI")) mdbp.setDrop_priv(true);
				else if(priv.equals("TOK_DELETE_PRI")) mdbp.setDelete_priv(true);
				else if(priv.equals("TOK_ALTER_PRI"))  mdbp.setAlter_priv(true);
				else if(priv.equals("TOK_UPDATE_PRI")) mdbp.setUpdate_priv(true);
				else if(priv.equals("TOK_INDEX_PRI")) mdbp.setIndex_priv(true);
				else if(priv.equals("TOK_CREATEVIEW_PRI")) mdbp.setCreateview_priv(true);
				else if(priv.equals("TOK_SHOWVIEW_PRI")) mdbp.setShowview_priv(true);
				else if(priv.equals("TOK_ALL_PRI")){
					mdbp.setSelect_priv(true);
					mdbp.setInsert_priv(true);
					mdbp.setCreate_priv(true);
					mdbp.setDrop_priv(true);
					mdbp.setDelete_priv(true);
					mdbp.setAlter_priv(true);
					mdbp.setUpdate_priv(true);
					mdbp.setIndex_priv(true);
					mdbp.setCreateview_priv(true);
					mdbp.setShowview_priv(true);
				}
				else throw new InvalidObjectException("Privilege does not exist: " + priv);
			}
			  
			success = commitTransaction();
		} finally {
			if (!success) {
				rollbackTransaction();
			}
		}
		return success;
	}
	
	//get user or role's privileges on some table
	private MTblPriv getMTblPriv(String who, String dbName, String tbl) {
	    MTblPriv mtblp = null;
	    boolean commited = false;
	    try {
	      openTransaction();
	      
	      Query query = pm.newQuery(MTblPriv.class, "user == who && db.name == dbName && table.tableName == tbl"); 
	      query.declareParameters("java.lang.String who, java.lang.String dbName, java.lang.String tbl"); 
	      query.setUnique(true); 
	      mtblp = (MTblPriv) query.execute(who, dbName, tbl); 
	      if(mtblp != null)
	    	  pm.retrieve(mtblp);
	      commited = commitTransaction();
	    } finally {
	      if(!commited) {
	        rollbackTransaction();
	      }
	    }
	    return mtblp;
	  }
	
	//get user or role's privileges on all tables
	private List<MTblPriv> getMTblPrivs(String who) {
	    List<MTblPriv> mtblps = null;
	    boolean commited = false;
	    try {
	      openTransaction();
	      
	      Query query = pm.newQuery(MTblPriv.class, "user == who"); 
	      query.declareParameters("java.lang.String who"); 
	      //query.setUnique(true); 
	      try{
	    	  mtblps = (List<MTblPriv>) query.execute(who); 
	      }catch(Exception e){
	    	  MTblPriv mt = (MTblPriv) query.execute(who);
	    	  mtblps = new ArrayList<MTblPriv>();
	    	  mtblps.add(mt);
	      }
	      
	      if(mtblps!=null)
	    	  pm.retrieveAll(mtblps);
	      commited = commitTransaction();
	    } finally {
	      if(!commited) {
	        rollbackTransaction();
	      }
	    }
	    return mtblps;
	  }

	//create a user with no privilege
	  private MTblPriv createMTblPriv(String username, String db, String tbl) throws NoSuchObjectException {
		boolean success = false;
	    boolean commited = false;
	    
	    MDatabase mdb = getMDatabase(db);
	    if(mdb == null){
			throw new NoSuchObjectException("Db does not exist: " + db);
		}
	    
	    MTable mtbl = getMTable(db, tbl);
	    if(mtbl == null){
			throw new NoSuchObjectException("Table does not exist: " + tbl +" in db: " + db);
		}
	    	    
	    MTblPriv mtblp = new MTblPriv(username, mdb, mtbl);
	    try {
	      openTransaction();
	      pm.makePersistent(mtblp);
	      success = true;
	      commited = commitTransaction();
	    } finally {
	      if(!commited) {
	        rollbackTransaction();
	        mtblp = null;
	      }
	    }
	    return mtblp;
	  }
	
	//grant privileges on table for the user or role
	public boolean grantAuthOnTbl(String who, List<String> privileges,
		String dbName, String tbl) throws NoSuchObjectException, InvalidObjectException {
		
		boolean success = false;
		try {
			openTransaction();
			
			Query query = pm.newQuery(MTblPriv.class, "user == who && db.name == dbName && table.tableName == tbl"); 
		    query.declareParameters("java.lang.String who, java.lang.String dbName, java.lang.String tbl"); 
		    query.setUnique(true); 
		    MTblPriv mtblp = (MTblPriv) query.execute(who, dbName, tbl); 
		      
		    if(mtblp == null){
				try{
					mtblp = createMTblPriv(who, dbName, tbl);
					if(mtblp == null) return false;
				}catch(NoSuchObjectException e){
					throw e;
				}
			}else
				pm.retrieve(mtblp);
			
			if(privileges == null)
				throw new InvalidObjectException("No privileges are given!");
			
			for(String priv: privileges){
				if(priv.equals("TOK_SELECT_PRI")) mtblp.setSelect_priv(true);
				else if(priv.equals("TOK_INSERT_PRI")) mtblp.setInsert_priv(true);
				else if(priv.equals("TOK_CREATE_PRI")) mtblp.setCreate_priv(true);
				else if(priv.equals("TOK_DROP_PRI")) mtblp.setDrop_priv(true);
				else if(priv.equals("TOK_DELETE_PRI")) mtblp.setDelete_priv(true);
				else if(priv.equals("TOK_ALTER_PRI"))  mtblp.setAlter_priv(true);
				else if(priv.equals("TOK_UPDATE_PRI")) mtblp.setUpdate_priv(true);
				else if(priv.equals("TOK_INDEX_PRI")) mtblp.setIndex_priv(true);
				else if(priv.equals("TOK_ALL_PRI")){
					mtblp.setSelect_priv(true);
					mtblp.setInsert_priv(true);
					mtblp.setCreate_priv(true);
					mtblp.setDrop_priv(true);
					mtblp.setDelete_priv(true);
					mtblp.setAlter_priv(true);
					mtblp.setUpdate_priv(true);
					mtblp.setIndex_priv(true);
				}
				else throw new InvalidObjectException("Privilege does not exist: " + priv);
			}
			      
			success = commitTransaction();
		} finally {
			if (!success) {
				rollbackTransaction();
			}
		}
		return success;
	}

	//grant role sys privileges in role table
	public boolean grantAuthRoleSys(String rolename, List<String> privileges)
	 throws NoSuchObjectException, InvalidObjectException {
		
		boolean success = false;
		try {
			openTransaction();
			Query query = pm.newQuery(MRole.class, "roleName == rolename"); 
		    query.declareParameters("java.lang.String rolename"); 
		    query.setUnique(true); 
		    MRole mrole = (MRole) query.execute(rolename); 
		    
			if(mrole == null){
				throw new NoSuchObjectException("Role does not exist: " + rolename);
			}else
				pm.retrieve(mrole);
			
			if(privileges == null)
				throw new InvalidObjectException("No privileges are given!");
			
			for(String priv: privileges){
				if(priv.equals("TOK_SELECT_PRI")) mrole.setSelect_priv(true);
				else if(priv.equals("TOK_INSERT_PRI")) mrole.setInsert_priv(true);
				else if(priv.equals("TOK_CREATE_PRI")) mrole.setCreate_priv(true);
				else if(priv.equals("TOK_DROP_PRI")) mrole.setDrop_priv(true);
				else if(priv.equals("TOK_DELETE_PRI")) mrole.setDelete_priv(true);
				else if(priv.equals("TOK_ALTER_PRI"))  mrole.setAlter_priv(true);
				else if(priv.equals("TOK_UPDATE_PRI")) mrole.setUpdate_priv(true);
				else if(priv.equals("TOK_INDEX_PRI")) mrole.setIndex_priv(true);
				else if(priv.equals("TOK_CREATEVIEW_PRI")) mrole.setCreateview_priv(true);
				else if(priv.equals("TOK_SHOWVIEW_PRI")) mrole.setShowview_priv(true);
				else if(priv.equals("TOK_DBA_PRI")) mrole.setDba_priv(true);
				else if(priv.equals("TOK_ALL_PRI")){
					mrole.setSelect_priv(true);
					mrole.setInsert_priv(true);
					mrole.setCreate_priv(true);
					mrole.setDrop_priv(true);
					mrole.setDelete_priv(true);
					mrole.setAlter_priv(true);
					mrole.setUpdate_priv(true);
					mrole.setIndex_priv(true);
					mrole.setCreateview_priv(true);
					mrole.setShowview_priv(true);
				}
				else throw new InvalidObjectException("Privilege does not exist: " + priv);
			}
			      
			success = commitTransaction();
		} finally {
			if (!success) {
				rollbackTransaction();
			}
		}
		return success;
	}

	//grant user sys privileges in user table
	public boolean grantAuthSys(String username, List<String> privileges)
		throws NoSuchObjectException, InvalidObjectException {
		
		boolean success = false;
		try {
			openTransaction();
			
			Query query = pm.newQuery(MUser.class, "userName == username"); 
		    query.declareParameters("java.lang.String username"); 
		    query.setUnique(true); 
		    MUser  muser = (MUser) query.execute(username); 
		    
			
			if(muser == null){
				throw new NoSuchObjectException("User does not exist: " + username);
			}else
				pm.retrieve(muser);
			
			for(String priv: privileges){
				if(priv.equals("TOK_SELECT_PRI")) muser.setSelect_priv(true);
				else if(priv.equals("TOK_INSERT_PRI")) muser.setInsert_priv(true);
				else if(priv.equals("TOK_CREATE_PRI")) muser.setCreate_priv(true);
				else if(priv.equals("TOK_DROP_PRI")) muser.setDrop_priv(true);
				else if(priv.equals("TOK_DELETE_PRI")) muser.setDelete_priv(true);
				else if(priv.equals("TOK_ALTER_PRI"))  muser.setAlter_priv(true);
				else if(priv.equals("TOK_UPDATE_PRI")) muser.setUpdate_priv(true);
				else if(priv.equals("TOK_INDEX_PRI")) muser.setIndex_priv(true);
				else if(priv.equals("TOK_CREATEVIEW_PRI")) muser.setCreateview_priv(true);
				else if(priv.equals("TOK_SHOWVIEW_PRI")) muser.setShowview_priv(true);
				else if(priv.equals("TOK_ALL_PRI")){
					muser.setSelect_priv(true);
					muser.setInsert_priv(true);
					muser.setCreate_priv(true);
					muser.setDrop_priv(true);
					muser.setDelete_priv(true);
					muser.setAlter_priv(true);
					muser.setUpdate_priv(true);
					muser.setIndex_priv(true);
					muser.setCreateview_priv(true);
					muser.setShowview_priv(true);
				}
				else if(priv.equals("TOK_DBA_PRI")) {
					muser.setDba_priv(true);
				}
				else throw new InvalidObjectException("Privilege does not exist: " + priv);
			}
			      
			success = commitTransaction();
		} finally {
			if (!success) {
				rollbackTransaction();
			}
		}
		return success;
	}

	//grant roles to role
	public boolean grantRoleToRole(String rolename, List<String> roles)
		throws NoSuchObjectException, InvalidObjectException {
		
		boolean success = false;
		try {
			openTransaction();
			
			Query query = pm.newQuery(MRole.class, "roleName == rolename"); 
		    query.declareParameters("java.lang.String rolename"); 
		    query.setUnique(true); 
		    MRole  mrole = (MRole) query.execute(rolename); 
		    
			if(mrole == null){
				throw new NoSuchObjectException("Role does not exist: " + rolename);
			}else
				pm.retrieve(mrole);
			
			List<MRole> mroles =  mrole.getPlay_roles();
			MRole mr;
			for(String role: roles){
				mr = getMRole(role);
				if(mr != null){
					if(!mroles.contains(mr))
						mroles.add(mr);
				}else
					throw new InvalidObjectException("Role does not exist: " + role);
			}
			      
			success = commitTransaction();
		} finally {
			if (!success) {
				rollbackTransaction();
			}
		}
		return success;
	}

	//grant roles to user
	public boolean grantRoleToUser(String username, List<String> roles)
		throws NoSuchObjectException, InvalidObjectException {
		
		boolean success = false;
		try {
			openTransaction();
			Query query = pm.newQuery(MUser.class, "userName == username"); 
		    query.declareParameters("java.lang.String username"); 
		    query.setUnique(true); 
		    MUser  muser = (MUser) query.execute(username); 
		   
			if(muser == null){
				throw new NoSuchObjectException("User does not exist: " + username);
			}else
				 pm.retrieve(muser);
			
			List<MRole> mroles = muser.getPlay_roles();
			MRole mrole;
			for(String role: roles){
				mrole = getMRole(role);
				if(mrole != null){
					if(!mroles.contains(mrole))
						mroles.add(mrole);
				}else
					throw new InvalidObjectException("Role does not exist: " + role);
			}
			      
			success = commitTransaction();
		} finally {
			if (!success) {
				rollbackTransaction();
			}
		}
		return success;
	}

	//verify a role
	public boolean isARole(String role) {
		MRole mrole = getMRole(role);
		
		if(mrole == null)
			return false;
		return true;
	}

	//verify a user
	public boolean isAUser(String username, String passwd) {
		MUser muser = getMUser(username);
		
		if(muser == null)
			return false;
		
		if(muser.getPasswd().equals(passwd)) return true;
		return false;
	}

	//revoke user's or role's privileges on db
	public boolean revokeAuthOnDb(String who, List<String> privileges, String dbName)
		throws NoSuchObjectException, InvalidObjectException {
		
		boolean success = false;
		MDbPriv mdbp = null;
		try {
			openTransaction();
			
			Query query = pm.newQuery(MDbPriv.class, "user == who && db.name == dbName"); 
		    query.declareParameters("java.lang.String who, java.lang.String dbName"); 
		    query.setUnique(true); 
		    mdbp = (MDbPriv) query.execute(who, dbName); 
		    
		    if(mdbp == null){
				throw new NoSuchObjectException("User " + who + " does not have privileges on db: " + dbName);
			}else
				pm.retrieve(mdbp);
			
			if(privileges == null)
				throw new InvalidObjectException("No privileges are given!");
		
			for(String priv: privileges){
				if(priv.equals("TOK_SELECT_PRI")) mdbp.setSelect_priv(false);
				else if(priv.equals("TOK_INSERT_PRI")) mdbp.setInsert_priv(false);
				else if(priv.equals("TOK_CREATE_PRI")) mdbp.setCreate_priv(false);
				else if(priv.equals("TOK_DROP_PRI")) mdbp.setDrop_priv(false);
				else if(priv.equals("TOK_DELETE_PRI")) mdbp.setDelete_priv(false);
				else if(priv.equals("TOK_ALTER_PRI"))  mdbp.setAlter_priv(false);
				else if(priv.equals("TOK_UPDATE_PRI")) mdbp.setUpdate_priv(false);
				else if(priv.equals("TOK_INDEX_PRI")) mdbp.setIndex_priv(false);
				else if(priv.equals("TOK_CREATEVIEW_PRI")) mdbp.setCreateview_priv(false);
				else if(priv.equals("TOK_SHOWVIEW_PRI")) mdbp.setShowview_priv(false);
				else if(priv.equals("TOK_ALL_PRI")){
					mdbp.setSelect_priv(false);
					mdbp.setInsert_priv(false);
					mdbp.setCreate_priv(false);
					mdbp.setDrop_priv(false);
					mdbp.setDelete_priv(false);
					mdbp.setAlter_priv(false);
					mdbp.setUpdate_priv(false);
					mdbp.setIndex_priv(false);
					mdbp.setCreateview_priv(false);
					mdbp.setShowview_priv(false);
				}
				else throw new InvalidObjectException("Privilege does not exist: " + priv);
			}
		    if(!mdbp.getSelect_priv() && !mdbp.getInsert_priv() && !mdbp.getCreate_priv() &&
		    		!mdbp.getDrop_priv() && !mdbp.getDelete_priv() && !mdbp.getAlter_priv() &&
		    		!mdbp.getUpdate_priv() && !mdbp.getIndex_priv() && !mdbp.getCreateview_priv()
		    		&& !mdbp.getShowview_priv()) 
		    	pm.deletePersistent(mdbp);
		    
			success = commitTransaction();
		} finally {
			if (!success) {
				rollbackTransaction();
			}
		}
		return success;
		}

	//revoke user's or role's privileges on table
	public boolean revokeAuthOnTbl(String who, List<String> privileges,
		String dbName, String tbl) throws NoSuchObjectException, InvalidObjectException {
		
		boolean success = false;
		MTblPriv mtblp = null;
		try {
			openTransaction();
			
			Query query = pm.newQuery(MTblPriv.class, "user == who && db.name == dbName && table.tableName == tbl"); 
		    query.declareParameters("java.lang.String who, java.lang.String dbName, java.lang.String tbl"); 
		    query.setUnique(true); 
		    mtblp = (MTblPriv) query.execute(who, dbName, tbl); 
		    
		    
			if(mtblp == null){
				throw new NoSuchObjectException("User " + who + " does not have privileges on table: " + tbl + " in db: " + dbName);
			}else
				pm.retrieve(mtblp);
			
			if(privileges == null)
				throw new InvalidObjectException("No privileges are given!");
		
			for(String priv: privileges){
				if(priv.equals("TOK_SELECT_PRI")) mtblp.setSelect_priv(false);
				else if(priv.equals("TOK_INSERT_PRI")) mtblp.setInsert_priv(false);
				else if(priv.equals("TOK_CREATE_PRI")) mtblp.setCreate_priv(false);
				else if(priv.equals("TOK_DROP_PRI")) mtblp.setDrop_priv(false);
				else if(priv.equals("TOK_DELETE_PRI")) mtblp.setDelete_priv(false);
				else if(priv.equals("TOK_ALTER_PRI"))  mtblp.setAlter_priv(false);
				else if(priv.equals("TOK_UPDATE_PRI")) mtblp.setUpdate_priv(false);
				else if(priv.equals("TOK_INDEX_PRI")) mtblp.setIndex_priv(false);
				else if(priv.equals("TOK_ALL_PRI")){
					mtblp.setSelect_priv(false);
					mtblp.setInsert_priv(false);
					mtblp.setCreate_priv(false);
					mtblp.setDrop_priv(false);
					mtblp.setDelete_priv(false);
					mtblp.setAlter_priv(false);
					mtblp.setUpdate_priv(false);
					mtblp.setIndex_priv(false);
				}
				else throw new InvalidObjectException("Privilege does not exist: " + priv);
			}
		    if(!mtblp.getSelect_priv() && !mtblp.getInsert_priv() && !mtblp.getCreate_priv() &&
		    		!mtblp.getDrop_priv() && !mtblp.getDelete_priv() && !mtblp.getAlter_priv() &&
		    		!mtblp.getUpdate_priv() && !mtblp.getIndex_priv()) 
		    	pm.deletePersistent(mtblp);
		    
			success = commitTransaction();
		} finally {
			if (!success) {
				rollbackTransaction();
			}
		}
		return success;
	}

	//revoke role's sys privileges from role table
	public boolean revokeAuthRoleSys(String rolename, List<String> privileges)
		throws NoSuchObjectException, InvalidObjectException {
		
		boolean success = false;
		try {
			openTransaction();
			Query query = pm.newQuery(MRole.class, "roleName == rolename"); 
		    query.declareParameters("java.lang.String rolename"); 
		    query.setUnique(true); 
		    MRole mrole = (MRole) query.execute(rolename); 
		    
			if(mrole == null){
				throw new NoSuchObjectException("Role does not exist: " + rolename);
			}else
				pm.retrieve(mrole);
			
			if(privileges == null)
				throw new InvalidObjectException("No privileges are given!");
			
			for(String priv: privileges){
				if(priv.equals("TOK_SELECT_PRI")) mrole.setSelect_priv(false);
				else if(priv.equals("TOK_INSERT_PRI")) mrole.setInsert_priv(false);
				else if(priv.equals("TOK_CREATE_PRI")) mrole.setCreate_priv(false);
				else if(priv.equals("TOK_DROP_PRI")) mrole.setDrop_priv(false);
				else if(priv.equals("TOK_DELETE_PRI")) mrole.setDelete_priv(false);
				else if(priv.equals("TOK_ALTER_PRI"))  mrole.setAlter_priv(false);
				else if(priv.equals("TOK_UPDATE_PRI")) mrole.setUpdate_priv(false);
				else if(priv.equals("TOK_INDEX_PRI")) mrole.setIndex_priv(false);
				else if(priv.equals("TOK_CREATEVIEW_PRI")) mrole.setCreateview_priv(false);
				else if(priv.equals("TOK_SHOWVIEW_PRI")) mrole.setShowview_priv(false);
				else if(priv.equals("TOK_DBA_PRI")) mrole.setDba_priv(false);
				else if(priv.equals("TOK_ALL_PRI")){
					mrole.setSelect_priv(false);
					mrole.setInsert_priv(false);
					mrole.setCreate_priv(false);
					mrole.setDrop_priv(false);
					mrole.setDelete_priv(false);
					mrole.setAlter_priv(false);
					mrole.setUpdate_priv(false);
					mrole.setIndex_priv(false);
					mrole.setCreateview_priv(false);
					mrole.setShowview_priv(false);
				}
				else throw new InvalidObjectException("Privilege does not exist: " + priv);
			}
			    
			success = commitTransaction();
		} finally {
			if (!success) {
				rollbackTransaction();
			}
		}
		return success;
	}

	//revoke user's sys privileges by editing user table
	public boolean revokeAuthSys(String username, List<String> privileges)
		throws NoSuchObjectException, InvalidObjectException {
		
		boolean success = false;
		try {
			openTransaction();
			Query query = pm.newQuery(MUser.class, "userName == username"); 
		    query.declareParameters("java.lang.String username"); 
		    query.setUnique(true); 
		    MUser  muser = (MUser) query.execute(username); 
		    
			if(muser == null){
				throw new NoSuchObjectException("User does not exist: " + username);
			}else
				pm.retrieve(muser);
			
			if(privileges == null)
				throw new InvalidObjectException("No privileges are given!");
			
			for(String priv: privileges){
				if(priv.equals("TOK_SELECT_PRI")) muser.setSelect_priv(false);
				else if(priv.equals("TOK_INSERT_PRI")) muser.setInsert_priv(false);
				else if(priv.equals("TOK_CREATE_PRI")) muser.setCreate_priv(false);
				else if(priv.equals("TOK_DROP_PRI")) muser.setDrop_priv(false);
				else if(priv.equals("TOK_DELETE_PRI")) muser.setDelete_priv(false);
				else if(priv.equals("TOK_ALTER_PRI"))  muser.setAlter_priv(false);
				else if(priv.equals("TOK_UPDATE_PRI")) muser.setUpdate_priv(false);
				else if(priv.equals("TOK_INDEX_PRI")) muser.setIndex_priv(false);
				else if(priv.equals("TOK_CREATEVIEW_PRI")) muser.setCreateview_priv(false);
				else if(priv.equals("TOK_SHOWVIEW_PRI")) muser.setShowview_priv(false);
				else if(priv.equals("TOK_DBA_PRI")) muser.setDba_priv(false);
				else if(priv.equals("TOK_ALL_PRI")){
					muser.setSelect_priv(false);
					muser.setInsert_priv(false);
					muser.setCreate_priv(false);
					muser.setDrop_priv(false);
					muser.setDelete_priv(false);
					muser.setAlter_priv(false);
					muser.setUpdate_priv(false);
					muser.setIndex_priv(false);
					muser.setCreateview_priv(false);
					muser.setShowview_priv(false);
				}
				else throw new InvalidObjectException("Privilege does not exist: " + priv);
			}
			      
			success = commitTransaction();
		} finally {
			if (!success) {
				rollbackTransaction();
			}
		}
		return success;
	}

	//revoke roles from role
	public boolean revokeRoleFromRole(String rolename, List<String> roles)
		throws NoSuchObjectException, InvalidObjectException {
		
		boolean success = false;
		try {
			openTransaction();
			Query query = pm.newQuery(MRole.class, "roleName == rolename"); 
		    query.declareParameters("java.lang.String rolename"); 
		    query.setUnique(true); 
		    MRole  mrole = (MRole) query.execute(rolename); 
		    
			if(mrole == null){
				throw new NoSuchObjectException("Role does not exist: " + rolename);
			}else
				pm.retrieve(mrole);
			
			List<MRole> mroles = mrole.getPlay_roles();
			MRole mr;
			for(String role: roles){
				mr = getMRole(role);
				if(mr != null){
					if(mroles.contains(mr))
						mroles.remove(mr);
				}else
					throw new InvalidObjectException("Role does not exist: " + role);
			}
			      
			success = commitTransaction();
		} finally {
			if (!success) {
				rollbackTransaction();
			}
		}
		return success;
	}

	//revoke roles from user
	public boolean revokeRoleFromUser(String username, List<String> roles)
		throws NoSuchObjectException, InvalidObjectException {
		
		boolean success = false;
		try {
			openTransaction();
			
			Query query = pm.newQuery(MUser.class, "userName == username"); 
		    query.declareParameters("java.lang.String username"); 
		    query.setUnique(true); 
		    MUser muser = (MUser) query.execute(username); 
		    
			if(muser == null){
				throw new NoSuchObjectException("User does not exist: " + username);
			}else
				pm.retrieve(muser);
			List<MRole> mroles = muser.getPlay_roles();
			MRole mrole;
			for(String role: roles){
				mrole = getMRole(role);
				if(mrole != null){
					if(mroles.contains(mrole))
						mroles.remove(mrole);
				}else
					throw new InvalidObjectException("Role does not exist: " + role);
			}
			      
			success = commitTransaction();
		} finally {
			if (!success) {
				rollbackTransaction();
			}
		}
		return success;
	}

	//set user's passwd
	public boolean setPasswd(String username, String newPasswd)
		throws NoSuchObjectException {
		
		boolean success = false;
		try {
			openTransaction();
			
			Query query = pm.newQuery(MUser.class, "userName == username"); 
		    query.declareParameters("java.lang.String username"); 
		    query.setUnique(true); 
		    MUser  muser = (MUser) query.execute(username); 
		    if(muser != null)
		    	pm.retrieve(muser);
			if(muser == null){
				throw new NoSuchObjectException("User does not exist: " + username);
				
			}
			muser.setPasswd(newPasswd);
      
			success = commitTransaction();
		} finally {
			if (!success) {
				rollbackTransaction();
			}
		}
		return success;
	}
	
// added by BrantZhang for authorization end
	
	// added by konten for index begin
	//create index, status Ä¬ÈÏÎª0
    public boolean createIndex(IndexItem index) 
    {
        boolean commited = false;
        MIndexItem mdbps = null;

        //String dbName, String tblName, String idxName, List<String> fieldList, String location, int idxType
        try
        {
            openTransaction();
            
//          Query query = pm.newQuery(MTblPriv.class,"db.name == dbName && table.tableName == tbl");
//          query.declareParameters("java.lang.String dbName, java.lang.String tbl"); 

            String dbName = index.getDb();
            String tblName = index.getTbl();
            String idxName = index.getName();
            Query query = pm.newQuery(MIndexItem.class, "db == dbName && tbl == tblName && name == idxName");
            query.declareParameters("java.lang.String dbName, java.lang.String tblName, java.lang.String idxName");
            query.setUnique(true);
            mdbps = (MIndexItem) query.execute(dbName.trim(), tblName.trim(), idxName.trim());

            if (mdbps != null)
            {   
                commited = commitTransaction();
                return false;                
            }        
            
            
            MIndexItem mindexItem = new MIndexItem(dbName, tblName, idxName, index.getFieldList(), index.getLocation(), index.getType(), MIndexItem.IndexStatusInit);
            pm.makePersistent(mindexItem);
           
            commited = commitTransaction();            
            return commited;            
           
        }        
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (!commited)
            {
                rollbackTransaction();
            }
            
            return commited;
        }
    }
        
    public boolean dropIndex(String dbName, String tblName, String idxName)
    {
        boolean commited = false;
        MIndexItem mdbps = null;
        try
        {
            openTransaction();
            
//          Query query = pm.newQuery(MTblPriv.class,"db.name == dbName && table.tableName == tbl");
//          query.declareParameters("java.lang.String dbName, java.lang.String tbl"); 

            Query query = pm.newQuery(MIndexItem.class, "db == dbName && tbl == tblName && name == idxName");
            query.declareParameters("java.lang.String dbName, java.lang.String tblName, java.lang.String idxName");
            query.setUnique(true);
            mdbps = (MIndexItem) query.execute(dbName.trim(), tblName.trim(), idxName.trim());

            if (mdbps == null)
            {
                commited = commitTransaction();
                return commited;  
            }  
            
            pm.deletePersistent(mdbps);           
            commited = commitTransaction();
            return commited;           
        }
        finally
        {
            if (!commited)
            {
                rollbackTransaction();
            }
            
            return commited;
        }
    }
    
    public int getIndexNum(String dbName, String tblName)
    {    
        boolean commited = false;
        List<MIndexItem> mdbps = null;
        int indexNum = 0;
        try
        {
            openTransaction();
            
            Query query = pm.newQuery(MIndexItem.class, "db == dbName && tbl == tblName");
            query.declareParameters("java.lang.String dbName, java.lang.String tblName");
            //query.setUnique(true);
            mdbps = (List<MIndexItem>) query.execute(dbName.trim(), tblName.trim());

            if (mdbps == null)
            {
                commitTransaction();
                return indexNum;
            }        
            
            pm.retrieveAll(mdbps);
            indexNum = mdbps.size();
            
            commited = commitTransaction();
            return indexNum;           
        }
        finally
        {
            if (!commited)
            {
                rollbackTransaction();
            }
            return indexNum;  
        }
    }
    
    public int getIndexType(String dbName, String tblName, String idxName)
    {
        boolean commited = false;
        MIndexItem indexItem = null;
        int indexType = -1;
        try
        {
            openTransaction();
            
            Query query = pm.newQuery(MIndexItem.class, "db == dbName && tbl == tblName && name == idxName");
            query.declareParameters("java.lang.String dbName, java.lang.String tblName, java.lang.String idxName");
            query.setUnique(true);
            indexItem = (MIndexItem)query.execute(dbName.trim(), tblName.trim(), idxName.trim());

            if (indexItem == null)
            {
                commitTransaction();
                return indexType;                
            }     
            
            pm.retrieve(indexItem);            
            
            commited = commitTransaction();
            return indexItem.getType();           
        }
        finally
        {
            if (!commited)
            {
                rollbackTransaction();
            }
            return indexType;    
        }
    }
    
    public String getIndexField(String dbName, String tblName, String idxName)
    {
        boolean commited = false;
        MIndexItem mindexItem = null;
        String fieldList = "";
        try
        {
            openTransaction();
            
            Query query = pm.newQuery(MIndexItem.class, "db == dbName && tbl == tblName && name == idxName");
            query.declareParameters("java.lang.String dbName, java.lang.String tblName, java.lang.String idxName");
            query.setUnique(true);
            mindexItem = (MIndexItem)query.execute(dbName.trim(), tblName.trim(), idxName.trim());

            if (mindexItem == null)
            {
                commitTransaction();
                return fieldList;
            }     
            
            pm.retrieve(mindexItem);            
            
            commited = commitTransaction();
            return mindexItem.getFieldList();           
        }
        finally
        {
            if (!commited)
            {
                rollbackTransaction();
            }
            return fieldList; 
        }
    }
    
    public String getIndexLocation(String dbName, String tblName, String idxName)
    {
        boolean commited = false;
        MIndexItem indexItem = null;
        String location = "";
        try
        {
            openTransaction();
            
            Query query = pm.newQuery(MIndexItem.class, "db == dbName && tbl == tblName && name == idxName");
            query.declareParameters("java.lang.String dbName, java.lang.String tblName, java.lang.String idxName");
            query.setUnique(true);
            indexItem = (MIndexItem)query.execute(dbName.trim(), tblName.trim(), idxName.trim());

            if (indexItem == null)
            {
                commitTransaction();
                return location;
            }     
            
            pm.retrieve(indexItem);            
            
            commited = commitTransaction();
            return indexItem.getLocation();           
        }
        finally
        {
            if (!commited)
            {
                rollbackTransaction();
            }
            return location;  
        }
    }
    
    public boolean setIndexLocation(String dbName, String tblName, String idxName, String location)
    {
        boolean commited = false;
        MIndexItem indexItem = null;
        
        try
        {
            openTransaction();
            
            Query query = pm.newQuery(MIndexItem.class, "db == dbName && tbl == tblName && name == idxName");
            query.declareParameters("java.lang.String dbName, java.lang.String tblName, java.lang.String idxName");
            query.setUnique(true);
            indexItem = (MIndexItem)query.execute(dbName.trim(), tblName.trim(), idxName.trim());

            if (indexItem == null)
            {
                commitTransaction();
                return commited;
            }     
            
            pm.retrieve(indexItem);            
            
            indexItem.setLocation(location);
            
            commited = commitTransaction();
            return true;           
        }
        finally
        {
            if (!commited)
            {
                rollbackTransaction();
            }
            return commited;  
        }
    }
    
    public boolean setIndexStatus(String dbName, String tblName, String idxName, int status)
    {
        boolean commited = false;
        MIndexItem mindexItem = null;
        
        try
        {
            openTransaction();
            
            Query query = pm.newQuery(MIndexItem.class, "db == dbName && tbl == tblName && name == idxName");
            query.declareParameters("java.lang.String dbName, java.lang.String tblName, java.lang.String idxName");
            query.setUnique(true);
            mindexItem = (MIndexItem)query.execute(dbName.trim(), tblName.trim(), idxName.trim());

            if (mindexItem == null)
            {
                commitTransaction();
                return commited;
            }     
            
            pm.retrieve(mindexItem);            
            
            mindexItem.setStatus(status);
            
            commited = commitTransaction();
            return true;           
        }
        finally
        {
            if (!commited)
            {
                rollbackTransaction();
            }
            return commited;  
        }
    }
    
    public List<IndexItem> getAllIndexTable(String dbName, String tblName)
    {
        boolean commited = false;
        List<MIndexItem> mindexItem = null;
        List<IndexItem> result = null;
        try
        {
            openTransaction();
            
            Query query = pm.newQuery(MIndexItem.class, "db == dbName && tbl == tblName");
            query.declareParameters("java.lang.String dbName, java.lang.String tblName");
            //query.setUnique(true);            
           
            mindexItem = (List<MIndexItem>)query.execute(dbName.trim(), tblName.trim());

            if (mindexItem == null)
            {                
                commitTransaction();
                return result;
            }     
            
            pm.retrieveAll(mindexItem);            
            
            result = new ArrayList<IndexItem>(10);
            for(MIndexItem item: mindexItem)
            {
                result.add(new IndexItem(item.getDb(), item.getTbl(), item.getName(), 
                                         item.getFieldList(), item.getLocation(), null, null,
                                         item.getType(), item.getStatus())); 
            }
            
            commited = commitTransaction();
            return result;       
        }
        finally
        {
            if (!commited)
            {
                rollbackTransaction();
                return null;  
            }
            
            return result; 
        }         
    }
    
    public IndexItem getIndexInfo(String dbName, String tblName, String idxName)
    {
        boolean commited = false;         
        IndexItem item = null;        
        try
        {
            openTransaction();
            
            Query query = pm.newQuery(MIndexItem.class, "db == dbName && tbl == tblName && name == idxName");
            query.declareParameters("java.lang.String dbName, java.lang.String tblName, java.lang.String idxName");
            query.setUnique(true);
            MIndexItem mindexItem = (MIndexItem)query.execute(dbName.trim(), tblName.trim(), idxName.trim());

            if (mindexItem == null)
            {                
                commited = commitTransaction();
                return null;  
                                
            }     
            
            pm.retrieve(mindexItem);            
            
            item = new IndexItem(mindexItem.getDb(), mindexItem.getTbl(), mindexItem.getName(), 
                            mindexItem.getFieldList(), mindexItem.getLocation(), null, null,
                            mindexItem.getType(), mindexItem.getStatus());
            
            commited = commitTransaction();
            return item;       
        }
        finally
        {
            if (!commited)
            {
                rollbackTransaction();
                return null; 
            }
            
            return item;
        }
    }
    
    public List<IndexItem> getAllIndexSys()
    {
        boolean commited = false;
        List<MIndexItem> mindexItem = null;
        List<IndexItem> result = null;
        try
        {
            openTransaction();
            
            Query query = pm.newQuery(MIndexItem.class);
            //query.declareParameters("java.lang.String db, java.lang.String tbl");
            //query.setUnique(true);
            mindexItem = (List<MIndexItem>)query.execute();

            if (mindexItem == null)
            {
                commitTransaction();
                return result;
            }     
            
            pm.retrieveAll(mindexItem);            
            
            result = new ArrayList<IndexItem>(10);
            for(MIndexItem item: mindexItem)
            {
                result.add(new IndexItem(item.getDb(), item.getTbl(), item.getName(), 
                                         item.getFieldList(), item.getLocation(), null, null,
                                         item.getType(), item.getStatus())); 
            }
            
            commited = commitTransaction();
            return result;       
        }
        finally
        {
            if (!commited)
            {
                rollbackTransaction();                
            }
            return result;            
        }
    }
	// added by konten for index end
}

