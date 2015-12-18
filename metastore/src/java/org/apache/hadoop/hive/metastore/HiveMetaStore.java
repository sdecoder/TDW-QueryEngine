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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DbPriv;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.IndexAlreadyExistsException;
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
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.FacebookService;
import com.facebook.fb303.fb_status;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

/**
 * TODO:pc remove application logic to a separate interface. 
 */
public class HiveMetaStore extends ThriftHiveMetastore {
	//added by Brantzhang begin
	public static final String ROOT_USER = "root";
	public static final String THE_SYSTEM = "hive";
	//added by Brantzhang end
  
    public static class HMSHandler extends FacebookBase implements ThriftHiveMetastore.Iface {
      public static final Log LOG = LogFactory.getLog(HiveMetaStore.class.getName());
      public static final Log AUTH_LOG = LogFactory.getLog("Authorization_LOG");
      private static boolean createDefaultDB = false;
      private static boolean createRootUser = false; //added by BrantZhang for authorization
      private boolean checkForRootUser;//added by BrantZhang for authorization
      private static String rootPasswd = "tdwroot";
      private String rawStoreClassName;
      private HiveConf hiveConf; // stores datastore (jpox) properties, right now they come from jpox.properties
      private Warehouse wh; // hdfs warehouse
      private int numOfHashPar;//added by BrantZhang for hash partition
      private ThreadLocal<RawStore> threadLocalMS = new ThreadLocal() {
        protected synchronized Object initialValue() {
            return null;
        }
      };

      // The next serial number to be assigned
      private boolean checkForDefaultDb;
      private static int nextSerialNum = 0;
      private static ThreadLocal<Integer> threadLocalId = new ThreadLocal() {
        protected synchronized Object initialValue() {
          return new Integer(nextSerialNum++);
        }
      };
      public static Integer get() {
        return threadLocalId.get();     
      }
      
      public HMSHandler(String name) throws MetaException {
        super(name);
        hiveConf = new HiveConf(this.getClass());
        init();
      }
      
      public HMSHandler(String name, HiveConf conf) throws MetaException {
        super(name);
        hiveConf = conf;
        init();
      }

      private ClassLoader classLoader;
      private AlterHandler alterHandler;
      {
        classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
          classLoader = Configuration.class.getClassLoader();
        }
      }
      
      private boolean init() throws MetaException {
        rawStoreClassName = hiveConf.get("hive.metastore.rawstore.impl");
        checkForDefaultDb = hiveConf.getBoolean("hive.metastore.checkForDefaultDb", true);
        checkForRootUser = hiveConf.getBoolean("hive.metastore.checkForRootUser", true);//added by BrantZhang for authorization
        String alterHandlerName = hiveConf.get("hive.metastore.alter.impl", HiveAlterHandler.class.getName());
        alterHandler = (AlterHandler) ReflectionUtils.newInstance(getClass(alterHandlerName, AlterHandler.class), hiveConf);
        wh = new Warehouse(hiveConf);
        //added by BrantZhang for hash partition Begin
        numOfHashPar = hiveConf.getInt("hive.hashPartition.num", 500);
        if(numOfHashPar<=0)
        	throw new MetaException("Hash Partition Number should be Positive Integer!");
        //added by BrantZhang for hash partition End
        createDefaultDB();
        createRootUser();
        return true;
      }

      /**
       * @return
       * @throws MetaException 
       */
      private RawStore getMS() throws MetaException {
        RawStore ms = threadLocalMS.get();
        if(ms == null) {
          LOG.info(threadLocalId.get() + ": Opening raw store with implemenation class:" + rawStoreClassName);
          ms = (RawStore) ReflectionUtils.newInstance(getClass(rawStoreClassName, RawStore.class), hiveConf);
          threadLocalMS.set(ms);
          ms = threadLocalMS.get();
        }
        return ms;
      }

      /**
       * create default database if it doesn't exist
       * @throws MetaException
       */
      private void createDefaultDB() throws MetaException {
        if(HMSHandler.createDefaultDB || !checkForDefaultDb) {
          return;
        }
        try {
          getMS().getDatabase(MetaStoreUtils.DEFAULT_DATABASE_NAME);
        } catch (NoSuchObjectException e) {
          getMS().createDatabase(new Database(MetaStoreUtils.DEFAULT_DATABASE_NAME, 
                    wh.getDefaultDatabasePath(MetaStoreUtils.DEFAULT_DATABASE_NAME).toString()));
        }
        HMSHandler.createDefaultDB = true;
      }

      private Class<?> getClass(String rawStoreClassName, Class<?> class1) throws MetaException {
        try {
          return Class.forName(rawStoreClassName, true, classLoader);
        } catch (ClassNotFoundException e) {
          throw new MetaException(rawStoreClassName + " class not found");
        }
      }
      
      private void logStartFunction(String m) {
        LOG.info(threadLocalId.get().toString() + ": " + m);
      }

      private void logStartFunction(String f, String db, String tbl) {
        LOG.info(threadLocalId.get().toString() + ": " + f + " : db=" + db + " tbl=" + tbl);
      }
      
   // added by BrantZhang for authorization begin
      private void logStartFunction(String bywho, String dowhat){
    	  AUTH_LOG.info(bywho + " " + dowhat);
      }
   // added by BrantZhang for authorization end
      
      @Override
      public int getStatus() {
        return fb_status.ALIVE;
      }
      
      public void shutdown() {
        logStartFunction("Shutting down the object store...");
        try {
          if(threadLocalMS.get() != null) {
            getMS().shutdown();
          }
        } catch (MetaException e) {
          LOG.error("unable to shutdown metastore", e);
        }
        System.exit(0);
      }

      public boolean create_database(String name, String location_uri)
      throws AlreadyExistsException, MetaException {
        this.incrementCounter("create_database");
        logStartFunction("create_database: " + name);
        boolean success = false;
        try {
          getMS().openTransaction();
          Database db = new Database(name, location_uri);
          if(getMS().createDatabase(db) && wh.mkdirs(wh.getDefaultDatabasePath(name))) {
            success = getMS().commitTransaction();
          }
        } finally {
          if(!success) {
            getMS().rollbackTransaction();
          }
        }
        return success;
      }

      public Database get_database(String name) throws NoSuchObjectException, MetaException {
        this.incrementCounter("get_database");
        logStartFunction("get_database: " + name);
        return getMS().getDatabase(name);
      }

      public boolean drop_database(String name) throws MetaException {
        this.incrementCounter("drop_database");
        logStartFunction("drop_database: " + name);
        if(name.equalsIgnoreCase(MetaStoreUtils.DEFAULT_DATABASE_NAME)) {
          throw new MetaException("Can't drop default database");
        }
        boolean success = false;
        try {
          getMS().openTransaction();
          if(getMS().dropDatabase(name)) {
            success = getMS().commitTransaction();
            
            // remove index dir
            Path indexPath = wh.getDefaultIndexPath(name, "", "");
            wh.deleteDir(indexPath, true);
          }
        } finally {
          if(!success) {
            getMS().rollbackTransaction();
          } else {
            wh.deleteDir(wh.getDefaultDatabasePath(name), true);
            // it is not a terrible thing even if the data is not deleted
          }
        }
        return success;
      }

      public List<String> get_databases() throws MetaException {
        this.incrementCounter("get_databases");
        logStartFunction("get_databases");
        return getMS().getDatabases();
      }
      
      public Warehouse getWarehouse(){
    	  return wh;
      }

      public boolean create_type(Type type) throws AlreadyExistsException, MetaException, InvalidObjectException {
        this.incrementCounter("create_type");
        logStartFunction("create_type: " + type.getName());
        // check whether type already exists
        if(get_type(type.getName()) != null) {
          throw new AlreadyExistsException("Type " + type.getName() + " already exists");
        }

        //TODO:pc Validation of types should be done by clients or here????
        return getMS().createType(type);
      }

      public Type get_type(String name) throws MetaException {
        this.incrementCounter("get_type");
        logStartFunction("get_type: " + name);
        return getMS().getType(name);
      }

      public boolean drop_type(String name) throws MetaException {
        this.incrementCounter("drop_type");
        logStartFunction("drop_type: " + name);
        // TODO:pc validate that there are no types that refer to this 
        return getMS().dropType(name);
      }

      public Map<String, Type> get_type_all(String name) throws MetaException {
        this.incrementCounter("get_type_all");
        // TODO Auto-generated method stub
        logStartFunction("get_type_all");
        throw new MetaException("Not yet implemented");
      }

      public void create_table(Table tbl) throws AlreadyExistsException, MetaException, InvalidObjectException {
        this.incrementCounter("create_table");
        logStartFunction("create_table: db=" + tbl.getDbName() + " tbl=" + tbl.getTableName());
        
        // validate table name & column names & partition name
        
        // Modification By : guosijie
        // Modification Date : 2010-03-10

        // start
        
        if(!MetaStoreUtils.validateName(tbl.getTableName()) ||
            !MetaStoreUtils.validateColNames(tbl.getSd().getCols()) ||
             (tbl.getPriPartition() != null &&
              !MetaStoreUtils.validateName(tbl.getPriPartition().getParKey().getName())) ||
             (tbl.getSubPartition() != null &&
              !MetaStoreUtils.validateName(tbl.getSubPartition().getParKey().getName()))) {
            throw new InvalidObjectException(tbl.getTableName() + " is not a valid object name");
        }
        
        // end
        
        Path tblPath = null;
        Path indexPath = null;
        boolean madeIndexDir = false;
        
        boolean success = false, madeDir = false;
        List<Path> partPaths = null;
        try {
          getMS().openTransaction();
          if(tbl.getSd().getLocation() == null || tbl.getSd().getLocation().isEmpty()) {
            tblPath = wh.getDefaultTablePath(tbl.getDbName(), tbl.getTableName());
          } else {
            if (!isExternal(tbl)) {
              LOG.warn("Location: " + tbl.getSd().getLocation() +
                       "specified for non-external table:" + tbl.getTableName());
            }
            tblPath = wh.getDnsPath(new Path(tbl.getSd().getLocation()));
          }

          tbl.getSd().setLocation(tblPath.toString());

          // get_table checks whether database exists, it should be moved here
          if(is_table_exists(tbl.getDbName(), tbl.getTableName())) {
            throw new AlreadyExistsException("Table " + tbl.getTableName() + " already exists");
          }
          
          if(!wh.isDir(tblPath)) {
            if(!wh.mkdirs(tblPath)) {
              throw new MetaException (tblPath + " is not a directory or unable to create one");
            }
            madeDir = true;
          }
          //Modified by Brantzhang for hash partition Begin
          if (tbl.getPriPartition() != null) { // a partitioned table
        	Set<String> priPartNames;
        	/*if(tbl.getPriPartition().getParType().equals("hash")){
        		priPartNames = new TreeSet<String>();
        		for(int i = 0; i < numOfHashPar; i++){
        			if(i < 10)
        				priPartNames.add("Hash_" + "000" + i);
        			else if(i < 100)
        				priPartNames.add("Hash_" + "00" + i);
        			else if(i < 1000)
        				priPartNames.add("Hash_" + "0" + i);
        			else priPartNames.add("Hash_" + i);
        		}
        	}else{
        		priPartNames = tbl.getPriPartition().getParSpaces().keySet();
        	}*/
        	priPartNames = tbl.getPriPartition().getParSpaces().keySet();
            
            Set<String> subPartNames = null;
            if (tbl.getSubPartition() != null) {
              /*if(tbl.getSubPartition().getParType().equals("hash")){
            	subPartNames = new TreeSet<String>();
            	for(int i = 0; i < numOfHashPar; i++){
          			if(i < 10)
          				subPartNames.add("Hash_" + "000" + i);
          			else if(i < 100)
          				subPartNames.add("Hash_" + "00" + i);
          			else if(i < 1000)
          				subPartNames.add("Hash_" + "0" + i);
          			else subPartNames.add("Hash_" + i);
          		}
              }else
            	  subPartNames = tbl.getSubPartition().getParSpaces().keySet();*/
              subPartNames = tbl.getSubPartition().getParSpaces().keySet();
            }
            
            partPaths = wh.getPartitionPaths(tblPath, priPartNames, subPartNames);
            
            for (Path partPath : partPaths) {
              if (!wh.mkdirs(partPath)) {
                throw new MetaException ("Partition path " + partPath + " is not a directory or unable to create one.");
              }
            }
          }
          //Modified by Brantzhang for hash partition End

          getMS().createTable(tbl);
          success = getMS().commitTransaction();
      
        } finally {
          if(!success) {
            getMS().rollbackTransaction();
            if(madeDir)
            {
                wh.deleteDir(tblPath, true);              
            }            
          }
        }
      }
      
      public boolean is_table_exists(String dbname, String name) throws MetaException {
        try {
          return (get_table(dbname, name) != null);
        } catch (NoSuchObjectException e) {
          return false;
        }
      }
      
      public void drop_table(String dbname, String name, boolean deleteData) throws NoSuchObjectException, MetaException {
        this.incrementCounter("drop_table");
        logStartFunction("drop_table", dbname, name);
        boolean success = false;
        boolean isExternal = false;
        Path tblPath = null;
        Path indexPath = null;
        Table tbl = null;
        isExternal = false;
        try {
          getMS().openTransaction();
          // drop any partitions
          tbl = get_table(dbname, name);
          if (tbl == null) {
            throw new NoSuchObjectException(name + " doesn't exist");
          }
          if(tbl.getSd() == null  || tbl.getSd().getLocation() == null) {
            throw new MetaException("Table metadata is corrupted");
          }
          isExternal = isExternal(tbl);
          tblPath = new Path(tbl.getSd().getLocation());
          if(!getMS().dropTable(dbname, name)) {
            throw new MetaException("Unable to drop table");
          }
          tbl = null; // table collections disappear after dropping
          success  = getMS().commitTransaction();
          
          // drop all table index dir here
          List<IndexItem> indexItemList = getMS().getAllIndexTable(dbname, name);
          if(indexItemList != null)
          {
              for(int i = 0; i < indexItemList.size(); i++)
              {
                  try
                  {
                      getMS().dropIndex(dbname, name, indexItemList.get(i).getName());
                  }
                  catch(Exception e)
                  {
                      LOG.info("drop index:"+indexItemList.get(i).getName()+" fail:"+e.getMessage()+",db:"+dbname+",table:"+name);
                  }
              }
          }
          // remove index dir
          indexPath = wh.getDefaultIndexPath(dbname, name, "");
          wh.deleteDir(indexPath, true);
          
        }
            finally
            {
                if (!success)
                {
                    getMS().rollbackTransaction();
                }
                else if (deleteData && (tblPath != null) && !isExternal)
                {
                    wh.deleteDir(tblPath, true);
                    // ok even if the data is not deleted
                }
            }
      }

      /**
       * Is this an external table?
       * @param table Check if this table is external.
       * @return True if the table is external, otherwise false.
       */
      private boolean isExternal(Table table) {
        if(table == null) {
          return false;
        }
        Map<String, String> params = table.getParameters();
        if(params == null) {
          return false;
        }
        
        return "TRUE".equalsIgnoreCase(params.get("EXTERNAL"));
      }

      public Table get_table(String dbname, String name) throws MetaException, NoSuchObjectException {
        this.incrementCounter("get_table");
        logStartFunction("get_table", dbname, name);
        Table t = getMS().getTable(dbname, name);
        if(t == null) {
          throw new NoSuchObjectException(dbname + "." + name + " table not found");
        }
        return t;
      }

      public boolean set_table_parameters(String dbname, String name, 
          Map<String, String> params) throws NoSuchObjectException,
          MetaException {
        this.incrementCounter("set_table_parameters");
        logStartFunction("set_table_parameters", dbname, name);
        // TODO Auto-generated method stub
        return false;
      }

//      public Partition append_partition(String dbName, String tableName, List<String> part_vals)
//          throws InvalidObjectException, AlreadyExistsException, MetaException {
//        this.incrementCounter("append_partition");
//        logStartFunction("append_partition", dbName, tableName);
//        if(LOG.isDebugEnabled()) {
//          for (String part : part_vals) {
//            LOG.debug(part);
//          }
//        }
//        Partition part = new Partition();
//        boolean success = false, madeDir = false;
//        Path partLocation = null;
//        try {
//          getMS().openTransaction();
//          part = new Partition();
//          part.setDbName(dbName);
//          part.setTableName(tableName);
//          part.setValues(part_vals);
//
//          Table tbl = getMS().getTable(part.getDbName(), part.getTableName());
//          if(tbl == null) {
//            throw new InvalidObjectException("Unable to add partition because table or database do not exist");
//          }
//
//          part.setSd(tbl.getSd());
//          partLocation = new Path(tbl.getSd().getLocation(),
//                                  Warehouse.makePartName(tbl.getPartitionKeys(), part_vals));
//          part.getSd().setLocation(partLocation.toString());
//
//          Partition old_part = this.get_partition(part.getDbName(),
//                                                  part.getTableName(), part.getValues());
//          if( old_part != null) {
//            throw new AlreadyExistsException("Partition already exists:" + part);
//          }
//          
//          if(!wh.isDir(partLocation)) {
//            if(!wh.mkdirs(partLocation)) {
//              throw new MetaException (partLocation + " is not a directory or unable to create one");
//            }
//            madeDir = true;
//          }
//
//          success = getMS().addPartition(part);
//          if(success) {
//            success = getMS().commitTransaction();
//          }
//        } finally {
//          if(!success) {
//            getMS().rollbackTransaction();
//            if(madeDir) {
//              wh.deleteDir(partLocation, true);
//            }
//          }
//        }
//        return part;
//      }
      
//      public int add_partitions(List<Partition> parts) throws MetaException, InvalidObjectException, AlreadyExistsException {
//        this.incrementCounter("add_partition");
//        if(parts.size() == 0) {
//          return 0;
//        }
//        String db = parts.get(0).getDbName();
//        String tbl = parts.get(0).getTableName();
//        logStartFunction("add_partitions", db, tbl);
//        boolean success = false;
//        try {
//          getMS().openTransaction();
//          for (Partition part : parts) {
//            this.add_partition(part);
//          }
//          success = true;
//          getMS().commitTransaction();
//        } finally {
//          if(!success) {
//            getMS().rollbackTransaction();
//          }
//        }
//        return parts.size();
//      }

//      public Partition add_partition(Partition part) throws InvalidObjectException,
//          AlreadyExistsException, MetaException {
//        this.incrementCounter("add_partition");
//        logStartFunction("add_partition", part.getDbName(), part.getTableName());
//        boolean success = false, madeDir = false;
//        Path partLocation = null;
//        try {
//          getMS().openTransaction();
//          Partition old_part = this.get_partition(part.getDbName(), part.getTableName(), part.getValues());
//          if( old_part != null) {
//            throw new AlreadyExistsException("Partition already exists:" + part);
//          }
//          Table tbl = getMS().getTable(part.getDbName(), part.getTableName());
//          if(tbl == null) {
//            throw new InvalidObjectException("Unable to add partition because table or database do not exist");
//          }
//
//          String partLocationStr = part.getSd().getLocation();
//          if (partLocationStr == null || partLocationStr.isEmpty()) {
//            // set default location if not specified
//            partLocation = new Path(tbl.getSd().getLocation(),
//                                    Warehouse.makePartName(tbl.getPartitionKeys(), part.getValues()));
//            
//          } else {
//            partLocation = wh.getDnsPath(new Path(partLocationStr));
//          }
//
//          part.getSd().setLocation(partLocation.toString());
//
//          if(!wh.isDir(partLocation)) {
//            if(!wh.mkdirs(partLocation)) {
//              throw new MetaException (partLocation + " is not a directory or unable to create one");
//            }
//            madeDir = true;
//          }
//
//          success = getMS().addPartition(part) && getMS().commitTransaction();
//
//        } finally {
//          if(!success) {
//            getMS().rollbackTransaction();
//            if(madeDir) {
//              wh.deleteDir(partLocation, true);
//            }
//          }
//        }
//        return part;
//      }

//      public boolean drop_partition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData) throws NoSuchObjectException, MetaException,
//          TException {
//        this.incrementCounter("drop_partition");
//        logStartFunction("drop_partition", db_name, tbl_name);
//        LOG.info("Partition values:" + part_vals);
//        boolean success = false;
//        Path partPath = null;
//        Table tbl = null;
//        try {
//          getMS().openTransaction();
//          Partition part = this.get_partition(db_name, tbl_name, part_vals);
//          if(part == null) {
//            throw new NoSuchObjectException("Partition doesn't exist. " + part_vals);
//          }
//          if(part.getSd() == null  || part.getSd().getLocation() == null) {
//            throw new MetaException("Partition metadata is corrupted");
//          }
//          if(!getMS().dropPartition(db_name, tbl_name, part_vals)) {
//            throw new MetaException("Unable to drop partition");
//          }
//          success  = getMS().commitTransaction();
//          partPath = new Path(part.getSd().getLocation());
//          tbl = get_table(db_name, tbl_name);
//        } finally {
//          if(!success) {
//            getMS().rollbackTransaction();
//          } else if(deleteData && (partPath != null)) {
//            if(tbl != null && !isExternal(tbl)) {
//              wh.deleteDir(partPath, true);
//              // ok even if the data is not deleted
//            }
//          }
//        }
//        return true;
//      }

      public Partition get_partition(String db_name, String tbl_name, int level)
          throws MetaException {
        this.incrementCounter("get_partition");
        logStartFunction("get_partition", db_name, tbl_name);
        return getMS().getPartition(db_name, tbl_name, level);
      }

      public List<Partition> get_partitions(String db_name, String tbl_name)
          throws NoSuchObjectException, MetaException {
        this.incrementCounter("get_partitions");
        logStartFunction("get_partitions", db_name, tbl_name);
        
        List<Partition> partitions = new ArrayList<Partition>();
        partitions.add(get_partition(db_name, tbl_name, 0));
        partitions.add(get_partition(db_name, tbl_name, 1));
        return partitions;
      }
      
//      public List<String> get_partition_names(String db_name, String tbl_name, short max_parts) throws MetaException {
//        this.incrementCounter("get_partition_names");
//        logStartFunction("get_partition_names", db_name, tbl_name);
//        return getMS().listPartitionNames(db_name, tbl_name, max_parts);
//      }

      public void alter_partition(String db_name, String tbl_name,
          Partition new_part) throws InvalidOperationException, MetaException,
          TException {
        this.incrementCounter("alter_partition");
        logStartFunction("alter_partition", db_name, tbl_name);
        //LOG.info("Partition :" + new_part.getParSpaces().keySet().iterator().next());
        try {
          getMS().alterPartition(db_name, tbl_name, new_part);
        } catch(InvalidObjectException e) {
          LOG.error(StringUtils.stringifyException(e));
          // throw new InvalidOperationException("alter is not possible");
          throw new InvalidOperationException(StringUtils.stringifyException(e));
        }
      }
      
      public boolean create_index(Index index_def)
          throws IndexAlreadyExistsException, MetaException {
        this.incrementCounter("create_index");
        // TODO Auto-generated method stub
        throw new MetaException("Not yet implemented");
      }

      public String getVersion() throws TException {
        this.incrementCounter("getVersion");
        logStartFunction("getVersion");
        return "3.0";
      }
      
      public void alter_table(String dbname, String name, Table newTable) throws InvalidOperationException,
          MetaException {
        this.incrementCounter("alter_table");
        logStartFunction("truncate_table: db=" + dbname + " tbl=" + name + " newtbl=" + newTable.getTableName());
        alterHandler.alterTable(getMS(), wh, dbname, name, newTable);
      }

      public List<String> get_tables(String dbname, String pattern) throws MetaException {
        this.incrementCounter("get_tables");
        logStartFunction("get_tables: db=" + dbname + " pat=" + pattern);
        return getMS().getTables(dbname, pattern);
      }


      public List<FieldSchema> get_fields(String db, String tableName) 
        throws MetaException,UnknownTableException, UnknownDBException {
        this.incrementCounter("get_fields");
        logStartFunction("get_fields: db=" + db + "tbl=" + tableName);
        String [] names = tableName.split("\\.");
        String base_table_name = names[0];

        Table tbl;
        try {
          tbl = this.get_table(db, base_table_name);
        } catch (NoSuchObjectException e) {
          throw new UnknownTableException(e.getMessage());
        }
        boolean isNative = SerDeUtils.isNativeSerDe(tbl.getSd().getSerdeInfo().getSerializationLib());
        if (isNative)
          return tbl.getSd().getCols();
        else {
          try {
            Deserializer s = MetaStoreUtils.getDeserializer(this.hiveConf, tbl);
            return MetaStoreUtils.getFieldsFromDeserializer(tableName, s);
          } catch(SerDeException e) {
            StringUtils.stringifyException(e);
            throw new MetaException(e.getMessage());
          }
        }
      }
      
      /**
       * Return the schema of the table. This function includes partition columns
       * in addition to the regular columns.
       * @param db Name of the database
       * @param tableName Name of the table
       * @return List of columns, each column is a FieldSchema structure
       * @throws MetaException
       * @throws UnknownTableException
       * @throws UnknownDBException
       */
      public List<FieldSchema> get_schema(String db, String tableName) 
        throws MetaException, UnknownTableException, UnknownDBException {
        this.incrementCounter("get_schema");
        logStartFunction("get_schema: db=" + db + "tbl=" + tableName);
        String [] names = tableName.split("\\.");
        String base_table_name = names[0];
        
        Table tbl;
        try {
          tbl = this.get_table(db, base_table_name);
        } catch (NoSuchObjectException e) {
          throw new UnknownTableException(e.getMessage());
        }
        List<FieldSchema> fieldSchemas = this.get_fields(db, base_table_name);

        if (tbl == null || fieldSchemas == null) {
          throw new UnknownTableException(tableName + " doesn't exist");
        }
        
        // Note: as the partition field is defined in the column lists, so we do not need to add it seperately.
//        if (tbl.getPartitionKeys() != null) {
//          // Combine the column field schemas and the partition keys to create the whole schema
//          fieldSchemas.addAll(tbl.getPartitionKeys());
//        }
        return fieldSchemas;
      }

      public String getCpuProfile(int profileDurationInSec) throws TException {
        return "";
      }
      
      // joeyli added for statics information collection begin
      
      public tdw_sys_table_statistics add_table_statistics(tdw_sys_table_statistics new_table_statistics) throws AlreadyExistsException, MetaException, TException{
          this.incrementCounter("add_table_statistics");
          logStartFunction("add_table_statistics "+new_table_statistics.getStat_table_name());
          boolean success = false;
          try {
            getMS().openTransaction();
            tdw_sys_table_statistics old_table_statistics = this.get_table_statistics(new_table_statistics.getStat_table_name());
            if( old_table_statistics != null) {
              throw new AlreadyExistsException("table_statistics already exists:" + new_table_statistics);
            }
            success = getMS().add_table_statistics(new_table_statistics) && getMS().commitTransaction();
          } finally {
            if(!success) {
              getMS().rollbackTransaction();
            }
          }
          return new_table_statistics;
  
      }
      

      public boolean delete_table_statistics(String table_statistics_name) throws NoSuchObjectException, MetaException, TException{
          this.incrementCounter("delete_table_statistics");
          logStartFunction("delete_table_statistics: " + table_statistics_name);
          boolean success = false;
          try {
            getMS().openTransaction();
            if(getMS().delete_table_statistics(table_statistics_name)) {
              success = getMS().commitTransaction();
            }
          } finally {
            if(!success) {
              getMS().rollbackTransaction();
            } 
          }
          return success;
      }

      public tdw_sys_table_statistics get_table_statistics(String table_statistics_name) throws MetaException, TException{
          this.incrementCounter("get_table_statistics");
          logStartFunction("get_table_statistics"+table_statistics_name);
          tdw_sys_table_statistics table_statistics = getMS().get_table_statistics(table_statistics_name);
          return table_statistics;
      }

      public List<tdw_sys_table_statistics> get_table_statistics_multi(int max) throws NoSuchObjectException, MetaException, TException{
          this.incrementCounter("get_table_statistics_multi");
          logStartFunction("get_table_statistics_multi");
          return getMS().get_table_statistics_multi(max);
      }

      public List<String> get_table_statistics_names(int max) throws NoSuchObjectException, MetaException, TException{
          this.incrementCounter("get_table_statistics_names");
          logStartFunction("get_table_statistics_names");
          return getMS().get_table_statistics_names(max);
      }

      public tdw_sys_fields_statistics add_fields_statistics(tdw_sys_fields_statistics new_fields_statistics) throws AlreadyExistsException, MetaException, TException{
          this.incrementCounter("add_fields_statistics");
          logStartFunction("add_fields_statistics "+new_fields_statistics.getStat_table_name());
          boolean success = false;
          try {
            getMS().openTransaction();
            tdw_sys_fields_statistics old_fields_statistics = this.get_fields_statistics(new_fields_statistics.getStat_table_name(),new_fields_statistics.getStat_field_name());
            if( old_fields_statistics != null) {
              throw new AlreadyExistsException("table_statistics already exists:" + new_fields_statistics);
            }
            success = getMS().add_fields_statistics(new_fields_statistics) && getMS().commitTransaction();
          } finally {
            if(!success) {
              getMS().rollbackTransaction();
            }
          }
          return new_fields_statistics;
  
      }
      

      public boolean delete_fields_statistics(String stat_table_name, String stat_field_name) throws NoSuchObjectException, MetaException, TException{
          this.incrementCounter("delete_fields_statistics");
          logStartFunction("delete_fields_statistics: " + stat_table_name+"."+stat_field_name);
          boolean success = false;
          try {
            getMS().openTransaction();
            if(getMS().delete_fields_statistics(stat_table_name,stat_field_name)) {
              success = getMS().commitTransaction();
            }
          } finally {
            if(!success) {
              getMS().rollbackTransaction();
            } 
          }
          return success;
      }

      public tdw_sys_fields_statistics get_fields_statistics(String stat_table_name,String stat_field_name) throws MetaException, TException{
          this.incrementCounter("get_fields_statistics");
          logStartFunction("get_fields_statistics"+stat_table_name+"."+stat_field_name);
          tdw_sys_fields_statistics fields_statistics = getMS().get_fields_statistics(stat_table_name,stat_field_name);
          return fields_statistics;
      }

      public List<tdw_sys_fields_statistics> get_fields_statistics_multi(String stat_table_name,int max) throws NoSuchObjectException, MetaException, TException{
          this.incrementCounter("get_fields_statistics_multi");
          logStartFunction("get_fields_statistics_multi "+stat_table_name);
          return getMS().get_fields_statistics_multi(stat_table_name,max);
      }

      public List<String> get_fields_statistics_names(String stat_table_name,int max) throws NoSuchObjectException, MetaException, TException{
          this.incrementCounter("get_fields_statistics_names");
          logStartFunction("get_fields_statistics_names "+stat_table_name);
          return getMS().get_fields_statistics_names(stat_table_name,max);
      }

      
      // joeyli added for statics information collection end      

// added by BrantZhang for authorization begin
     
      //create a new role
	public boolean create_role(String byWho, String roleName)
			throws AlreadyExistsException, TException, MetaException{
		 this.incrementCounter("create_role");
		 logStartFunction(byWho, "create_role: " + roleName);
	     
	     return getMS().createRole(roleName);
	}

	// create a new user
	public boolean create_user(String byWho, String newUser, String passwd)
			throws AlreadyExistsException, TException, MetaException {
		this.incrementCounter("create_user");
		logStartFunction(byWho, "create user: " + newUser);
	    
	    return getMS().createUser(newUser, passwd);
	}

	//drop a role
	public boolean drop_role(String byWho, String roleName)
			throws NoSuchObjectException, TException, MetaException {
		this.incrementCounter("drop_role");
		logStartFunction(byWho, "drop role: " + roleName);
	    
	    return getMS().dropRole(roleName);
	}

	//drop a user
	public boolean drop_user(String byWho, String userName)
			throws NoSuchObjectException, TException, MetaException {
		this.incrementCounter("drop_user");
		logStartFunction(byWho, "drop user: " + userName);
	    
	    return getMS().dropUser(userName);
	}
	
	//get some user's privileges on default db
	public DbPriv get_auth_on_db(String byWho, String who)
			throws TException, MetaException {
		return get_auth_on_db(byWho, who, MetaStoreUtils.DEFAULT_DATABASE_NAME);
	}

	//get some user's privileges on some db
	public DbPriv get_auth_on_db(String byWho, String who, String db)
			throws TException, MetaException {
		//this.incrementCounter("get_auth_on_db");
		//logStartFunction(byWho, "get " + who + "'s auth on db: " + db);
	     
	    return getMS().getAuthOnDb(who, db);
	}
	
	//get some user's privileges on all dbs
	public List<DbPriv> get_auth_on_dbs(String byWho, String who)
			throws TException, MetaException {
		//this.incrementCounter("get_auth_on_dbs");
		//logStartFunction(byWho, "get " + who + "'s auth on all db");
	     
	    return getMS().getAuthOnDbs(who);
	}
	
	//get some user's privileges on some table in default db
	public TblPriv get_auth_on_tbl(String byWho, String who,
			String tbl) throws TException, MetaException {
		return get_auth_on_tbl(byWho, who, MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
	}

	//get some user's privileges on some table
	public TblPriv get_auth_on_tbl(String byWho, String who, String db,
			String tbl) throws TException, MetaException {
		//this.incrementCounter("get_auth_on_tbl");
		//logStartFunction(byWho, "get " + who + "'s auth on tbl: " + tbl + " in db: "+ db);
	     
	    return getMS().getAuthOnTbl(who, db, tbl);
	}
	
	//get some user's privileges on some table
	public List<TblPriv> get_auth_on_tbls(String byWho, String who) throws TException, MetaException {
		//this.incrementCounter("get_auth_on_tbls");
		//logStartFunction(byWho, "get " + who + "'s auth on all tbls");
	     
	    return getMS().getAuthOnTbls(who);
	}
	
	//get all privileges on default db
	public List<DbPriv> get_db_auth(String byWho)
			throws TException, MetaException {
		return get_db_auth(byWho, MetaStoreUtils.DEFAULT_DATABASE_NAME);
	}

	//get all privileges on some db
	public List<DbPriv> get_db_auth(String byWho, String db)
			throws TException, MetaException {
		//this.incrementCounter("get_db_auth");
		//logStartFunction(byWho, "get all auth on db: " + db);
	     
	    return getMS().getDbAuth(db);
	}
	
	//get all privileges on db
	public List<DbPriv> get_db_auth_all(String byWho)
			throws TException, MetaException {
		//this.incrementCounter("get_db_auth_all");
		//logStartFunction(byWho, "get all auth on dbs");
	     
	    return getMS().getDbAuthAll();
	}

	//get some role
	public Role get_role(String byWho, String roleName)
			throws NoSuchObjectException, TException, MetaException {
		//this.incrementCounter("get_role");
		//logStartFunction(byWho, "get the role: " + roleName);
	     
	    return getMS().getRole(roleName);
	}

	//get all roles
	public List<String> get_roles_all(String byWho) throws TException, MetaException {
		//this.incrementCounter("get_role_all");
		//logStartFunction(byWho, "get all the roles");
	     
	    return getMS().getRolesAll();
	}
	
	//get all privileges on some table in default db
	public List<TblPriv> get_tbl_auth(String byWho, String tbl)
			throws TException, MetaException {
		return get_tbl_auth(byWho, MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
	}

	//get all privileges on some table
	public List<TblPriv> get_tbl_auth(String byWho, String db, String tbl)
			throws TException, MetaException {
		//this.incrementCounter("get_tbl_auth");
		//logStartFunction(byWho,  "get the auths on tbl: " + tbl + " in db: "+ db);
	     
	    return getMS().getTblAuth(db, tbl);
	}

	//get all privileges on tables
	public List<TblPriv> get_tbl_auth_all(String byWho)
			throws TException, MetaException {
		//this.incrementCounter("get_tbl_auth_all");
		//logStartFunction(byWho,  "get all the auths on tbls");
	     
	    return getMS().getTblAuthAll();
	}

	//get user
	public User get_user(String byWho, String userName)
			throws NoSuchObjectException, TException, MetaException {
		//this.incrementCounter("get_user");
		//logStartFunction(byWho,  "get the sys privileges of the user: " + userName);
	     
	    return getMS().getUser(userName);
	}

	//get all users' names
	public List<String> get_users_all(String byWho) throws TException, MetaException {
		//this.incrementCounter("get_users_all");
		//logStartFunction(byWho,  "get all the users' names");
	     
	    return getMS().getUsersAll();
	}
	
	//grant auth on default db
	public boolean grant_auth_on_db(String byWho, String forWho,
			List<String> privileges) throws NoSuchObjectException,
			TException, MetaException, InvalidObjectException {
		return grant_auth_on_db(byWho, forWho, privileges, MetaStoreUtils.DEFAULT_DATABASE_NAME);
	}

	//grant auth on db
	public boolean grant_auth_on_db(String byWho, String forWho,
			List<String> privileges, String db) throws NoSuchObjectException,
			TException, MetaException, InvalidObjectException {
		this.incrementCounter("grant_auth_on_db");
		String privs = "";
		int privNum = privileges.size();
		int num = 0;
		for(String priv: privileges){
			if(num < privNum-1)
				privs += priv + ", ";
			else
				privs += priv;
			num++;
		}
		
		logStartFunction(byWho, "grant privileges: " + privs + " on db: " + db + " for: " + forWho);
	     
	    return getMS().grantAuthOnDb(forWho, privileges, db);
	}
	
	//grant auth on default table
	public boolean grant_auth_on_tbl(String byWho, String forWho,
			List<String> privileges, String tbl)
			throws NoSuchObjectException, TException, InvalidObjectException, MetaException {
		return grant_auth_on_tbl(byWho, forWho, privileges, MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
	}

	//grant auth on table
	public boolean grant_auth_on_tbl(String byWho, String forWho,
			List<String> privileges, String db, String tbl)
			throws NoSuchObjectException, TException, InvalidObjectException, MetaException {
		this.incrementCounter("grant_auth_on_tbl");
		String privs = "";
		int privNum = privileges.size();
		int num = 0;
		for(String priv: privileges){
			if(num < privNum-1)
				privs += priv + ", ";
			else
				privs += priv;
			num++;
		}
		
		logStartFunction(byWho, "grant privileges: " + privs + " on tbl: " + tbl + " in db:" + db + " for: " + forWho);
	     
	    return getMS().grantAuthOnTbl(forWho, privileges, db, tbl);
	}

	//grant sys privileges to role
	public boolean grant_auth_role_sys(String byWho, String roleName,
			List<String> privileges) throws NoSuchObjectException, TException, InvalidObjectException, MetaException {
		this.incrementCounter("grant_auth_role_sys");
		String privs = "";
		int privNum = privileges.size();
		int num = 0;
		for(String priv: privileges){
			if(num < privNum-1)
				privs += priv + ", ";
			else
				privs += priv;
			num++;
		}
		
		logStartFunction(byWho, "grant sys privileges: " + privs + " for role: " + roleName);
	     
	    return getMS().grantAuthRoleSys(roleName, privileges);
	}

	//grant sys privileges to user
	public boolean grant_auth_sys(String byWho, String userName,
			List<String> privileges) throws NoSuchObjectException, TException, InvalidObjectException, MetaException {
		this.incrementCounter("grant_auth_sys");
		String privs = "";
		int privNum = privileges.size();
		int num = 0;
		for(String priv: privileges){
			if(num < privNum-1)
				privs += priv + ", ";
			else
				privs += priv;
			num++;
		}
		
		logStartFunction(byWho, "grant sys privileges: " + privs + " for user: " + userName);
	     
	    return getMS().grantAuthSys(userName, privileges);
	}

	//grant roles to role
	public boolean grant_role_to_role(String byWho, String roleName,
			List<String> roleNames) throws NoSuchObjectException, TException, InvalidObjectException, MetaException {
		this.incrementCounter("grant_role_to_role");
		String roles = "";
		int roleNum = roleNames.size();
		int num = 0;
		for(String role: roleNames){
			if(num < roleNum-1)
				roles += role + ", ";
			else
				roles += role;
			num++;
		}
		
		logStartFunction(byWho, "grant role: " + roles + " to role: " + roleName);
	     
	    return getMS().grantRoleToRole(roleName, roleNames);
	}

	//grant roles to user
	public boolean grant_role_to_user(String byWho, String userName,
			List<String> roleNames) throws NoSuchObjectException, TException, InvalidObjectException, MetaException {
		this.incrementCounter("grant_role_to_user");
		String roles = "";
		int roleNum = roleNames.size();
		int num = 0;
		for(String role: roleNames){
			if(num < roleNum-1)
				roles += role + ", ";
			else
				roles += role;
			num++;
		}
		
		logStartFunction(byWho, "grant role: " + roles + " to user: " + userName);
	     
	    return getMS().grantRoleToUser(userName, roleNames);
	}

	// judge a role
	public boolean is_a_role(String roleName) throws TException, MetaException {
		 
	    return getMS().isARole(roleName);
	}

	// judge a user
	public boolean is_a_user(String userName, String passwd) throws TException, MetaException {
		
		return getMS().isAUser(userName, passwd);
	}
	
	//revoke user's or role's privileges on default db
	public boolean revoke_auth_on_db(String byWho, String who,
			List<String> privileges) throws NoSuchObjectException,
			TException, InvalidObjectException, MetaException {
		return revoke_auth_on_db(byWho, who, privileges, MetaStoreUtils.DEFAULT_DATABASE_NAME);
	}

	//revoke user's or role's privileges on db
	public boolean revoke_auth_on_db(String byWho, String who,
			List<String> privileges, String db) throws NoSuchObjectException,
			TException, InvalidObjectException, MetaException {
		this.incrementCounter("revoke_auth_on_db");
		String privs = "";
		int privNum = privileges.size();
		int num = 0;
		for(String priv: privileges){
			if(num < privNum-1)
				privs += priv + ", ";
			else
				privs += priv;
			num++;
		}
		
		logStartFunction(byWho, "revoke privileges: " + privs + " on db: " + db +" from user/role: " + who);
	     
	    return getMS().revokeAuthOnDb(who, privileges, db);
	}
	
	//revoke user's or role's privileges on table in default db
	public boolean revoke_auth_on_tbl(String byWho, String who,
			List<String> privileges, String tbl)
			throws NoSuchObjectException, TException, InvalidObjectException, MetaException {
		return revoke_auth_on_tbl(byWho, who, privileges, MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
	}

	//revoke user's or role's privileges on table
	public boolean revoke_auth_on_tbl(String byWho, String who,
			List<String> privileges, String db, String tbl)
			throws NoSuchObjectException, TException, InvalidObjectException, MetaException {
		this.incrementCounter("revoke_auth_on_tbl");
		String privs = "";
		int privNum = privileges.size();
		int num = 0;
		for(String priv: privileges){
			if(num < privNum-1)
				privs += priv + ", ";
			else
				privs += priv;
			num++;
		}
		
		logStartFunction(byWho, "revoke privileges: " + privs + " on tbl: " + tbl + "in db: "+ db + " from user/role: " + who);
	     
	    return getMS().revokeAuthOnTbl(who, privileges, db, tbl);
	}

	//revoke role's sys privileges
	public boolean revoke_auth_role_sys(String byWho, String roleName,
			List<String> privileges) throws NoSuchObjectException, TException, InvalidObjectException, MetaException {
		this.incrementCounter("revoke_auth_role_sys");
		String privs = "";
		int privNum = privileges.size();
		int num = 0;
		for(String priv: privileges){
			if(num < privNum-1)
				privs += priv + ", ";
			else
				privs += priv;
			num++;
		}
		logStartFunction(byWho, "revoke sys privileges: " + privs + " from role: " + roleName);
	     
	    return getMS().revokeAuthRoleSys(roleName, privileges);
	}

	//revoke user's sys privileges
	public boolean revoke_auth_sys(String byWho, String userName,
			List<String> privileges) throws NoSuchObjectException, TException, InvalidObjectException, MetaException {
		this.incrementCounter("revoke_auth_sys");
		String privs = "";
		int privNum = privileges.size();
		int num = 0;
		for(String priv: privileges){
			if(num < privNum-1)
				privs += priv + ", ";
			else
				privs += priv;
			num++;
		}
		
		logStartFunction(byWho, "revoke sys privileges: " + privs + " from user: " + userName);
	     
	    return getMS().revokeAuthSys(userName, privileges);
	}

	//revoke role from role
	public boolean revoke_role_from_role(String byWho, String roleName,
			List<String> roleNames) throws NoSuchObjectException, TException, InvalidObjectException, MetaException {
		this.incrementCounter("revoke_role_from_role");
		String roles = "";
		int roleNum = roleNames.size();
		int num = 0;
		for(String role: roleNames){
			if(num < roleNum-1)
				roles += role + ", ";
			else
				roles += role;
			num++;
		}
		
		logStartFunction(byWho, "revoke roles: " + roles + " from role: " + roleName);
	     
	    return getMS().revokeRoleFromRole(roleName, roleNames);
	}

	//revoke role from user
	public boolean revoke_role_from_user(String byWho, String userName,
			List<String> roleNames) throws NoSuchObjectException, TException, InvalidObjectException, MetaException {
		this.incrementCounter("revoke_role_from_user");
		String roles = "";
		int roleNum = roleNames.size();
		int num = 0;
		for(String role: roleNames){
			if(num < roleNum-1)
				roles += role + ", ";
			else
				roles += role;
			num++;
		}
			
		logStartFunction(byWho, "revoke roles: " + roles + " from user: " + userName);
	     
	    return getMS().revokeRoleFromUser(userName, roleNames);
	}

	//set passwd
	public boolean set_passwd(String byWho, String forWho, String newPasswd)
			throws NoSuchObjectException, TException, MetaException {
		
		logStartFunction(byWho, "set passwd for user: " + forWho);
		return getMS().setPasswd(forWho, newPasswd);
	}
	
	//drop auth in DbPriv table for some user
	public boolean drop_auth_in_db(String byWho, String forWho) throws TException, MetaException {
		logStartFunction(byWho, "drop auth in DbPriv table for user: " + forWho);
		return getMS().dropAuthInDb(forWho);
	}

	//drop auth in TblPriv table for some user
	public boolean drop_auth_in_tbl(String byWho, String forWho) throws TException, MetaException {
		logStartFunction(byWho, "drop auth in TblPriv table for user: " + forWho);
		return getMS().dropAuthInTbl(forWho);
	}

	//drop auth on some db in DbPriv table for some user 
	public boolean drop_auth_on_db(String byWho, String forWho, String db) throws TException, MetaException{
		logStartFunction(byWho, "drop auth on db: " + db + " in DbPriv table for user: " + forWho);
		return getMS().dropAuthOnDb(forWho, db);
	}

	//drop auth on some tbl in TblPriv table for some user
	public boolean drop_auth_on_tbl(String byWho, String forWho, String db,
			String tbl) throws TException, MetaException{
		logStartFunction(byWho, "drop auth on table: " + tbl + " in db: " + db + " in TblPriv table for user: " + forWho);
		return getMS().dropAuthOnTbl(forWho, db, tbl);
	}
	
	//create root user at init stage
	private void createRootUser() throws MetaException{
		 if(HMSHandler.createRootUser || !checkForRootUser) {
	          return;
	        }
	        try {
	          getMS().getUser(ROOT_USER);
	        } catch (NoSuchObjectException e) {
	        	try{
	        		getMS().createUser(ROOT_USER, HMSHandler.rootPasswd);
	        		List<String> privileges = new ArrayList<String>();
	        		privileges.add("TOK_DBA_PRI");
	        		getMS().grantAuthSys(ROOT_USER, privileges);
	        	}catch(Exception ee){
	        		throw new MetaException("Failed to create the ROOT user!");
	        	}
	          
	        }
	        HMSHandler.createRootUser = true;
	}
	
// added by BrantZhang for authorization end
	
	//add by konten for index begin
	public boolean create_index(IndexItem index)throws MetaException
	{ 
        Path indexPath = null;
        boolean madeIndexDir = false;
        boolean success = false;
        try
        {
            String indexName = index.getName();
            String fieldList = "";
            int type = 1;
                        
            getMS().openTransaction();
            
             // create table中最多只有1个index, indexInfo格式固定:name;fieldList;type;        
            indexPath = wh.getDefaultIndexPath(index.getDb(), index.getTbl(), index.getName());
            if (!wh.mkdirs(indexPath))
            {
                throw new MetaException ("Index path " + indexPath + " is not a directory or unable to create one.");
            }
            
            index.setLocation(indexPath.toString());
         
            madeIndexDir = true;
            
            // create primary part dir
            Set<String> priPartNames = index.getPartPath();
            if(priPartNames != null)
            {            
                List<Path> indexPartPath = wh.getPartitionPaths(indexPath, priPartNames, null);
                for (Path partPath : indexPartPath)
                {
                    if (!wh.mkdirs(partPath))
                    {
                        throw new MetaException ("Index Partition path " + partPath + " is not a directory or unable to create one.");
                    }             
                }
            }
            
            logStartFunction("create index: " + indexName+",db:"+index.getDb()+",table:"+index.getTbl());
                       
            getMS().createIndex(index);            
            
            success = getMS().commitTransaction();
        }   
        finally
        {
            if(!success)
            {
                getMS().rollbackTransaction();
                
                if(madeIndexDir)
                {
                    wh.deleteDir(indexPath, true);
                }
            }
        }
        
        return success;
    }	
	
    public boolean drop_index(String db, String table, String name)throws MetaException
    {
        logStartFunction("drop index: " + name+",db:"+db+",table:"+table);
                
        Path indexPath = null;
        boolean madeIndexDir = false;
        boolean success = false;
        try
        {           
            getMS().openTransaction();  
            
            IndexItem item = getMS().getIndexInfo(db, table, name);
            if(item == null)
            {   
                getMS().commitTransaction();
                return true;
            }
            
            if(item.getLocation() != null)
            {
                indexPath = new Path(item.getLocation());
            }
            
            getMS().dropIndex(db, table, name);
            success = getMS().commitTransaction();
            
            // 若是最后一个index被删除, 整个index目录也不在此处删除. 在drop table操作中执行.
            
        }   
        finally
        {
            if(!success)
            {
                getMS().rollbackTransaction();
            }
            else  if(indexPath != null)// 元数据删除成功后才删除索引目录.
            {
                wh.deleteDir(indexPath, true);
            }
        }
        
        return success;
    }
    
    public int get_index_num(String db, String table)throws MetaException
    {
        logStartFunction("get index num, db:"+db+",table:"+table);
        return getMS().getIndexNum(db, table);
    }
    
    public int  get_index_type(String db, String table, String name)throws MetaException
    {
        logStartFunction("get index type, name:" + name + ",db:"+db+",table:"+table);
        return getMS().getIndexType(db, table, name);
    }
    
    public String get_index_field(String db, String table, String name)throws MetaException
    {
        logStartFunction("get index field, name:" + name + ",db:"+db+",table:"+table);
        return getMS().getIndexField(db, table, name);
    }
    
    public String get_index_location(String db, String table, String name)throws MetaException
    {
        logStartFunction("get index location, name:" + name + ",db:"+db+",table:"+table);
        return getMS().getIndexLocation(db, table, name);
    }
    
    public boolean set_index_location(String db, String table, String name, String location)throws MetaException
    {
        logStartFunction("set index location, name:" + name + ",db:"+db+",table:"+table);
        return getMS().setIndexLocation(db, table, name, location);
    }
    
    public boolean set_index_status(String db, String table, String name, int status)throws MetaException
    {
        logStartFunction("set index status, name:" + name + ",db:"+db+",table:"+table+",new status:"+status);
        return getMS().setIndexStatus(db, table, name, status);
    }
    
    public List<IndexItem>  get_all_index_table(String db, String table)throws MetaException
    {
        logStartFunction("get all index in table" + ",db:"+db+",table:"+table);
        return getMS().getAllIndexTable(db, table);
    }
    
    public IndexItem get_index_info(String db, String table, String name)throws MetaException
    {
        logStartFunction("get index info:" + name + ",db:"+db+",table:"+table);
        return getMS().getIndexInfo(db, table, name);
    }
    
    public List<IndexItem> get_all_index_sys()throws MetaException
    {
        logStartFunction("get all index sys");
        return getMS().getAllIndexSys();
    }
	//add by konten for index end
  }
    
  /**
   * @param args
   */
  public static void main(String[] args) {
    int port = 9083;

    if(args.length > 0) {
      port = Integer.getInteger(args[0]);
    }
    try {
      TServerTransport serverTransport = new TServerSocket(port);
      Iface handler = new HMSHandler("new db based metaserver");
      FacebookService.Processor processor = new ThriftHiveMetastore.Processor(handler);
      TThreadPoolServer.Options options = new TThreadPoolServer.Options();
      options.minWorkerThreads = 200;
      TServer server = new TThreadPoolServer(processor, serverTransport,
          new TTransportFactory(), new TTransportFactory(),
          new TBinaryProtocol.Factory(), new TBinaryProtocol.Factory(), options);
      HMSHandler.LOG.info("Started the new metaserver on port [" + port + "]...");
      HMSHandler.LOG.info("Options.minWorkerThreads = " + options.minWorkerThreads);
      HMSHandler.LOG.info("Options.maxWorkerThreads = " + options.maxWorkerThreads);
      server.serve();
    } catch (Throwable x) {
      x.printStackTrace();
      HMSHandler.LOG.error("Metastore Thrift Server threw an exception. Exiting...");
      HMSHandler.LOG.error(StringUtils.stringifyException(x));
      System.exit(1);
    }
  }
}

