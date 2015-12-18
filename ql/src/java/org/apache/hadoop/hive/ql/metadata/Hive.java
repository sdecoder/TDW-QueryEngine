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

package org.apache.hadoop.hive.ql.metadata;

import java.io.IOException;
//import java.io.InvalidObjectException;
import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DbPriv;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.IndexItem;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.tdw_sys_fields_statistics;
import org.apache.hadoop.hive.metastore.api.tdw_sys_table_statistics;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.TblPriv;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.metastore.Warehouse;

import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.parse.PartitionDesc;
import org.apache.hadoop.hive.ql.parse.PartitionType;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.dropTableDesc;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.util.StringUtils;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;

/**
 * The Hive class contains information about this instance of Hive. An instance
 * of Hive represents a set of data in a file system (usually HDFS) organized
 * for easy query processing
 * 
 */

public class Hive {
	
	private LogHelper console;

	static final private Log LOG = LogFactory.getLog("hive.ql.metadata.Hive");
	static Hive db = null;
	
	//added by Brantzhang begin
	/*public static final String ROOT_USER = "root";
	public static final String THE_SYSTEM = "hive";*/
	
	public enum Privilege{
		SELECT_PRIV, INSERT_PRIV, INDEX_PRIV, CREATE_PRIV, DROP_PRIV, DELETE_PRIV, ALTER_PRIV, UPDATE_PRIV, CREATE_VIEW_PRIV, SHOW_VIEW_PRIV, ALL_PRIV, DBA_PRIV
	}
		
	//added by Brantzhang end

	private HiveConf conf = null;
	private int numOfHashPar;//added by BrantZhang for hash partition
	private ThreadLocal<IMetaStoreClient> threadLocalMSC = new ThreadLocal() {
		protected synchronized Object initialValue() {
			return null;
		}

		public synchronized void remove() {
			if (this.get() != null) {
				((IMetaStoreClient) this.get()).close();
			}
			super.remove();
		}
	};

	/**
	 * Gets hive object for the current thread. If one is not initialized then a
	 * new one is created If the new configuration is different in metadata conf
	 * vars then a new one is created.
	 * 
	 * @param c
	 *            new Hive Configuration
	 * @return Hive object for current thread
	 * @throws HiveException
	 * 
	 */
	public static Hive get(HiveConf c) throws HiveException {
		boolean needsRefresh = false;

		if (db != null) {
			for (HiveConf.ConfVars oneVar : HiveConf.metaVars) {
				String oldVar = db.getConf().getVar(oneVar);
				String newVar = c.getVar(oneVar);
				if (oldVar.compareToIgnoreCase(newVar) != 0) {
					needsRefresh = true;
					break;
				}
			}
		}
		return get(c, needsRefresh);
	}

	/**
	 * get a connection to metastore. see get(HiveConf) function for comments
	 * 
	 * @param c
	 *            new conf
	 * @param needsRefresh
	 *            if true then creates a new one
	 * @return The connection to the metastore
	 * @throws HiveException
	 */
	public static Hive get(HiveConf c, boolean needsRefresh)
	throws HiveException {
		if (db == null || needsRefresh) {
			closeCurrent();
			c.set("fs.scheme.class", "dfs");
			db = new Hive(c);
		}
		return db;
	}

	public static Hive get() throws HiveException {
		if (db == null) {
			db = new Hive(new HiveConf(Hive.class));
		}
		return db;
	}

	public static void closeCurrent() {
		if (db != null) {
			db.close();
		}
	}

	/**
	 * Hive
	 * 
	 * @param argFsRoot
	 * @param c
	 * 
	 */
	private Hive(HiveConf c) throws HiveException {
		this.conf = c;
		//added by BrantZhang for hash partition Begin
        numOfHashPar = this.conf.getInt("hive.hashPartition.num", 500);
        if(numOfHashPar<=0)
        	throw new HiveException("Hash Partition Number should be Positive Integer!");
        //added by BrantZhang for hash partition End
		console = new LogHelper(LOG,c);
	}

	/**
	 * closes the connection to metastore for the calling thread
	 */
	private void close() {
		LOG.info("Closing current thread's connection to Hive Metastore.");
		db.threadLocalMSC.remove();
	}

	/**
	 * Creates a table metdata and the directory for the table data
	 * 
	 * @param tableName
	 *            name of the table
	 * @param columns
	 *            list of fields of the table
	 * @param partCols
	 *            partition keys of the table
	 * @param fileInputFormat
	 *            Class of the input format of the table data file
	 * @param fileOutputFormat
	 *            Class of the output format of the table data file
	 * @throws HiveException
	 *             thrown if the args are invalid or if the metadata or the data
	 *             directory couldn't be created
	 */
	public void createTable(String tableName, List<String> columns,
			PartitionDesc partdesc,
			Class<? extends InputFormat> fileInputFormat,
			Class<?> fileOutputFormat) throws HiveException {
		this.createTable(tableName, columns, partdesc, fileInputFormat,
				fileOutputFormat, -1, null);
	}

	/**
	 * Creates a table metdata and the directory for the table data
	 * 
	 * @param tableName
	 *            name of the table
	 * @param columns
	 *            list of fields of the table
	 * @param partCols
	 *            partition keys of the table
	 * @param fileInputFormat
	 *            Class of the input format of the table data file
	 * @param fileOutputFormat
	 *            Class of the output format of the table data file
	 * @param bucketCount
	 *            number of buckets that each partition (or the table itself)
	 *            should be divided into
	 * @throws HiveException
	 *             thrown if the args are invalid or if the metadata or the data
	 *             directory couldn't be created
	 */
	public void createTable(String tableName, List<String> columns,
			PartitionDesc partdesc,
			Class<? extends InputFormat> fileInputFormat,
			Class<?> fileOutputFormat, int bucketCount, List<String> bucketCols)
	throws HiveException {
		if (columns == null) {
			throw new HiveException("columns not specified for table "
					+ tableName);
		}

		Table tbl = new Table(tableName);
		tbl.setInputFormatClass(fileInputFormat.getName());
		tbl.setOutputFormatClass(fileOutputFormat.getName());

		for (String col : columns) {
			FieldSchema field = new FieldSchema(col,
					org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME,
			"default");
			tbl.getCols().add(field);
		}

		if (partdesc != null) {
			/*
			 * for (String partCol : partCols) { FieldSchema part = new
			 * FieldSchema(); part.setName(partCol);
			 * part.setType(org.apache.hadoop
			 * .hive.serde.Constants.STRING_TYPE_NAME); // default partition key
			 * tbl.getPartCols().add(part); }
			 */
			tbl.setPartitions(partdesc);
		}
		tbl.setSerializationLib(LazySimpleSerDe.class.getName());
		tbl.setNumBuckets(bucketCount);
		tbl.setBucketCols(bucketCols);
		createTable(tbl);
	}

	/**
	 * Updates the existing table metadata with the new metadata.
	 * 
	 * @param tblName
	 *            name of the existing table
	 * @param newTbl
	 *            new name of the table. could be the old name
	 * @throws InvalidOperationException
	 *             if the changes in metadata is not acceptable
	 * @throws TException
	 */
	public void alterTable(String tblName, Table newTbl)
	throws InvalidOperationException, HiveException {
		try {
		/*
		    String indexNumString = newTbl.getParameters().get("indexNum");
		    
		    int indexNum = 0;
		    if(indexNumString != null)
		    {
		        indexNum = Integer.valueOf(indexNumString);
		        System.out.println("indexNum:"+indexNumString);
		    }
		    
		    for(int i = 0; i < indexNum; i++)
		    {
		        String indexInfoString = newTbl.getParameters().get("index"+i);
		        if(indexInfoString != null)
		        {
		            System.out.println("indexInfo " + i +": " + indexInfoString);
		        }
		        else
		        {
		            System.out.println("index " + i + ", null");
		        }
		    }*/
		    
			getMSC().alter_table(SessionState.get().getDbName(), tblName,
					newTbl.getTTable());
		} catch (MetaException e) {
			throw new HiveException("Unable to alter table.", e);
		} catch (TException e) {
			throw new HiveException("Unable to alter table.", e);
		}
	}

	/**
	 * Creates the table with the give objects
	 * 
	 * @param tbl
	 *            a table object
	 * @throws HiveException
	 */
	public void createTable(Table tbl) throws HiveException {
		createTable(tbl, false);
	}

	/**
	 * Creates the table with the give objects
	 * 
	 * @param tbl
	 *            a table object
	 * @param ifNotExists
	 *            if true, ignore AlreadyExistsException
	 * @throws HiveException
	 */
	public void createTable(Table tbl, boolean ifNotExists)
	throws HiveException {
		try {
			tbl.initSerDe();
			if (tbl.getCols().size() == 0) {
				tbl.setFields(MetaStoreUtils.getFieldsFromDeserializer(tbl
						.getName(), tbl.getDeserializer()));
			}
			tbl.checkValidity();
			getMSC().createTable(tbl.getTTable());
		} catch (AlreadyExistsException e) {
			if (!ifNotExists) {
				throw new HiveException(e);
			}
		} catch (Exception e) {
			throw new HiveException(e);
		}
	}

	/**
	 * Drops table along with the data in it. If the table doesn't exist then it
	 * is a no-op
	 * 
	 * @param dbName
	 *            database where the table lives
	 * @param tableName
	 *            table to drop
	 * @throws HiveException
	 *             thrown if the drop fails
	 */
	public void dropTable(String dbName, String tableName) throws HiveException {
		dropTable(dbName, tableName, true, true);
	}

	/**
	 * Drops the table.
	 * 
	 * @param tableName
	 * @param deleteData
	 *            deletes the underlying data along with metadata
	 * @param ignoreUnknownTab
	 *            an exception if thrown if this is falser and table doesn't
	 *            exist
	 * @throws HiveException
	 */
	public void dropTable(String dbName, String tableName, boolean deleteData,
			boolean ignoreUnknownTab) throws HiveException {

		try {
			getMSC().dropTable(dbName, tableName, deleteData, ignoreUnknownTab);
		} catch (NoSuchObjectException e) {
			if (!ignoreUnknownTab) {
				throw new HiveException(e);
			}
		} catch (Exception e) {
			throw new HiveException(e);
		}
	}

	public void truncateTable(String dbName, String tblName)
	throws HiveException,MetaException {
	    Table tbl = getTable(dbName, tblName);
		Path tblPath = tbl.getPath();
		
		
		
		//delete and make
		getMSC().getWarehouse().deleteDir(tblPath, true);
		getMSC().getWarehouse().mkdirs(tblPath);
		
		// 如果有分区, 重新创建分区目录.
		List<Path> partPathList = null;
		List<String> partNameList = null;
		if(tbl.isPartitioned())
		{
		    partPathList = new ArrayList<Path>();
		    partNameList = new ArrayList<String>();
            partPathList.addAll(Warehouse.getPartitionPaths(tbl.getPath(), tbl.getTTable().getPriPartition().getParSpaces().keySet(), tbl.getTTable().getSubPartition() != null ? tbl.getTTable().getSubPartition().getParSpaces().keySet():null));
            for(Path path: partPathList)
            {
                getMSC().getWarehouse().mkdirs(path);
                LOG.debug("mkdir : " + path.toString());
            }
            LOG.debug("delete and create dir OK!");
            Iterator<String> iter = tbl.getTTable().getPriPartition().getParSpaces().keySet().iterator();
            while(iter.hasNext())
            {     	
                partNameList.add(iter.next());
                LOG.debug("add pri partition names : " + partNameList.get(partNameList.size() - 1));
            }
		}
		
		// drop all table index dir here,  then mkdir, index status don't change.
		try
        {
            List<IndexItem> indexItemList = getMSC().get_all_index_table(dbName, tblName);
            if(indexItemList == null || indexItemList.isEmpty())
            {
                return;
            }
        
            for(int i = 0; i < indexItemList.size(); i++)
            {
                // remove index dir
                Path indexPath = new Path(indexItemList.get(i).getLocation());
                getMSC().getWarehouse().deleteDir(indexPath, true);
                getMSC().getWarehouse().mkdirs(indexPath);
                
                if(tbl.isPartitioned())
                {
                    Set<String> partNameSet = tbl.getTTable().getPriPartition().getParSpaces().keySet();
                    List<Path> indexPartPath = getMSC().getWarehouse().getPartitionPaths(indexPath, partNameSet, null);
                    for(Path partPath: indexPartPath)
                    {
                        getMSC().getWarehouse().mkdirs(partPath);
                    }
                }
            }
        }
    	catch(Exception e)
        {
            LOG.info("drop index fail:"
                            +e.getMessage()+",db:"+dbName+",table:"+tblName + " when trunk table");
        }       
	}

	public HiveConf getConf() {
		return (conf);
	}

	/**
	 * Returns metadata of the table.
	 * 
	 * @param dbName
	 *            the name of the database
	 * @param tableName
	 *            the name of the table
	 * @return the table
	 * @exception HiveException
	 *                if there's an internal error or if the table doesn't exist
	 */
	public Table getTable(final String dbName, final String tableName)
	throws HiveException {

		return this.getTable(dbName, tableName, true);
	}

	/**
	 * Returns metadata of the table
	 * 
	 * @param dbName
	 *            the name of the database
	 * @param tableName
	 *            the name of the table
	 * @param throwException
	 *            controls whether an exception is thrown or a returns a null
	 * @return the table or if throwException is false a null value.
	 * @throws HiveException
	 */
	public Table getTable(final String dbName, final String tableName,
			boolean throwException) throws HiveException{

		if (tableName == null || tableName.equals("")) {
			throw new HiveException("empty table creation??");
		}
		
		LOG.info("getTable: DBname: " + dbName + " ,TBname: " + tableName );
		
		Table table = new Table();
		org.apache.hadoop.hive.metastore.api.Table tTable = null;
		try {
			tTable = getMSC().getTable(dbName, tableName);
		} catch (NoSuchObjectException e) {
			if (throwException) {
				LOG.error(StringUtils.stringifyException(e));
				throw new InvalidTableException("Table not found ", tableName);
			}
			return null;
		} catch (Exception e) {
			throw new HiveException("Unable to fetch table " + tableName, e);
		}
		// just a sanity check
		assert (tTable != null);
		try {

			// Use LazySimpleSerDe for MetadataTypedColumnsetSerDe.
			// NOTE: LazySimpleSerDe does not support tables with a single
			// column of col
			// of type "array<string>". This happens when the table is created
			// using an
			// earlier version of Hive.
			if (tTable
					.getSd()
					.getSerdeInfo()
					.getSerializationLib()
					.equals(
							org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.class
							.getName())
							&& tTable.getSd().getColsSize() > 0
							&& tTable.getSd().getCols().get(0).getType().indexOf('<') == -1) {
				tTable
				.getSd()
				.getSerdeInfo()
				.setSerializationLib(
						org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class
						.getName());
			}

			// first get a schema (in key / vals)
			Properties p = MetaStoreUtils.getSchema(tTable);
			table.setSchema(p);
			table.setTTable(tTable);
			table
			.setInputFormatClass((Class<? extends InputFormat<WritableComparable, Writable>>) Class
					.forName(
							table
							.getSchema()
							.getProperty(
									org.apache.hadoop.hive.metastore.api.Constants.FILE_INPUT_FORMAT,
									org.apache.hadoop.mapred.SequenceFileInputFormat.class
									.getName()), true,
									JavaUtils.getClassLoader()));
			table
			.setOutputFormatClass((Class<? extends HiveOutputFormat>) Class
					.forName(
							table
							.getSchema()
							.getProperty(
									org.apache.hadoop.hive.metastore.api.Constants.FILE_OUTPUT_FORMAT,
									HiveSequenceFileOutputFormat.class
									.getName()), true,
									JavaUtils.getClassLoader()));
			table.setDeserializer(MetaStoreUtils.getDeserializer(getConf(), p));
			table.setDataLocation(new URI(tTable.getSd().getLocation()));
		} catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
			throw new HiveException(e);
		}
		String sf = table
		.getSerdeParam(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT);
		if (sf != null) {
			char[] b = sf.toCharArray();
			if ((b.length == 1) && (b[0] < 10)) { // ^A, ^B, ^C, ^D, \t
				table
				.setSerdeParam(
						org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT,
						Integer.toString(b[0]));
			}
		}
		table.checkValidity();
		
		
		if(tTable.getSubPartition() != null)
			table.setHasSubPartition(true);
		
		return table;
	}

	public List<String> getAllTables() throws HiveException {
		return getTablesByPattern(".*");
	}

	/**
	 * returns all existing tables that match the given pattern. The matching
	 * occurs as per Java regular expressions
	 * 
	 * @param tablePattern
	 *            java re pattern
	 * @return list of table names
	 * @throws HiveException
	 */
	public List<String> getTablesByPattern(String tablePattern)
	throws HiveException {
		try {
			return getMSC().getTables(SessionState.get().getDbName(),
					tablePattern);
		} catch (Exception e) {
			throw new HiveException(e);
		}
	}

	// for testing purposes
	protected List<String> getTablesForDb(String database, String tablePattern)
	throws HiveException {
		try {
			return getMSC().getTables(database, tablePattern);
		} catch (Exception e) {
			throw new HiveException(e);
		}
	}

	/**
	 * @param name
	 * @param locationUri
	 * @return true or false
	 * @throws AlreadyExistsException
	 * @throws MetaException
	 * @throws TException
	 * @see org.apache.hadoop.hive.metastore.HiveMetaStoreClient#createDatabase(java.lang.String,
	 *      java.lang.String)
	 */
	public boolean createDatabase(String name, String locationUri)
	throws AlreadyExistsException, MetaException, TException {
		return getMSC().createDatabase(name, locationUri);
	}

	public boolean createDatabase(String name)
	throws AlreadyExistsException, MetaException, TException {
		Warehouse wh = new Warehouse(conf);

		return getMSC().createDatabase(name,wh.getDefaultDatabasePath(name).toString());
	}
	
	/**
	 * @param name
	 * @return true or false
	 * @throws MetaException
	 * @throws TException
	 * @see org.apache.hadoop.hive.metastore.HiveMetaStoreClient#dropDatabase(java.lang.String)
	 */
	public boolean dropDatabase(String name) throws MetaException,
	TException {
		return getMSC().dropDatabase(name);
	}

	public Database getDatabase(String name)throws NoSuchObjectException, MetaException, TException{
		return getMSC().getDatabase(name);
	}
	// now we do not allow user to load file to partition directly
	/**
	 * Load a directory into a Hive Table Partition - Alters existing content of
	 * the partition with the contents of loadPath. - If he partition does not
	 * exist - one is created - files in loadPath are moved into Hive. But the
	 * directory itself is not removed.
	 * 
	 * @param loadPath
	 *            Directory containing files to load into Table
	 * @param tableName
	 *            name of table to be loaded.
	 * @param partSpec
	 *            defines which partition needs to be loaded
	 * @param replace
	 *            if true - replace files in the partition, otherwise add files
	 *            to the partition
	 * @param tmpDirPath
	 *            The temporary directory.
	 */
	/*
	 * public void loadPartition(Path loadPath, String tableName,
	 * AbstractMap<String, String> partSpec, boolean replace, Path tmpDirPath)
	 * throws HiveException { Table tbl =
	 * getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName); try {
	 */
	/**
	 * Move files before creating the partition since down stream processes
	 * check for existence of partition in metadata before accessing the data.
	 * If partition is created before data is moved, downstream waiting
	 * processes might move forward with partial data
	 */
	/*
	 * FileSystem fs; Path partPath;
	 * 
	 * // check if partition exists without creating it Partition part =
	 * getPartition (tbl, partSpec, false); if (part == null) { // Partition
	 * does not exist currently. The partition name is extrapolated from // the
	 * table's location (even if the table is marked external) fs =
	 * FileSystem.get(tbl.getDataLocation(), getConf()); partPath = new
	 * Path(tbl.getDataLocation().getPath(), Warehouse.makePartName(partSpec));
	 * } else { // Partition exists already. Get the path from the partition.
	 * This will // get the default path for Hive created partitions or the
	 * external path // when directly created by user partPath =
	 * part.getPath()[0]; fs = partPath.getFileSystem(getConf()); } if(replace)
	 * { Hive.replaceFiles(loadPath, partPath, fs, tmpDirPath); } else {
	 * Hive.copyFiles(loadPath, partPath, fs); }
	 * 
	 * if (part == null) { // create the partition if it didn't exist before
	 * getPartition(tbl, partSpec, true); } } catch (IOException e) {
	 * LOG.error(StringUtils.stringifyException(e)); throw new HiveException(e);
	 * } catch (MetaException e) { LOG.error(StringUtils.stringifyException(e));
	 * throw new HiveException(e); } }
	 */

	/**
	 * Load a directory into a Hive Table. - Alters existing content of table
	 * with the contents of loadPath. - If table does not exist - an exception
	 * is thrown - files in loadPath are moved into Hive. But the directory
	 * itself is not removed.
	 * 
	 * @param loadPath
	 *            Directory containing files to load into Table
	 * @param tableName
	 *            name of table to be loaded.
	 * @param replace
	 *            if true - replace files in the table, otherwise add files to
	 *            table
	 * @param tmpDirPath
	 *            The temporary directory.
	 */
	public void loadTable(Path loadPath, String tableName, boolean replace,
			Path tmpDirPath) throws HiveException {
		Table tbl = getTable(SessionState.get().getDbName(), tableName);
		if (replace) {
			tbl.replaceFiles(loadPath, tmpDirPath);
		} else {
			tbl.copyFiles(loadPath);
		}
	}

	/**
	 * Creates a partition.
	 * 
	 * @param tbl
	 *            table for which partition needs to be created
	 * @param partSpec
	 *            partition keys and their values
	 * @return created partition object
	 * @throws HiveException
	 *             if table doesn't exist or partition already exists
	 * 
	 *             public Partition createPartition(Table tbl, Map<String,
	 *             String> partSpec) throws HiveException { return
	 *             createPartition(tbl, partSpec, null); }
	 */
	public void addPartitions(Table tbl, AddPartitionDesc apd)
	throws HiveException,MetaException,TException,InvalidOperationException{
		ArrayList<String> partToAdd = new ArrayList<String>();
		ArrayList<Path> pathToMake = new ArrayList<Path>();

		if(apd.getIsSubPartition()){
			
			PrimitiveTypeInfo pti = new PrimitiveTypeInfo();
			pti.setTypeName(tbl.getTTable().getSubPartition().getParKey().getType());
			ObjectInspector StringIO = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
			ObjectInspector ValueIO = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(pti.getPrimitiveCategory());	
			ObjectInspectorConverters.Converter converter1 = ObjectInspectorConverters.getConverter(StringIO, ValueIO);
			ObjectInspectorConverters.Converter converter2 = ObjectInspectorConverters.getConverter(StringIO, ValueIO);
			
			
			if(apd.getPartDesc().getPartitionType() == PartitionType.LIST_PARTITION){
				if(tbl.getTTable().getSubPartition().getParType().compareToIgnoreCase("LIST") != 0)
					throw new HiveException("Sub Partition is not LIST Type!");
				//TODO:metaStore should return a linkedHashMap type
				LinkedHashMap<String, List<String> > subPartSpaces =/* new */(LinkedHashMap<String, List<String> >)(tbl.getTTable().getSubPartition().getParSpaces());

				Iterator<String> itr = apd.getPartDesc().getPartitionSpaces().keySet().iterator();

				//check key,do not allow duplicate partition name;
				while(itr.hasNext()){
					String key = itr.next().toLowerCase();
					if(subPartSpaces.containsKey(key))
						throw new HiveException("table : " + tbl.getName() + " have already contain a sub parititon named: " + key);
					if(key.equalsIgnoreCase("default")){
						throw new HiveException("use : 'alter table tblname add default subpartition' to add default subpartition!");
					}
					partToAdd.add(key);
				}

				Iterator<List<String > > listItr= apd.getPartDesc().getPartitionSpaces().values().iterator();

				//check values,user should not add a list partition values already contained by other partitions;
				while(listItr.hasNext()){
					Iterator<String> valueItr = listItr.next().iterator();
					if(valueItr.hasNext()){
						String value = valueItr.next();
						
						if(converter1.convert(value) == null){
							throw new HiveException("value : " + value + " should be type of " + tbl.getTTable().getSubPartition().getParKey().getType());
						}

						Iterator<List<String>> PartValuesItr = subPartSpaces.values().iterator();
						while(PartValuesItr.hasNext()){
							if(PartValuesItr.next().contains(value))
								throw new HiveException("table : " + tbl.getName() + " have already contain a sub partition contain value: " + value);

						}
					}
				}

				//check OK,then we add the partition

				subPartSpaces.putAll(apd.getPartDesc().getPartitionSpaces());
				if(subPartSpaces.containsKey("default")){//make sure default partition is the last partition
					subPartSpaces.put("default", subPartSpaces.remove("default"));
				}
				tbl.getTTable().getSubPartition().setParSpaces(subPartSpaces);

				for(String partName:partToAdd){
					pathToMake.addAll(getMSC().getWarehouse().getSubPartitionPaths(tbl.getDbName(), partName,tbl.getTTable().getPriPartition().getParSpaces().keySet(),partName));
				}

			}
			else if(apd.getPartDesc().getPartitionType() == PartitionType.RANGE_PARTITION){
				if(tbl.getTTable().getSubPartition().getParType().compareToIgnoreCase("RANGE") != 0)
					throw new HiveException("Sub Partition is not RANGE Type!");

				//TODO:metaStore should return a linkedHashMap type
				LinkedHashMap<String, List<String> > subPartSpaces = new LinkedHashMap<String, List<String> >(tbl.getTTable().getSubPartition().getParSpaces());

				Iterator<String> itr = apd.getPartDesc().getPartitionSpaces().keySet().iterator();

				//check key,do not allow duplicate partition name;
				while(itr.hasNext()){
					String key = itr.next();
					if(subPartSpaces.containsKey(key))
						throw new HiveException("table : " + tbl.getName() + " have already contain a sub parititon named: " + key);
					partToAdd.add(key);
				}

				
				boolean isDefault=false;
				
				if(subPartSpaces.containsKey("default")){
					isDefault=true;
					subPartSpaces.remove("default");
				}
				
				
				//the partition valid
				
				Iterator<Entry<String,List<String>>>  apdItr = apd.getPartDesc().getPartitionSpaces().entrySet().iterator();
				Entry<String,List<String>> apdLastTmp = null;
				Entry<String,List<String>> apdCurTmp = null;
				while(apdItr.hasNext()){
					apdCurTmp = apdItr.next();
					if(apdLastTmp == null){
						apdLastTmp = apdCurTmp;
						continue;
					}
					else{
						if(((Comparable)converter1.convert(apdLastTmp.getValue().get(0))).compareTo(((Comparable)converter2.convert(apdCurTmp.getValue().get(0)))) < 0){
							apdLastTmp=apdCurTmp;
						}
						else
							throw new HiveException("partition : " + apdLastTmp.getKey() + " less than value should smaller than partition : " + apdCurTmp.getKey());
					}
				}
				
				
				
				
				LinkedHashMap<String,List<String>> newPartitionSpace= new LinkedHashMap<String,List<String>>();
				Iterator<Entry<String,List<String>>> oldEntryItr = subPartSpaces.entrySet().iterator();

				Iterator<Entry<String,List<String>>> newEntryItr = apd.getPartDesc().getPartitionSpaces().entrySet().iterator();

				Entry<String,List<String>> lastEntry = null;

				
				Entry<String,List<String>> newEnt = null;
				Entry<String,List<String>> oldEnt = null;
				
				boolean newEnd = false;
				boolean oldEnd = false;
				
				boolean first = true;
			
				while (true){
					//first time
					if(first){
					if(newEntryItr.hasNext()){
						newEnt = newEntryItr.next();
						
					}
					else
					{
						newEnd = true;
					}
					
					if(oldEntryItr.hasNext())
						oldEnt = oldEntryItr.next();
					else
					{
						oldEnd = true;
					}
					first = false;
					}
					
					if(newEnd == true && oldEnd == true){
						LOG.info("newEnd and oldEnd is true...break!");
						break;
					}
					
					if(newEnd == false && oldEnd == false && ((Comparable)converter1.convert(newEnt.getValue().get(0))).compareTo(((Comparable)converter2.convert(oldEnt.getValue().get(0)))) < 0){
						LOG.info("newEnt < oldEnt : " + newEnt.getKey() + "  < " + oldEnt.getKey());
						newPartitionSpace.put(newEnt.getKey(), newEnt.getValue());
						LOG.info("put partition : " + newEnt.getKey());
						
						if(newEntryItr.hasNext())
							newEnt = newEntryItr.next();
						else
						{
							newEnd = true;
						}
						
					}
					else if(newEnd == false && oldEnd == false && ((Comparable)converter1.convert(newEnt.getValue().get(0))).compareTo(((Comparable)converter2.convert(oldEnt.getValue().get(0)))) > 0){
						LOG.info("newEnt > oldEnt : " + newEnt.getKey() + "  > " + oldEnt.getKey());
						newPartitionSpace.put(oldEnt.getKey(), oldEnt.getValue());
						LOG.info("put partition : " + oldEnt.getKey());
						
						if(oldEntryItr.hasNext())
							oldEnt = oldEntryItr.next();
						else
						{
							oldEnd = true;
						}
						
					}
					else if(newEnd == false && oldEnd == false && (((Comparable)converter1.convert(newEnt.getValue().get(0))).compareTo(((Comparable)converter2.convert(oldEnt.getValue().get(0)))) == 0))
						throw new HiveException("partition : " + newEnt.getKey() + " less than value should not equal : " + oldEnt.getKey());
					
			
					if(newEnd){
						
						newPartitionSpace.put(oldEnt.getKey(), oldEnt.getValue());
						LOG.info("newEnd == true ,put old partition : " + oldEnt.getKey());
						while(oldEntryItr.hasNext()){
							oldEnt = oldEntryItr.next();
							
							newPartitionSpace.put(oldEnt.getKey(), oldEnt.getValue());
							LOG.info("newEnd == true ,put old partition : " + oldEnt.getKey());
						}
						
						oldEnd = true;
					}
					
					if(oldEnd){
						
						newPartitionSpace.put(newEnt.getKey(), newEnt.getValue());
						LOG.info("oldEnd == true ,put new partition : " + newEnt.getKey());
						while(newEntryItr.hasNext()){
							newEnt = newEntryItr.next();
							
							newPartitionSpace.put(newEnt.getKey(), newEnt.getValue());
							LOG.info("oldEnd == true ,put new partition : " + newEnt.getKey());
						}
						newEnd = true;
						
					}	
					
				}
				

				subPartSpaces = newPartitionSpace;
				
				if(isDefault){
					subPartSpaces.put("default", new ArrayList());
				}
				
				tbl.getTTable().getSubPartition().setParSpaces(subPartSpaces);

				for(String partName:partToAdd){
					pathToMake.addAll(getMSC().getWarehouse().getSubPartitionPaths(tbl.getDbName(), tbl.getName(),tbl.getTTable().getPriPartition().getParSpaces().keySet(),partName));
				}
			}



			LOG.info("DBname:" + tbl.getDbName()+ ",table: " + tbl.getName() + ",level" + tbl.getTTable().getSubPartition().getLevel());

			getMSC().setSubPartition(tbl.getDbName(), tbl.getName(), tbl.getTTable().getSubPartition());

		}

		else//for pri partitions
		{
			///////////////////////////
			PrimitiveTypeInfo pti = new PrimitiveTypeInfo();
			pti.setTypeName(tbl.getTTable().getPriPartition().getParKey().getType());
			ObjectInspector StringIO = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
			ObjectInspector ValueIO = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(pti.getPrimitiveCategory());	
			ObjectInspectorConverters.Converter converter1 = ObjectInspectorConverters.getConverter(StringIO, ValueIO);
			ObjectInspectorConverters.Converter converter2 = ObjectInspectorConverters.getConverter(StringIO, ValueIO);
			////////////////[TODO] go on for check type
			if(apd.getPartDesc().getPartitionType() == PartitionType.LIST_PARTITION){
				if(tbl.getTTable().getPriPartition().getParType().compareToIgnoreCase("LIST") != 0)
					throw new HiveException("Partition is not LIST Type!");
				
				console.DEBUG("add list type  pri partition!");
				//TODO:metaStore should return a linkedHashMap type
				LinkedHashMap<String, List<String> > priPartSpaces = new LinkedHashMap<String, List<String> >(tbl.getTTable().getPriPartition().getParSpaces());

				Iterator<String> itr = apd.getPartDesc().getPartitionSpaces().keySet().iterator();

				//check key,do not allow duplicate partition name;
				while(itr.hasNext()){
					String key = itr.next();
					if(priPartSpaces.containsKey(key))
						throw new HiveException("table : " + tbl.getName() + " have already contain a parititon named: " + key);
					partToAdd.add(key);
				}

				Iterator<List<String > > listItr= apd.getPartDesc().getPartitionSpaces().values().iterator();

				//check values,user should not add a list partition values already contained by other partitions;
				while(listItr.hasNext()){
					Iterator<String> valueItr = listItr.next().iterator();
					if(valueItr.hasNext()){
						String value = valueItr.next();
						
						if(converter1.convert(value) == null){
							throw new HiveException("value : " + value + " should be type of " + tbl.getTTable().getPriPartition().getParKey().getType());
						}

						Iterator<List<String>> PartValuesItr = priPartSpaces.values().iterator();
						while(PartValuesItr.hasNext()){
							if(PartValuesItr.next().contains(value))
								throw new HiveException("table : " + tbl.getName() + " have already contain a partition contain value: " + value);

						}
					}
				}

				//check OK,then we add the partition

				priPartSpaces.putAll(apd.getPartDesc().getPartitionSpaces());
				if(priPartSpaces.containsKey("default")){//make sure default partition is the last partition
					priPartSpaces.put("default", priPartSpaces.remove("default"));
				}
				tbl.getTTable().getPriPartition().setParSpaces(priPartSpaces);
				
				
				//Modified by Brantzhang Begin
				Set<String> subPartNames = null;
				if(tbl.getTTable().getSubPartition() != null){
					/*if(tbl.getTTable().getSubPartition().getParType().equals("hash")){
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
					}else{
						subPartNames = tbl.getTTable().getSubPartition().getParSpaces().keySet();
					}*/
					subPartNames = tbl.getTTable().getSubPartition().getParSpaces().keySet();
				} 
				for(String partName:partToAdd){
					pathToMake.addAll(getMSC().getWarehouse().getPriPartitionPaths(tbl.getDbName(), tbl.getName(), partName, subPartNames));
				}
				//Modified by Brantzhang End
			}//for range pri partition:
			else if(apd.getPartDesc().getPartitionType() == PartitionType.RANGE_PARTITION){
				if(tbl.getTTable().getPriPartition().getParType().compareToIgnoreCase("RANGE") != 0)
					throw new HiveException("Partition is not RANGE Type!");

				console.DEBUG("add range type  pri partition!");
				
				LinkedHashMap<String, List<String> > priPartSpaces = /*new*/ (LinkedHashMap<String, List<String> >)(tbl.getTTable().getPriPartition().getParSpaces());

				for(String part: priPartSpaces.keySet()){
					console.DEBUG("partitions before add : " + part);
				}
				
				boolean isDefault=false;
				
				if(priPartSpaces.containsKey("default")){
					isDefault=true;
					priPartSpaces.remove("default");
				}
				
				
				
				Iterator<String> itr = apd.getPartDesc().getPartitionSpaces().keySet().iterator();

				//check key,do not allow duplicate partition name;
				
				while(itr.hasNext()){
					String key = itr.next();
					
					console.DEBUG("partition to add :" + key);
					
					if(priPartSpaces.containsKey(key))
						throw new HiveException("table : " + tbl.getName() + " has already contain a parititon named: " + key);
					partToAdd.add(key);
				}
				
				//the partition valid
				
				Iterator<Entry<String,List<String>>>  apdItr = apd.getPartDesc().getPartitionSpaces().entrySet().iterator();
				Entry<String,List<String>> apdLastTmp = null;
				Entry<String,List<String>> apdCurTmp = null;
				while(apdItr.hasNext()){
					apdCurTmp = apdItr.next();
					if(apdLastTmp == null){
						apdLastTmp = apdCurTmp;
						continue;
					}
					else{
						if(((Comparable)converter1.convert(apdLastTmp.getValue().get(0))).compareTo(((Comparable)converter2.convert(apdCurTmp.getValue().get(0)))) < 0){
							apdLastTmp=apdCurTmp;
						}
						else
							throw new HiveException("partition : " + apdLastTmp.getKey() + " less than value should smaller than partition : " + apdCurTmp.getKey());
					}
				}

				LinkedHashMap<String,List<String>> newPartitionSpace= new LinkedHashMap<String,List<String>>();
				Iterator<Entry<String,List<String>>> oldEntryItr = priPartSpaces.entrySet().iterator();

				Iterator<Entry<String,List<String>>> newEntryItr = apd.getPartDesc().getPartitionSpaces().entrySet().iterator();

				Entry<String,List<String>> lastEntry = null;
				
				LOG.info(apd.getPartDesc().getPartitionSpaces().size());
				
				
				Entry<String,List<String>> newEnt = null;
				Entry<String,List<String>> oldEnt = null;
				
				boolean newEnd = false;
				boolean oldEnd = false;
				
				boolean first = true;
			
				while (true){
					//first time
					if(first){
					if(newEntryItr.hasNext()){
						newEnt = newEntryItr.next();
						
					}
					else
					{
						newEnd = true;
					}
					
					if(oldEntryItr.hasNext())
						oldEnt = oldEntryItr.next();
					else
					{
						oldEnd = true;
					}
					first = false;
					}
					
					if(newEnd == true && oldEnd == true){
						LOG.info("newEnd and oldEnd is true...break!");
						break;
					}
					
					if(newEnd == false && oldEnd == false && ((Comparable)converter1.convert(newEnt.getValue().get(0))).compareTo(((Comparable)converter2.convert(oldEnt.getValue().get(0)))) < 0){
						LOG.info("newEnt < oldEnt : " + newEnt.getKey() + "  < " + oldEnt.getKey());
						newPartitionSpace.put(newEnt.getKey(), newEnt.getValue());
						LOG.info("put partition : " + newEnt.getKey());
						
						if(newEntryItr.hasNext())
							newEnt = newEntryItr.next();
						else
						{
							newEnd = true;
						}
						
					}
					else if(newEnd == false && oldEnd == false && ((Comparable)converter1.convert(newEnt.getValue().get(0))).compareTo(((Comparable)converter2.convert(oldEnt.getValue().get(0)))) > 0){
						LOG.info("newEnt > oldEnt : " + newEnt.getKey() + "  > " + oldEnt.getKey());
						newPartitionSpace.put(oldEnt.getKey(), oldEnt.getValue());
						LOG.info("put partition : " + oldEnt.getKey());
						
						if(oldEntryItr.hasNext())
							oldEnt = oldEntryItr.next();
						else
						{
							oldEnd = true;
						}
						
					}
					else if(newEnd == false && oldEnd == false && (((Comparable)converter1.convert(newEnt.getValue().get(0))).compareTo(((Comparable)converter2.convert(oldEnt.getValue().get(0)))) == 0))
						throw new HiveException("partition : " + newEnt.getKey() + " less than value should not equal : " + oldEnt.getKey());
					
			
					if(newEnd){
						
						newPartitionSpace.put(oldEnt.getKey(), oldEnt.getValue());
						LOG.info("newEnd == true ,put old partition : " + oldEnt.getKey());
						while(oldEntryItr.hasNext()){
							oldEnt = oldEntryItr.next();
							
							newPartitionSpace.put(oldEnt.getKey(), oldEnt.getValue());
							LOG.info("newEnd == true ,put old partition : " + oldEnt.getKey());
						}
						
						oldEnd = true;
					}
					
					if(oldEnd){
						
						newPartitionSpace.put(newEnt.getKey(), newEnt.getValue());
						LOG.info("oldEnd == true ,put new partition : " + newEnt.getKey());
						while(newEntryItr.hasNext()){
							newEnt = newEntryItr.next();
							
							newPartitionSpace.put(newEnt.getKey(), newEnt.getValue());
							LOG.info("oldEnd == true ,put new partition : " + newEnt.getKey());
						}
						newEnd = true;
						
					}	
					
				}
				

				priPartSpaces = newPartitionSpace;
				
//				if(priPartSpaces.containsKey("default")){//make sure default partition is the last partition
//					priPartSpaces.put("default", priPartSpaces.remove("default"));
//				}
				
				if(isDefault){
					priPartSpaces.put("default", new ArrayList());
				}
				
				for(String part: newPartitionSpace.keySet()){
					console.DEBUG("partitions after add : " + part);
				}
				
				tbl.getTTable().getPriPartition().setParSpaces(priPartSpaces);

				//Modified by Brantzhang Begin
				Set<String> subPartNames = null;
				if(tbl.getTTable().getSubPartition() != null){
					/*if(tbl.getTTable().getSubPartition().getParType().equals("hash")){
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
					}else{
						subPartNames = tbl.getTTable().getSubPartition().getParSpaces().keySet();
					}*/
					subPartNames = tbl.getTTable().getSubPartition().getParSpaces().keySet();
				}
				for(String partName:partToAdd){
					pathToMake.addAll(getMSC().getWarehouse().getPriPartitionPaths(tbl.getDbName(), tbl.getName(), partName, subPartNames));
				}
				//Modified by Brantzhang End

			}
			
			
			if(conf.getBoolVar(HiveConf.ConfVars.DEBUG)){
				console.DEBUG("partition type: " + tbl.getTTable().getPriPartition().getParType() + " ,partition column: " + tbl.getTTable().getPriPartition().getParKey().getName());
				for(String part:tbl.getTTable().getPriPartition().getParSpaces().keySet())
					console.DEBUG("partition name: " + part);
			}

			getMSC().setPriPartition(tbl.getDbName(), tbl.getName(), tbl.getTTable().getPriPartition());

		}

		for(Path path:pathToMake){
			//console.DEBUG("make partition dir:" + path);
			getMSC().getWarehouse().mkdirs(path);
		
		}

	}
	
	public void addDefalutSubPartition(Table tbl)
	throws HiveException,MetaException,TException,InvalidOperationException{
		tbl.getTTable().getSubPartition().getParSpaces().put("default", new ArrayList<String>());
		
		getMSC().setSubPartition(tbl.getDbName(), tbl.getName(), tbl.getTTable().getSubPartition());
		
		List<Path> pathToMake = Warehouse.getSubPartitionPaths(tbl.getPath(), tbl.getTTable().getPriPartition().getParSpaces().keySet(), "default");
		
		for(Path path:pathToMake){
			console.DEBUG("make partition dir:" + path);
			getMSC().getWarehouse().mkdirs(path);
		
		}
	}
	
	public void addDefalutPriPartition(Table tbl)
	throws HiveException,MetaException,TException,InvalidOperationException{
		tbl.getTTable().getPriPartition().getParSpaces().put("default", new ArrayList<String>());
		
		getMSC().setPriPartition(tbl.getDbName(), tbl.getName(), tbl.getTTable().getPriPartition());
		
		List<Path> pathToMake = Warehouse.getPriPartitionPaths(tbl.getPath(), "default", (tbl.getTTable().getSubPartition() != null ? tbl.getTTable().getSubPartition().getParSpaces().keySet() : null));
		
		for(Path path:pathToMake){
			console.DEBUG("make partition dir:" + path);
			getMSC().getWarehouse().mkdirs(path);	
		}
	}
	
	
	public void truncatePartition(Table tbl,String pri,String sub)
	throws HiveException,MetaException,TException,InvalidOperationException{
		List<Path> partPaths;
		if(pri != null && sub != null){//清空叶子分区
			//delete and make
			getMSC().getWarehouse().deleteDir(Warehouse.getPartitionPath(tbl.getPath(), pri, sub), true);
			
			getMSC().getWarehouse().mkdirs(Warehouse.getPartitionPath(tbl.getPath(),pri,sub));
			
		}
		else if(pri != null){//清空一级分区
			//Modified by Brantzhang for hash partition Begin
			Set<String> subPartNames = null;
			if(tbl.getTTable().getSubPartition() != null){
				if(tbl.getTTable().getSubPartition().getParType().equals("hash")){
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
				}else{
					subPartNames = tbl.getTTable().getSubPartition().getParSpaces().keySet();
				}
			} 
			
			partPaths = Warehouse.getPriPartitionPaths(tbl.getPath(), pri, subPartNames);
			
			for(Path p : partPaths){
				getMSC().getWarehouse().deleteDir(p, true);
			}
			
			for(Path p : partPaths){
				getMSC().getWarehouse().mkdirs(p);
			}
			//Modified by Brantzhang for hash partition End
			
			// add by konten for index begin
			
			/**
             *  删除一级分区对应的索引目录, 如果存在的话.  二级分区不需要处理.
             */              
              List<Path> indexPath2Delete = new ArrayList<Path>();
              List<IndexItem> indexItemList = getMSC().get_all_index_table(tbl.getDbName(), tbl.getName());
              if(indexItemList != null)
              {
                  for(int i = 0; i < indexItemList.size(); i++)
                  {
                      String indexName = indexItemList.get(i).getName();
                      String indexPathName = getMSC().getWarehouse().getDefaultIndexPath(tbl.getDbName(), tbl.getName(), indexName).toString();
                      
                      indexPath2Delete.add(new Path(indexPathName, pri));                        
                  }
              } 
              
              for(Path path:indexPath2Delete){
                  getMSC().getWarehouse().deleteDir(path, true);
              }
              for(Path path:indexPath2Delete){
                  getMSC().getWarehouse().mkdirs(path);
              }
              // add by konten for index end
		}
		else{//清空二级分区
			
				partPaths = Warehouse.getSubPartitionPaths(tbl.getPath(), tbl.getTTable().getPriPartition().getParSpaces().keySet(), sub);
				
				for(Path p : partPaths){
					getMSC().getWarehouse().deleteDir(p, true);
				}
				
				for(Path p : partPaths){
					getMSC().getWarehouse().mkdirs(p);
				}
			
		}
	}

	/**
	 * Creates a partition
	 * 
	 * @param tbl
	 *            table for which partition needs to be created
	 * @param partSpec
	 *            partition keys and their values
	 * @param location
	 *            location of this partition
	 * @return created partition object
	 * @throws HiveException
	 *             if table doesn't exist or partition already exists
	 */
	/*
	 * public Partition createPartition(Table tbl, Map<String, String> partSpec,
	 * Path location) throws HiveException {
	 * 
	 * org.apache.hadoop.hive.metastore.api.Partition partition = null;
	 * 
	 * try { Partition tmpPart = new Partition(tbl, partSpec, location);
	 * partition = getMSC().add_partition(tmpPart.getTPartition()); } catch
	 * (Exception e) { LOG.error(StringUtils.stringifyException(e)); throw new
	 * HiveException(e); }
	 * 
	 * return new Partition(tbl, partition); }
	 */

	/**
	 * Returns partition metadata
	 * 
	 * @param tbl
	 *            the partition's table
	 * @param partSpec
	 *            partition keys and values
	 * @param forceCreate
	 *            if this is true and partition doesn't exist then a partition
	 *            is created
	 * @return result partition object or null if there is no partition
	 * @throws HiveException
	 */

	/*
	 * public PartitionDesc getPartition(Table tbl, Map<String, String>
	 * partSpec, boolean forceCreate) throws HiveException {
	 * 
	 * if(!tbl.isValidSpec(partSpec)) { throw new
	 * HiveException("Invalid partition: " + partSpec); } List<String> pvals =
	 * new ArrayList<String>(); for (FieldSchema field : tbl.getPartCols()) {
	 * String val = partSpec.get(field.getName()); if(val == null ||
	 * val.length() == 0) { throw new HiveException("Value for key " +
	 * field.getName() + " is null or empty"); } pvals.add(val); }
	 * org.apache.hadoop.hive.metastore.api.Partition tpart = null; try { tpart
	 * = getMSC().getPartition(tbl.getDbName(), tbl.getName(), pvals); if(tpart
	 * == null && forceCreate) { LOG.debug("creating partition for table " +
	 * tbl.getName() + " with partition spec : " + partSpec); tpart =
	 * getMSC().appendPartition(tbl.getDbName(), tbl.getName(), pvals);; }
	 * if(tpart == null){ return null; } } catch (Exception e) {
	 * LOG.error(StringUtils.stringifyException(e)); throw new HiveException(e);
	 * } return new Partition(tbl, tpart); }
	 */

	public boolean dropPartition(dropTableDesc dropTbl,boolean deleteData) throws HiveException {
		try {

			Table tbl = getTable(SessionState.get().getDbName(), dropTbl.getTableName());
			Boolean isSub = dropTbl.getIsSub();



			org.apache.hadoop.hive.metastore.api.Partition PriParts = tbl.getTTable().getPriPartition();
			org.apache.hadoop.hive.metastore.api.Partition SubParts = null;
			ArrayList<Path> PathToDel = new ArrayList<Path>();

			if(isSub){//如果要删除的是某些子分区
				SubParts = tbl.getTTable().getSubPartition();

				for(String str:dropTbl.getPartitionNames()){
					PathToDel.addAll(getMSC().getWarehouse().getSubPartitionPaths(tbl.getDbName(), tbl.getName(), PriParts.getParSpaces().keySet(), str));
				}
				for(String part:dropTbl.getPartitionNames()){
					SubParts.getParSpaces().remove(part);
				}

				getMSC().setSubPartition(tbl.getDbName(),tbl.getName(),SubParts);
			}
			else{//如果要删除的是某些一级分区

				for(String str:dropTbl.getPartitionNames()){
					PathToDel.add(getMSC().getWarehouse().getPartitionPath(tbl.getDbName(), tbl.getName(), str));
				}

				for(String part:dropTbl.getPartitionNames()){
					PriParts.getParSpaces().remove(part);
				}

				getMSC().setPriPartition(tbl.getDbName(), tbl.getName(), PriParts);

				/**
				 *  删除一级分区对应的索引目录, 如果存在的话.  二级分区不需要处理.
				 */				 
                  List<Path> indexPath2Delete = new ArrayList<Path>();
                  List<IndexItem> indexItemList = getMSC().get_all_index_table(tbl.getDbName(), tbl.getName());
                  if(indexItemList != null)
                  {
                      for(int i = 0; i < indexItemList.size(); i++)
                      {
                          String indexName = indexItemList.get(i).getName();
                          String indexPathName = getMSC().getWarehouse().getDefaultIndexPath(tbl.getDbName(), tbl.getName(), indexName).toString();
                          
                          /*
                           * 对每个index,都需要删除相应的分区.
                           */
                          for(int k = 0; k < dropTbl.getPartitionNames().size(); k++)
                          {
                              indexPath2Delete.add(new Path(indexPathName, dropTbl.getPartitionNames().get(k)));
                          }   
                      }
                  }	
                  
                  for(Path path:indexPath2Delete){
                      getMSC().getWarehouse().deleteDir(path, true);
                  }
			}

			for(Path path:PathToDel){
				getMSC().getWarehouse().deleteDir(path, true);
			}

			
			return true;

			/*

			for(String str: dropTbl.getPartitionNames()){
				if(LevelParts.getParSpaces().containsKey(str)){
					if(LevelParts.getParSpaces().remove(str) != null){

						//TODO:get path from storagedesc or build path manu?
								Path partPath = getMSC().getWarehouse().getPartitionPath(tbl.getDbName(),tbl.getName(), str);

								getMSC().getWarehouse().deleteDir(partPath, true);

					}
				}

				else
					console.printInfo("Partition " + str + " does not exist.");
			}


			;
			List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getMSC()
			.getPartitions(db_name, tbl_name);
			// [TODO] delete partition
			 * */


		} catch (HiveException e) {
			throw new HiveException("Partition or table doesn't exist.", e);
		} catch (Exception e) {
			throw new HiveException("Unknow error. Please check logs.", e);
		}

	}

	public ArrayList<ArrayList<String>> getPartitionNames(String dbName, Table tbl,
			short max) throws HiveException {
		List<String> priNames = null;
		List<String> subNames = null;
		ArrayList<ArrayList<String>> rt = null;
		try {
			System.out.println("Get partitions from db " + dbName + ", table " + tbl.getName());
			List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getMSC()
			.getPartitions(dbName,tbl.getName());
			if (partitions == null || partitions.get(0) == null) {
				throw new HiveException("table " + tbl.getName()
						+ " do not partitioned");
			}
			
			rt = new ArrayList<ArrayList<String>>();
			//priNames = new ArrayList<String>();
			
			//Modified By Brantzhang Begin
			rt.add(new ArrayList<String>());
			if(partitions.get(0).getParType().equals("hash")){
				Set<String> partNames = new TreeSet<String>();
				partNames.add("hash("+numOfHashPar+")");
				rt.get(0).addAll(partNames);
				/*for(int i = 0; i < numOfHashPar; i++){
					if(i < 10)
						partNames.add("Hash_" + "000" + i);
					else if(i < 100)
						partNames.add("Hash_" + "00" + i);
					else if(i < 1000)
						partNames.add("Hash_" + "0" + i);
					else partNames.add("Hash_" + i);
				}*/
			}else
				rt.get(0).addAll(partitions.get(0).getParSpaces().keySet());
			
			
			//rt.add(priNames);
			
			if(partitions.get(1) != null){
				//subNames = new ArrayList<String>();
				rt.add(new ArrayList<String>());
				if(partitions.get(1).getParType().equals("hash")){
					Set<String> partNames = new TreeSet<String>();
					partNames.add("hash("+numOfHashPar+")");
					rt.get(1).addAll(partNames);
					/*for(int i = 0; i < numOfHashPar; i++){
						if(i < 10)
							partNames.add("Hash_" + "000" + i);
						else if(i < 100)
							partNames.add("Hash_" + "00" + i);
						else if(i < 1000)
							partNames.add("Hash_" + "0" + i);
						else partNames.add("Hash_" + i);
					}*/
				}else
					rt.get(1).addAll(partitions.get(1).getParSpaces().keySet());
				//rt.add(subNames);
			}					
			// names = getMSC().listPartitionNames(dbName, tblName, max);
			//Modified by Brantzhang End
		} catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
			throw new HiveException(e);
		}
		return rt;
	}

	/**
	 * get all the partitions that the table has
	 * 
	 * @param tbl
	 *            object for which partition is needed
	 * @return list of partition objects
	 * @throws HiveException
	 */
	public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitions(Table tbl) throws HiveException,MetaException,TException,NoSuchObjectException {
		
		
		/*
		 * if(tbl.isPartitioned()) {
		 * List<org.apache.hadoop.hive.metastore.api.Partition> tParts; try {
		 * tParts = getMSC().listPartitions(tbl.getDbName(), tbl.getName(),
		 * (short) -1); } catch (Exception e) {
		 * LOG.error(StringUtils.stringifyException(e)); throw new
		 * HiveException(e); } List<Partition> parts = new
		 * ArrayList<Partition>(tParts.size()); for
		 * (org.apache.hadoop.hive.metastore.api.Partition tpart : tParts) {
		 * parts.add(new Partition(tbl, tpart)); } return parts; } else { //
		 * create an empty partition. // HACK, HACK. SemanticAnalyzer code
		 * requires that an empty partition when the table is not partitioned
		 * org.apache.hadoop.hive.metastore.api.Partition tPart = new
		 * org.apache.hadoop.hive.metastore.api.Partition();
		 * tPart.setSd(tbl.getTTable().getSd()); // TODO: get a copy Partition
		 * part = new Partition(tbl, tPart); ArrayList<Partition> parts = new
		 * ArrayList<Partition>(1); parts.add(part); return parts; }
		 */
		
		//PartitionDesc partdesc = new partitionDesc();
		
		return getMSC().getPartitions(tbl.getDbName(), tbl.getName());
	}

	static private void checkPaths(FileSystem fs, FileStatus[] srcs,
			Path destf, boolean replace) throws HiveException {
		try {
			for (int i = 0; i < srcs.length; i++) {
				FileStatus[] items = fs.listStatus(srcs[i].getPath());
				for (int j = 0; j < items.length; j++) {

					if (Utilities.isTempPath(items[j])) {
						// This check is redundant because temp files are
						// removed by execution layer before
						// calling loadTable/Partition. But leaving it in just
						// in case.
						fs.delete(items[j].getPath(), true);
						continue;
					}
					// modified by guosijie
					/**
					if (items[j].isDir()) {
						throw new HiveException("checkPaths: "
								+ srcs[i].toString() + " has nested directory"
								+ items[j].toString());
					}
					**/
					// end modification
					Path tmpDest = new Path(destf, items[j].getPath().getName());
					if (!replace && fs.exists(tmpDest) && fs.isFile(tmpDest)) {
						throw new HiveException("checkPaths: " + tmpDest
								+ " already exists");
					}
				}
			}
		} catch (IOException e) {
			throw new HiveException(
					"checkPaths: filesystem error in check phase", e);
		}
	}

	static protected void copyFiles(Path srcf, Path destf, FileSystem fs)
	throws HiveException {
		try {
			// create the destination if it does not exist
			if (!fs.exists(destf))
				fs.mkdirs(destf);
		} catch (IOException e) {
			throw new HiveException(
					"copyFiles: error while checking/creating destination directory!!!",
					e);
		}

		FileStatus[] srcs;
		try {
			srcs = fs.globStatus(srcf);
		} catch (IOException e) {
			LOG.error(StringUtils.stringifyException(e));
			throw new HiveException(
					"addFiles: filesystem error in check phase", e);
		}
		if (srcs == null) {
			LOG.info("No sources specified to move: " + srcf);
			return;
			// srcs = new FileStatus[0]; Why is this needed?
		}
		// check that source and target paths exist
		checkPaths(fs, srcs, destf, false);

		// move it, move it
		try {
			for (int i = 0; i < srcs.length; i++) {
			  copyDir(srcs[i].getPath(), destf, fs);
			}
		} catch (IOException e) {
			throw new HiveException("copyFiles: error while moving files!!!", e);
		}
	}
	
	private static void copyDir(Path srcf, Path destf, FileSystem fs) throws IOException {
	  FileStatus[] items = fs.listStatus(srcf);
	  for (int i=0; i<items.length; i++) {
	    if (items[i].isDir()) {
	      copyDir(items[i].getPath(), new Path(destf, items[i].getPath().getName()), fs);
	    } else {
	      fs.rename(items[i].getPath(), new Path(destf, items[i].getPath().getName()));
	    }
	  }
	}

	/**
	 * Replaces files in the partition with new data set specifed by srcf. Works
	 * by moving files
	 * 
	 * @param srcf
	 *            Files to be moved. Leaf Directories or Globbed File Paths
	 * @param destf
	 *            The directory where the final data needs to go
	 * @param fs
	 *            The filesystem handle
	 * @param tmppath
	 *            Temporary directory
	 */
	static protected void replaceFiles(Path srcf, Path destf, FileSystem fs,
			Path tmppath) throws HiveException {
		FileStatus[] srcs;
		try {
			srcs = fs.globStatus(srcf);
		} catch (IOException e) {
			throw new HiveException(
					"addFiles: filesystem error in check phase", e);
		}
		if (srcs == null) {
			LOG.info("No sources specified to move: " + srcf);
			return;
			// srcs = new FileStatus[0]; Why is this needed?
		}
		checkPaths(fs, srcs, destf, true);

		try {
			fs.mkdirs(tmppath);
			for (int i = 0; i < srcs.length; i++) {
				FileStatus[] items = fs.listStatus(srcs[i].getPath());
				for (int j = 0; j < items.length; j++) {
					if (!fs.rename(items[j].getPath(), new Path(tmppath,
							items[j].getPath().getName()))) {
						throw new HiveException("Error moving: "
								+ items[j].getPath() + " into: " + tmppath);
					}
				}
			}

			// point of no return
			boolean b = fs.delete(destf, true);
			LOG.info("Deleting:" + destf.toString() + ",Status:" + b);

			// create the parent directory otherwise rename can fail if the
			// parent doesn't exist
			if (!fs.mkdirs(destf.getParent())) {
				throw new HiveException(
						"Unable to create destination directory: "
						+ destf.getParent().toString());
			}

			b = fs.rename(tmppath, destf);
			if (!b) {
				throw new HiveException(
						"Unable to move results to destination directory: "
						+ destf.getParent().toString());
			}
			LOG.info("Renaming:" + tmppath.toString() + ",Status:" + b);

		} catch (IOException e) {
			throw new HiveException(
					"replaceFiles: error while moving files!!!", e);
		} finally {
			try {
				fs.delete(tmppath, true);
			} catch (IOException e) {
				LOG.warn("Unable delete path " + tmppath, e);
			}
		}
	}

	/**
	 * Creates a metastore client. Currently it creates only JDBC based client
	 * as File based store support is removed
	 * 
	 * @returns a Meta Store Client
	 * @throws HiveMetaException
	 *             if a working client can't be created
	 */
	private IMetaStoreClient createMetaStoreClient() throws MetaException {
		return new HiveMetaStoreClient(this.conf);
	}

	/**
	 * 
	 * @return the metastore client for the current thread
	 * @throws MetaException
	 */
	//TODO:should change thrift for getWarehouse(), type cast is not well 
	private HiveMetaStoreClient getMSC() throws MetaException {
		IMetaStoreClient msc = threadLocalMSC.get();
		if (msc == null) {
			msc = this.createMetaStoreClient();
			threadLocalMSC.set(msc);
		}
		return (HiveMetaStoreClient)msc;
	}

	public static List<FieldSchema> getFieldsFromDeserializer(String name,
			Deserializer serde) throws HiveException {
		try {
			return MetaStoreUtils.getFieldsFromDeserializer(name, serde);
		} catch (SerDeException e) {
			throw new HiveException("Error in getting fields from serde. "
					+ e.getMessage(), e);
		} catch (MetaException e) {
			throw new HiveException("Error in getting fields from serde."
					+ e.getMessage(), e);
		}
	}
	  // joeyli added for statics information collection begin
	  
	  public tdw_sys_table_statistics add_table_statistics(tdw_sys_table_statistics new_table_statistics) throws AlreadyExistsException, MetaException, TException{
		  return getMSC().add_table_statistics(new_table_statistics);
	  }

	  public boolean delete_table_statistics(String table_statistics_name) throws NoSuchObjectException, MetaException, TException{
		  return getMSC().delete_table_statistics(table_statistics_name);
	  }

	  public tdw_sys_table_statistics get_table_statistics(String table_statistics_name) throws MetaException, TException{
		  return getMSC().get_table_statistics(table_statistics_name);
	  }

	  public List<tdw_sys_table_statistics> get_table_statistics_multi(int max_parts) throws NoSuchObjectException, MetaException, TException{
		  return getMSC().get_table_statistics_multi(max_parts);
	  }

	  public List<String> get_table_statistics_names(int max_parts) throws NoSuchObjectException, MetaException, TException{
		  return getMSC().get_table_statistics_names(max_parts);
	  }

	  public tdw_sys_fields_statistics add_fields_statistics(tdw_sys_fields_statistics new_fields_statistics) throws AlreadyExistsException, MetaException, TException{
		  return getMSC().add_fields_statistics(new_fields_statistics);
	  }

	  public boolean delete_fields_statistics(String table_statistics_name,String fields_statistics_name) throws NoSuchObjectException, MetaException, TException{
		  return getMSC().delete_fields_statistics(table_statistics_name,fields_statistics_name);
	  }

	  public tdw_sys_fields_statistics get_fields_statistics(String table_statistics_name,String fields_statistics_name) throws MetaException, TException{
		  return getMSC().get_fields_statistics(table_statistics_name,fields_statistics_name);
	  }

	  public List<tdw_sys_fields_statistics> get_fields_statistics_multi(String table_statistics_name,int max_parts) throws NoSuchObjectException, MetaException, TException{
		  return getMSC().get_fields_statistics_multi(table_statistics_name,max_parts);
	  }

	  public List<String> get_fields_statistics_names(String table_statistics_name,int max_parts) throws NoSuchObjectException, MetaException, TException{
		  return getMSC().get_fields_statistics_names(table_statistics_name,max_parts);
	  }
	  
	  public enum skewstat
	  {
	    unknow,
	    noskew,
	    skew
	  };
	  
	  public enum mapjoinstat
	  {
	    unknow,
	    canmapjoin,
	    cannotmapjoin
	  };	  
	  
	  static final double skewfactor=0.1;
	  static final int MapJoinSizeLimit=1024*1024*200;	  
	  
	  public skewstat getSkew(String table_statistics_name,String fields_statistics_name)
	  {
		  try {
			  
			  tdw_sys_fields_statistics t=get_fields_statistics(table_statistics_name,fields_statistics_name);
			  if (t==null){
				  console.printInfo(table_statistics_name+"."+fields_statistics_name+" has no stat info");
				  return skewstat.unknow;
			  }
			  String mcvnums[]=t.getStat_numbers_1().split("\1");
			  String mcvvalues[]=t.getStat_values_1().split("\1");
			  int index=-1;
			  for (String str: mcvnums )
			  {
				  index++;
				  if(!str.trim().equals(""))
				  {	
					if (Double.valueOf(str.trim()).doubleValue()>skewfactor)
					{
						//console.printInfo(mcvvalues[index]+"="+mcvnums[index]);
						return skewstat.skew;
					}
				    
				  }
				  
			  }
			  
			  
		  }catch(Exception e){
		    	return skewstat.unknow;
		    }
		  
		  return skewstat.noskew;
      }
	  
	  public mapjoinstat canTableMapJoin(String table_statistics_name)
	  {
		  try {
			  
			  tdw_sys_table_statistics t=get_table_statistics(table_statistics_name);
			  if (t!=null){
   			     if(t.getStat_total_size()< MapJoinSizeLimit && t.getStat_total_size()>0)
			     {
   			    	return mapjoinstat.canmapjoin;
			     }
   			     else
   			     {
   			    	return mapjoinstat.cannotmapjoin;
   			     }
			  }
			  else
			  {
				  console.printInfo(table_statistics_name+" has no stat info");
				  return mapjoinstat.unknow;
			  }
			  
		  }catch(Exception e){
			  return mapjoinstat.unknow;
		    }	
		  
	  }
	  // joeyli added for statics information collection end

	// added by BrantZhang for authorization begin
	  
	  //for user table
	  public boolean createUser(String byWho, String newUser, String passwd) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  newUser = newUser.trim().toLowerCase();
		  
		  if(newUser.equals(HiveMetaStore.ROOT_USER)){
			  throw new HiveException("Can not create the ROOT user!");
		  }
		  
		  if(isARole(newUser)){
			  throw new HiveException("Fail to create the new user! There is a role with the same name!");
		  }
		  
		  boolean byDBA = isDBAUser(byWho);
		  if(byDBA){//create the user
			  if(newUser.length()>16 || newUser.length()<1)
				  throw new HiveException("Length of the new user's name must be between 1 and 16 !");
			  if(passwd.length()>41 || passwd.length()<1)
				  throw new HiveException("Length of the passwd must be between 1 and 41 !");
			  try{
				  return getMSC().create_user(byWho, newUser, passwd);
			  }catch (AlreadyExistsException e){
				  throw new HiveException("The user already exists! Can not recreate it!");
			  }catch (Exception e){
				  throw new HiveException("Fail to create the new user: " + newUser);
			  }
		  }else {
			  throw new HiveException("Only DBAs have the privilege to create a new user!");
		  }
			  
	}
	  public boolean dropUsers(String byWho, List<String> users) throws HiveException{
		  boolean success = true;
		  for(String user: users){
			  success &= dropUser(byWho, user);
		  }
		  return success;
	  }
	  
	  private boolean dropUser(String byWho, String userName) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  userName = userName.trim().toLowerCase();
		  
		  if(userName.equals(HiveMetaStore.ROOT_USER))
			  throw new HiveException("Can not drop the ROOT user!");
		  
		  boolean byRoot = byWho.equals(HiveMetaStore.ROOT_USER);
		  boolean byDBA = isDBAUser(byWho);
		  boolean dropDBA = isDBAUser(userName);
		  
		  if(dropDBA){
			  if(byRoot){
				  try{
					  return getMSC().drop_user(byWho, userName);
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to drop the user: " + userName + "! The user does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to drop the user: " + userName + "!" + e.toString());
				  }
			  }else 
				  throw new HiveException("Only the ROOT user can drop a DBA user!");
		  }
		  
		  if(byDBA){
			  try{
				  return getMSC().drop_user(byWho, userName);
			  }catch(NoSuchObjectException e){
				  throw new HiveException("Fail to drop the user: " + userName + "! The user does not exist!");
			  }catch(Exception e){
				  throw new HiveException("Fail to drop the user: " + userName + "!" + org.apache.hadoop.util.StringUtils.stringifyException(e));
			  }
		  }else
			  throw new HiveException("Only DBAs can drop users!");
		  
	  }
	  
	  private User getUser(String byWho, String userName) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  userName = userName.trim().toLowerCase();
		  
		  try{
			  return getMSC().get_user(byWho, userName);
		  }catch(NoSuchObjectException e){
			  //LOG.info(StringUtils.stringifyException(e));
			  throw new HiveException("Fail to get information of the user: " + userName + "! The user does not exist!");
			  
		  }catch(Exception e){
			  throw new HiveException("Fail to get information of the user: " + userName + "!");
		  }
	  }
	  
	  public List<String>  showUsers(String byWho) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  try{
			  return getMSC().get_users_all(byWho);
		  }catch(Exception e){
			  throw new HiveException("Failed to get all the users' names! Please check the log.");
		  }
	  }
	  									  
	  public boolean setPasswd(String byWho, String userName, String newPasswd) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  if(userName == null){
			  userName = byWho;
		  }else{
			  userName = userName.trim().toLowerCase();
		  }
		  
		  if(newPasswd.length()<1 || newPasswd.length()>41)
			  throw new HiveException("The length of the new passwd must be between 1 and 41!");
		  		  
		  if(userName.equals(HiveMetaStore.ROOT_USER)){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					  return getMSC().set_passwd(byWho, userName, newPasswd);
				  }catch(Exception e){
					  throw new HiveException("Failed to set passwd for the ROOT user! Please check the log.");
				  }
			  }else
				  throw new HiveException("You have no privilege to set the passwd for the ROOT user!");
		  }
			  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = isDBAUser(userName);
		  
		  if(forDBA){
			  if(byWho.equals(userName) || byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					  return getMSC().set_passwd(byWho, userName, newPasswd);
				  }catch(Exception e){
					  throw new HiveException("Failed to set passwd for the user: " + userName + " ! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the DBA user himself or the ROOT user has the privilege to set the passwd for the DBA user: " + userName + " !");
			  }
		  }
		  
		  if(byWho.equals(userName) || byDBA){
			  try{
				  return getMSC().set_passwd(byWho, userName, newPasswd);
			  }catch(Exception e){
				  throw new HiveException("Failed to set passwd for the user: " + userName + " ! Please check the log.");
			  }
		  }else{
			  throw new HiveException("Only the user himself or DBAs have the privilege to set passwd for the user: " + userName + " !");
		  }
	  }
	  									  
	  public boolean isAUser(String userName, String passwd) throws HiveException{
		  try{
			  return getMSC().is_a_user(userName, passwd);
		  }catch(Exception e){
			  throw new HiveException("Failed to judge the user: " + userName + " ! Please check the log.");
		  }
	  }
	  
	  private boolean isAUser(String userName) throws HiveException{
		  userName = userName.trim().toLowerCase();
		  try{
			  User user = getMSC().get_user(HiveMetaStore.THE_SYSTEM, userName);
			  if(user != null)
				  return true;
			  else return false;
		  }catch(NoSuchObjectException e){
			  return false;
		  }catch(Exception e){
			  throw new HiveException("Failed to judge the user: " + userName + " ! Please check the log.");
		  }
	  }
	  
	  public boolean grantAuth(String byWho, String who, List<String> privileges, String db, String table) throws HiveException{
		  if(db == null || db.equals("*")){
			  //console.printInfo("grant sys privileges!");
			  if(!(table == null || table.equals("*")))
				  throw new HiveException("Can not grant privileges in this format!");
			  if(isAUser(who))
				  return grantAuthSys(byWho, who, privileges);
			  else if(isARole(who))
				  return grantAuthRoleSys(byWho, who, privileges);
			  else throw new HiveException("Can not grant sys privileges to user or role which doesn't exist!");
		  }else if(table == null || table.equals("*")){
			  //console.printInfo("grant db privileges!");
			  return grantAuthOnDb(byWho, who, privileges, db);
		  }else{
			  //console.printInfo("grant table privileges!");
			  return grantAuthOnTbl(byWho, who, privileges, db, table);
		  }
	  }
	  
	  private boolean grantAuthSys(String byWho, String userName, List<String> privileges) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  userName = userName.trim().toLowerCase();
		  if(userName.equals(HiveMetaStore.ROOT_USER)){
			  throw new HiveException("You have no privilege to grant sys privileges for the ROOT user!" );
		  }
		  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = isDBAUser(userName);
		  List<String> privs = new ArrayList<String>();
		  
		  for(String priv: privileges){
			  priv = priv.trim().toUpperCase();
			  if(priv.equals("TOK_DBA_PRI")) forDBA = true;
			  privs.add(priv);
		  }
		  
		  if(forDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().grant_auth_sys(byWho, userName, privs);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to grant sys privileges to the user: " + userName + "! The user does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to grant sys privileges to the user: " + userName + "! Some privilege in: " + privs + " does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to grant sys privileges to the user: " + userName + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the ROOT user has the privilege to grant sys privileges for the DBA user or grant the DBA privilege to the user!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
				return getMSC().grant_auth_sys(byWho, userName, privs);  
			  }catch(NoSuchObjectException e){
				  throw new HiveException("Fail to grant sys privileges to the user: " + userName + "! The user does not exist!");
			  }catch(InvalidObjectException e){
				  throw new HiveException("Fail to grant sys privileges to the user: " + userName + "! Some privilege in: " + privs+ " does not exist!");
			  }catch(Exception e){
				  throw new HiveException("Fail to grant sys privileges to the user: " + userName + "! Please check the log.");
			  }
		  }else{
			  throw new HiveException("Only the DBA users have the privilege to grant sys privileges for the user!" );
		  }
	  }
	  
	  private boolean isDBAUser(String username) throws HiveException{
		  User user = getUser(HiveMetaStore.THE_SYSTEM, username);
		  
		  if(user == null) return false;
		  
		  boolean isDBA = user.isDbaPriv();
		  if(isDBA)
			  return isDBA;
		  else{
			   List<String> playRoles = user.getPlayRoles();
			   for(String r: playRoles){
				  isDBA = isDBA || isDBARole(r);
			  }
			  return isDBA;
		  } 
	  }
	  
	  private boolean isDBARole(String rolename) throws HiveException{
		  Role role = getRole(HiveMetaStore.THE_SYSTEM, rolename);
		  
		  if(role == null) return false;
		 
		  boolean isDBA = role.isDbaPriv();
		  if(isDBA)
			  return isDBA;
		  else{
			   List<String> playRoles = role.getPlayRoles();
			   for(String r: playRoles){
				  isDBA = isDBA || isDBARole(r);
			  }
			  return isDBA;
		  }  
	  }
	  
	  public boolean grantRoleToUser(String byWho, List<String> users, List<String> roles) throws HiveException{
		  boolean success = true;
		  byWho = byWho.trim().toLowerCase();
		  for(String user: users){
			  user = user.trim().toLowerCase();
			  if(isAUser(user))
				  success &= grantRoleToUser(byWho, user, roles);
			  else if(isARole(user))
				  success &= grantRoleToRole(byWho, user, roles);
			  else throw new HiveException("Can not grant role to a user or role not exist!");
		  }
		  return success;
	  }
	  
	  private boolean grantRoleToUser(String byWho, String userName, List<String> roles) throws HiveException{
		  if(userName.equals(HiveMetaStore.ROOT_USER)){
			  throw new HiveException("You have no privilege to grant roles for the ROOT user!" );
		  }
		  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = isDBAUser(userName);
		  boolean grantDBA = false;
		  List<String> rs = new ArrayList<String>();
		  for(String r: roles){
			  r = r.trim().toLowerCase();
			  grantDBA = grantDBA || isDBARole(r);
			  rs.add(r);
		  }  
		  
		  if(forDBA ||grantDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().grant_role_to_user(byWho, userName, rs);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to grant roles to the user: " + userName + "! The user does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to grant roles to the user: " + userName + "! Some role does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to grant roles to the user: " + userName + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the ROOT user has the privilege to grant roles for the DBA user OR grant DBA role to the user!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
				return getMSC().grant_role_to_user(byWho, userName, rs);  
			  }catch(NoSuchObjectException e){
				  throw new HiveException("Fail to grant roles to the user: " + userName + "! The user does not exist!");
			  }catch(InvalidObjectException e){
				  throw new HiveException("Fail to grant roles to the user: " + userName + "! Some role does not exist!");
			  }catch(Exception e){
				  throw new HiveException("Fail to grant roles to the user: " + userName + "! Please check the log.");
			  }
		  }else{
			  throw new HiveException("Only the DBAs have the privilege to grant roles for the user!" );
		  }
	  }
	  
	  public boolean revokeAuth(String byWho, String who, List<String> privileges, String db, String table) throws HiveException{
		  if(db == null || db.equals("*")){
			  if(!(table == null || table.equals("*")))
				  throw new HiveException("Can not revoke privileges in this format!");
			  if(isAUser(who))
				  return revokeAuthSys(byWho, who, privileges);
			  else if(isARole(who))
				  return revokeAuthRoleSys(byWho, who, privileges);
			  else throw new HiveException("Can not revoke sys privileges from user or role which doesn't exist!");
		  }else if(table == null || table.equals("*")){
			  return revokeAuthOnDb(byWho, who, privileges, db);
		  }else{
			  return revokeAuthOnTbl(byWho, who, privileges, db, table);
		  }
	  }
	  
	  private boolean revokeAuthSys(String byWho, String userName, List<String> privileges) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  userName = userName.trim().toLowerCase();
		  if(userName.equals(HiveMetaStore.ROOT_USER)){
			  throw new HiveException("You have no privilege to revoke sys privileges from the ROOT user!" );
		  }
		  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = isDBAUser(userName);
		  List<String> privs = new ArrayList<String>();
		  for(String p: privileges)
			  privs.add(p.trim().toUpperCase());
		  
		  if(forDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().revoke_auth_sys(byWho, userName, privs);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to revoke sys privileges from the user: " + userName + "! The user does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to revoke sys privileges from the user: " + userName + "! Some privilege in: " + privs +" does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to revoke sys privileges from the user: " + userName + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Onlu the ROOT user has the privilege to revoke sys privileges from the DBA user!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
				return getMSC().revoke_auth_sys(byWho, userName, privs);  
			  }catch(NoSuchObjectException e){
				  throw new HiveException("Fail to revoke sys privileges from the user: " + userName + "! The user does not exist!");
			  }catch(InvalidObjectException e){
				  throw new HiveException("Fail to revoke sys privileges from the user: " + userName + "! Some privilege in: " + privs +" does not exist!");
			  }catch(Exception e){
				  throw new HiveException("Fail to revoke sys privileges from the user: " + userName + "! Please check the log.");
			  }
		  }else{
			  throw new HiveException("Only DBAs have the privilege to revoke sys privileges from the user!" );
		  }
	  }
	  
	  public boolean revokeRoleFromUser(String byWho, List<String> users, List<String> roles) throws HiveException{
		  boolean success = true;
		  byWho = byWho.trim().toLowerCase();
		  for(String user: users){
			  user = user.trim().toLowerCase();
			  if(isAUser(user))
				  success &= revokeRoleFromUser(byWho, user, roles);
			  else if(isARole(user))
				  success &= revokeRoleFromRole(byWho, user, roles);
			  else throw new HiveException("Can not revoke role from a user or role not exist!");
		  }
		  return success;
	  }
	  
	  private boolean revokeRoleFromUser(String byWho, String userName, List<String> roles) throws HiveException{
		  if(userName.equals(HiveMetaStore.ROOT_USER)){
			  throw new HiveException("You have no privilege to revoke roles from the ROOT user!" );
		  }
		  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = isDBAUser(userName);
		  List<String> rs = new ArrayList<String>();
		  for(String r: roles){
			  r = r.trim().toLowerCase();
			  rs.add(r);
		  }
			
		  if(forDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().revoke_role_from_user(byWho, userName, rs);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to revoke roles from the user: " + userName + "! The user does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to revoke roles from the user: " + userName + "! Some role does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to revoke roles from the user: " + userName + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the ROOT user has the privilege to revoke roles from the DBA user!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
				  return getMSC().revoke_role_from_user(byWho, userName, rs);   
			  }catch(NoSuchObjectException e){
				  throw new HiveException("Fail to revoke roles from the user: " + userName + "! The user does not exist!");
			  }catch(InvalidObjectException e){
				  throw new HiveException("Fail to revoke roles from the user: " + userName + "! Some role does not exist!");
			  }catch(Exception e){
				  throw new HiveException("Fail to revoke roles from the user: " + userName + "! Please check the log.");
			  }
		  }else{
			  throw new HiveException("Only DBAs have the privilege to revoke roles from the user!" );
		  }
	  }
	  
	  public List<String> showGrants(String byWho, String who) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  who = who.trim().toLowerCase();
		  List<String> grants = new ArrayList<String>();
		  String grant = "";
		  List<String> roles = null;
		  List<String> privs = new ArrayList<String>();
		  int num = 0;
		  
		  if(isAUser(who)){
			  //user table
			  grant += "User name: " + who;
			  grants.add(grant);
			  grant = "Play roles: ";
			  User user = getUser(byWho, who);
			  roles = user.getPlayRoles();
			  if(!roles.isEmpty()){
				  for(String role: roles)
					  grant +=role + " ";
			  }
			  grants.add(grant);
			  grants.add("Privileges:");
			  grant = "GRANT ";
			  if(user.isSelectPriv()){
				  privs.add("SELECT");
				  num++;
			  }
			  if(user.isAlterPriv()){
				  privs.add("ALTER"); 
				  num++;
			  }
			  if(user.isCreatePriv()){
				  privs.add("CREATE"); 
				  num++;
			  }
			  if(user.isUpdatePriv()){
				  privs.add("UPDATE");
				  num++;
			  }
			  if(user.isDeletePriv()){
				  privs.add("DELETE");
				  num++;
			  }
			  if(user.isDropPriv()){
				  privs.add("DROP");
				  num++;
			  }
			  if(user.isInsertPriv()){
				  privs.add("INSERT");
				  num++;
			  }
			  if(user.isIndexPriv()){
				  privs.add("INDEX");
				  num++;
			  }
			  if(user.isCreateviewPriv()){
				  privs.add("CREATE_VIEW"); 
				  num++;
			  }
			  if(user.isShowviewPriv()){
				  privs.add("SHOW_VIEW");
				  num++;
			  }
			  if(user.isDbaPriv()){
				  privs.add("DBA");
				  num++;
			  }
			  if(num>=1){
				if(num>1){
				  for(int i=1;i<num; i++)
					  grant += privs.get(i-1) + ", ";
				}  
				grant += privs.get(num-1) + " ON *.*";
				grants.add(grant);
			  }
			  grant = "";
			}else if(isARole(who)){
			  grant += "Role name: " + who;
			  grants.add(grant);
			  grant = "Play roles: ";
			  Role role = getRole(byWho, who);
			  roles = role.getPlayRoles();
			  if(!roles.isEmpty()){
				  for(String r: roles)
					  grant +=r + " ";
			  }
			  grants.add(grant);
			  grants.add("Privileges:");
			  grant = "GRANT ";
			  if(role.isSelectPriv()){
				  privs.add("SELECT");
				  num++;
			  }
			  if(role.isAlterPriv()){
				  privs.add("ALTER"); 
				  num++;
			  }
			  if(role.isCreatePriv()){
				  privs.add("CREATE"); 
				  num++;
			  }
			  if(role.isUpdatePriv()){
				  privs.add("UPDATE");
				  num++;
			  }
			  if(role.isDeletePriv()){
				  privs.add("DELETE");
				  num++;
			  }
			  if(role.isDropPriv()){
				  privs.add("DROP");
				  num++;
			  }
			  if(role.isInsertPriv()){
				  privs.add("INSERT");
				  num++;
			  }
			  if(role.isIndexPriv()){
				  privs.add("INDEX");
				  num++;
			  }
			  if(role.isCreateviewPriv()){
				  privs.add("CREATE_VIEW"); 
				  num++;
			  }
			  if(role.isShowviewPriv()){
				  privs.add("SHOW_VIEW");
				  num++;
			  }
			  if(role.isDbaPriv()){
				  privs.add("DBA");
				  num++;
			  }
			  if(num>=1){
				if(num>1){
				  for(int i=1;i<num; i++)
					  grant += privs.get(i-1) + ", ";
				}  
				grant += privs.get(num-1) + " ON *.*";
				grants.add(grant);
			  }
			  grant = "";
		  }else{
			  throw new HiveException("The user or role doesn't exist!" ); 
		  }
		  
		  //DbPriv table
		  List<DbPriv> dbps = getAuthOnDbs(byWho, who);
		  if(dbps != null){
			  for(DbPriv dbp: dbps){
				  grant = "GRANT ";
				  privs.clear();
				  num = 0;
				  if(dbp.isSelectPriv()){
					  privs.add("SELECT");
					  num++;
				  }
				  if(dbp.isAlterPriv()){
					  privs.add("ALTER"); 
					  num++;
				  }
				  if(dbp.isCreatePriv()){
					  privs.add("CREATE"); 
					  num++;
				  }
				  if(dbp.isUpdatePriv()){
					  privs.add("UPDATE");
					  num++;
				  }
				  if(dbp.isDeletePriv()){
					  privs.add("DELETE");
					  num++;
				  }
				  if(dbp.isDropPriv()){
					  privs.add("DROP");
					  num++;
				  }
				  if(dbp.isInsertPriv()){
					  privs.add("INSERT");
					  num++;
				  }
				  if(dbp.isIndexPriv()){
					  privs.add("INDEX");
					  num++;
				  }
				  if(dbp.isCreateviewPriv()){
					  privs.add("CREATE_VIEW"); 
					  num++;
				  }
				  if(dbp.isShowviewPriv()){
					  privs.add("SHOW_VIEW");
					  num++;
				  }
				  
				  if(num>1){
					  for(int i=1;i<num; i++)
						  grant += privs.get(i-1) + ", ";
				  }  
				  grant += privs.get(num-1) + " ON " + dbp.getDb() + ".*";
			      grants.add(grant);
				  
				  grant = "";
			  }
		  }
		  //TblPriv table
		  List<TblPriv> tblps = getAuthOnTbls(byWho, who);
		  if(tblps != null){
			 for(TblPriv tblp: tblps){
				  grant = "GRANT ";
				  privs.clear();
				  num = 0;
				  if(tblp.isSelectPriv()){
					  privs.add("SELECT");
					  num++;
				  }
				  if(tblp.isAlterPriv()){
					  privs.add("ALTER"); 
					  num++;
				  }
				  if(tblp.isCreatePriv()){
					  privs.add("CREATE"); 
					  num++;
				  }
				  if(tblp.isUpdatePriv()){
					  privs.add("UPDATE");
					  num++;
				  }
				  if(tblp.isDeletePriv()){
					  privs.add("DELETE");
					  num++;
				  }
				  if(tblp.isDropPriv()){
					  privs.add("DROP");
					  num++;
				  }
				  if(tblp.isInsertPriv()){
					  privs.add("INSERT");
					  num++;
				  }
				  if(tblp.isIndexPriv()){
					  privs.add("INDEX");
					  num++;
				  }
				 if(num>1){
					  for(int i=1;i<num; i++){
						 grant += privs.get(i-1) + ", "; 
					  }
				  }  
				  grant += privs.get(num-1) + " ON " + tblp.getDb() + "." + tblp.getTbl();
			      grants.add(grant);
				  
				  grant = "";
			  }
		  
		  }
		  return grants;
	  }
	  
	  //for role table
	  private boolean isARole(String roleName) throws HiveException{
		  roleName = roleName.trim().toLowerCase();
		  try{
			  return getMSC().is_a_role(roleName);
		  }catch(Exception e){
			  throw new HiveException(" Failed to judge the role: " + roleName + " Error: " + e.toString());
		  }
	  }
	  
	  public boolean createRoles(String byWho, List<String> roles) throws HiveException{
		  boolean success = true;
		  for(String role: roles){
			  success &= createRole(byWho, role);
		  }
		  return success;
	  }
	  
	  private boolean createRole(String byWho, String roleName) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  roleName = roleName.trim().toLowerCase();
		  
		  if(roleName.equals(HiveMetaStore.ROOT_USER)){
			  throw new HiveException("No one can create the ROOT role!");
		  }
		  
		  if(isAUser(roleName)){
			  throw new HiveException("Fail to create the new role! There is a user with the same name!");
		  }
		  
		  boolean byDBA = isDBAUser(byWho);
		  if(byDBA){//create the role
			  if(roleName.length()>16 || roleName.length()<1)
				  throw new HiveException("Length of the new role's name must between 1 and 16!");
			  try{
				  return getMSC().create_role(byWho, roleName);
			  }catch (AlreadyExistsException e){
				  throw new HiveException("The role already exists!");
			  }catch (Exception e){
				  throw new HiveException("Fail to create the new role: " + roleName);
			  }
		  }else {
			  throw new HiveException("Only DBAs have the privilege to create a new role!");
		  }
	  }
	  
	  public boolean dropRoles(String byWho, List<String> roles) throws HiveException{
		  boolean success = true;
		  for(String role: roles){
			  success &= dropRole(byWho, role);
		  }
		  return success;
	  }
	  
	  private boolean dropRole(String byWho, String roleName) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  roleName = roleName.trim().toLowerCase();
		  		  
		  boolean byRoot = byWho.equals(HiveMetaStore.ROOT_USER);
		  boolean byDBA = isDBAUser(byWho);
		  boolean dropDBA = isDBARole(roleName);
		  
		  if(dropDBA){
			  if(byRoot){
				  try{
					  return getMSC().drop_role(byWho, roleName);
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to drop the role: " + roleName + "! The role does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to drop the role: " + roleName + "! ");
				  }
			  }else 
				  throw new HiveException("Only the ROOT user can drop a DBA role!");
		  }
		  
		  if(byDBA){
			  try{
				  return getMSC().drop_role(byWho, roleName);
			  }catch(NoSuchObjectException e){
				  throw new HiveException("Fail to drop the role: " + roleName + "! The role does not exist!");
			  }catch(Exception e){
				  throw new HiveException("Fail to drop the role: " + roleName + "!");
			  }
		  }else
			  throw new HiveException("Only DBAs can drop roles!");
	  }
	  
	  private boolean hasSonRole(String rolename) throws HiveException{
		  Role role = getRole(HiveMetaStore.THE_SYSTEM, rolename);
		  
		  if(role.getPlayRoles().isEmpty()){
			  return false;
		  }else return true;
		   
	  }
	  
	  private boolean grantRoleToRole(String byWho, String roleName, List<String> roles) throws HiveException{
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = isDBARole(roleName);
		  boolean grantDBA = false;
		  List<String> rs = new ArrayList<String>();
		  for(String r: roles){
			  r = r.trim().toLowerCase();
			  if(r.equals(roleName)){
				  throw new HiveException("Can not grant a role to himself!");
			  }
			  if(hasSonRole(r)){
				  throw new HiveException("Can not grant a role, which has son roles, to this role!");
			  }
			  grantDBA = grantDBA || isDBARole(r);
			  rs.add(r);
		  }
		  
		  if(forDBA ||grantDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().grant_role_to_role(byWho, roleName, rs);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to grant roles to the role: " + roleName + "! The role does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to grant roles to the role: " + roleName + "! Some role does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to grant roles to the role: " + roleName + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the ROOT user has the privilege to grant roles for the DBA role OR grant DBA role to the role!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
				return getMSC().grant_role_to_role(byWho, roleName, rs);  
			  }catch(NoSuchObjectException e){
				  throw new HiveException("Fail to grant roles to the role: " + roleName + "! The role does not exist!");
			  }catch(InvalidObjectException e){
				  throw new HiveException("Fail to grant roles to the role: " + roleName + "! Some role does not exist!");
			  }catch(Exception e){
				  throw new HiveException("Fail to grant roles to the role: " + roleName + "! Please check the log.");
			  }
		  }else{
			  throw new HiveException("Only DBAs have the privilege to grant roles for the role!" );
		  }
	  }

	private boolean revokeRoleFromRole(String byWho, String roleName, List<String> roles) throws HiveException{
		  		  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = isDBARole(roleName);
		  boolean revokeDBA = false;
		  List<String> rs = new ArrayList<String>();
		  for(String r: roles){
			  r = r.trim().toLowerCase();
			  revokeDBA = revokeDBA || isDBARole(r);
			  rs.add(r);
		  }
		  
		  if(forDBA || revokeDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().revoke_role_from_role(byWho, roleName, rs);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to revoke roles from the role: " + roleName + "! The role does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to revoke roles from the role: " + roleName + "! Some role does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to revoke roles from the role: " + roleName + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the ROOT user has the privilege to revoke roles from the DBA role or revoke DBA role from the role!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
				  return getMSC().revoke_role_from_role(byWho, roleName, rs);   
			  }catch(NoSuchObjectException e){
				  throw new HiveException("Fail to revoke roles from the role: " + roleName + "! The role does not exist!");
			  }catch(InvalidObjectException e){
				  throw new HiveException("Fail to revoke roles from the role: " + roleName + "! Some role does not exist!");
			  }catch(Exception e){
				  throw new HiveException("Fail to revoke roles from the role: " + roleName + "! Please check the log.");
			  }
		  }else{
			  throw new HiveException("Only DBAs have the privilege to revoke roles from the role!" );
		  }
	  }
	  
	  private Role getRole(String byWho, String roleName) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  roleName = roleName.trim().toLowerCase();
		  try{
			  return getMSC().get_role(byWho, roleName);
		  }catch(NoSuchObjectException e){
			  throw new HiveException("Fail to get the role: " + roleName + "! The role does not exist!");
		  }catch(Exception e){
			  throw new HiveException("Fail to get the role: " + roleName + "!");
		  }
	  }
	  
	  public List<String>  showRoles(String byWho) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  try{
			  return getMSC().get_roles_all(byWho);
		  }catch(Exception e){
			  throw new HiveException("Failed to get all the roles' names! Please check the log.");
		  }
	  }
	  
	  private boolean grantAuthRoleSys(String byWho, String role, List<String> privileges) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  role = role.trim().toLowerCase();
		  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = isDBARole(role);
		  List<String> privs = new ArrayList<String>();
		  
		  for(String priv: privileges){
			  priv = priv.trim().toUpperCase();
			  if(priv.equals("TOK_DBA_PRI")) forDBA = true;
			  privs.add(priv);
		  }
		  
		  if(forDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().grant_auth_role_sys(byWho, role, privs);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to grant sys privileges to the role: " + role + "! The role does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to grant sys privileges to the role: " + role + "! Some privilege in: " + privs +" does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to grant sys privileges to the role: " + role + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the ROOT user has the privilege to grant sys privileges for the DBA role or grant DBA privilege to the user!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
					return getMSC().grant_auth_role_sys(byWho, role, privs);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to grant sys privileges to the role: " + role + "! The role does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to grant sys privileges to the role: " + role + "! Some privilege in: " + privs +" does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to grant sys privileges to the role: " + role + "! Please check the log.");
				  }
		  }else{
			  throw new HiveException("Only DBAs have the privilege to grant sys privileges for the role!" );
		  }
	  }
	  
	  private boolean revokeAuthRoleSys(String byWho, String role, List<String> privileges) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  role = role.trim().toLowerCase();
		  		  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = isDBARole(role);
		  
		  List<String> privs = new ArrayList<String>();
		  for(String priv: privileges){
			  priv = priv.trim().toUpperCase();
			  privs.add(priv);
		  }
		  
		  if(forDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().revoke_auth_role_sys(byWho, role, privs);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to revoke sys privileges from the role: " + role + "! The role does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to revoke sys privileges from the role: " + role + "! Some privilege in: " + privs +" does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to revoke sys privileges from the role: " + role + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the ROOT user has the privilege to revoke sys privileges from the DBA role!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
					return getMSC().revoke_auth_role_sys(byWho, role, privs);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to revoke sys privileges from the role: " + role + "! The role does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to revoke sys privileges from the role: " + role + "! Some privilege in: " + privs +" does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to revoke sys privileges from the role: " + role + "! Please check the log.");
				  }
		  }else{
			  throw new HiveException("Only DBAs have the privilege to revoke sys privileges from the role!" );
		  }
	  }
	  
	  //grant auth on default db
	  private boolean grantAuthOnDb(String byWho, String forWho, List<String> privileges) throws HiveException{
		  return grantAuthOnDb(byWho, forWho, privileges, MetaStoreUtils.DEFAULT_DATABASE_NAME);
	  }
	  
	  //for dbpriv table
	  private boolean grantAuthOnDb(String byWho, String forWho, List<String> privileges, String db) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  forWho = forWho.trim().toLowerCase();
		  db = db.trim().toLowerCase();
		  if(forWho.equals(HiveMetaStore.ROOT_USER)){
			  throw new HiveException("You have no privilege to grant privileges for the ROOT user!" );
		  }
		  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = false;
		  if(isAUser(forWho))
			  forDBA = isDBAUser(forWho);
		  else if(isARole(forWho))
			  forDBA = isDBARole(forWho);
		  else throw new HiveException("Fail to grant privileges on db: " + db + " to the user/role: " + forWho + "! The user/role does not exist!");
		  
		  List<String> privs = new ArrayList<String>();
		  for(String priv: privileges){
			  priv = priv.trim().toUpperCase();
			  privs.add(priv);
		  }
		  
		  if(forDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().grant_auth_on_db(byWho, forWho, privs, db);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to grant privileges on db: " + db + " to the user/role: " + forWho + "! The user/role does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to grant privileges on db: " + db + " to the user/role: " + forWho + "! Some privilege in: " + privs +" does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to grant privileges on db: " + db + " to the user/role: " + forWho + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the ROOT user has the privilege to grant db privileges for the DBA user/role!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
					return getMSC().grant_auth_on_db(byWho, forWho, privs, db);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to grant privileges on db: " + db + " to the user/role: " + forWho + "! The user/role does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to grant privileges on db: " + db + " to the user/role: " + forWho + "! Some privilege in: " + privs +" does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to grant privileges on db: " + db + " to the user/role: " + forWho + "! Please check the log.");
				  }
		  }else{
			  throw new HiveException("Only DBAs have the privilege to grant db privileges for the user/role!" );
		  }
	  }
	  
	  private DbPriv getAuthOnDb(String byWho, String forWho) throws HiveException{
		  return getAuthOnDb(byWho, forWho, MetaStoreUtils.DEFAULT_DATABASE_NAME);
	  }
	  									   
	  private DbPriv getAuthOnDb(String byWho, String forWho, String db) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  forWho = forWho.trim().toLowerCase();
		  db = db.trim().toLowerCase();
		  try{
			  return getMSC().get_auth_on_db(byWho, forWho, db);
		  }catch(NoSuchObjectException e){
			  throw new HiveException("Fail to get the privileges on db: " + db + " for the user/role: " + forWho + "! The user/role does not exist!");
		  }catch(Exception e){
			  throw new HiveException("Fail to get the privileges on db: " + db + " for the user/role: " + forWho + "!" + org.apache.hadoop.util.StringUtils.stringifyException(e));
		  }
	  }
	  
	  private List<DbPriv> getAuthOnDbs(String byWho, String forWho) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  forWho = forWho.trim().toLowerCase();
		  try{
			  return getMSC().get_auth_on_dbs(byWho, forWho);
		  }catch(NoSuchObjectException e){
			  throw new HiveException("Fail to get all the db privileges for the user/role: " + forWho + "! The user/role does not exist!");
		  }catch(Exception e){
			  throw new HiveException("Fail to get all the db privileges for the user/role: " + forWho + "!"  + org.apache.hadoop.util.StringUtils.stringifyException(e) );
		  }
	  }
	  
	  private List<DbPriv> getDbAuth(String byWho) throws HiveException{
		  return getDbAuth(byWho, MetaStoreUtils.DEFAULT_DATABASE_NAME);
	  }
	  private List<DbPriv> getDbAuth(String byWho, String db) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  db = db.trim().toLowerCase();
		  try{
			  return getMSC().get_db_auth(byWho, db);
		  }catch(NoSuchObjectException e){
			  throw new HiveException("Fail to get the privileges on db: " + db + " ! The db doesn't exist!");
		  }catch(Exception e){
			  throw new HiveException("Fail to get the privileges on db: " + db + " !");
		  }
	  }
	  
	  private List<DbPriv> getDbAuthAll(String byWho) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  try{
			  return getMSC().get_db_auth_all(byWho);
		  }catch(Exception e){
			  throw new HiveException("Fail to get all the privileges on dbs!");
		  }
	  }
	  
	  private boolean revokeAuthOnDb(String byWho, String forWho, List<String> privileges) throws HiveException{
		  return revokeAuthOnDb(byWho, forWho, privileges, MetaStoreUtils.DEFAULT_DATABASE_NAME);
	  }
	  
	  private boolean revokeAuthOnDb(String byWho, String forWho, List<String> privileges, String db) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  forWho = forWho.trim().toLowerCase();
		  db = db.trim().toLowerCase();
		  if(forWho.equals(HiveMetaStore.ROOT_USER)){
			  throw new HiveException("You have no privilege to revoke privileges from the ROOT user!" );
		  }
		  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = false;
		  if(isAUser(forWho))
			  forDBA = isDBAUser(forWho);
		  else if(isARole(forWho))
			  forDBA = isDBARole(forWho);
		  else throw new HiveException("Fail to revoke privileges on db: " + db + " from the user/role: " + forWho + "! The user/role does not exist!");
		  
		  List<String> privs = new ArrayList<String>();
		  for(String priv: privileges){
			  priv = priv.trim().toUpperCase();
			  privs.add(priv);
		  }
		  
		  if(forDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().revoke_auth_on_db(byWho, forWho, privs, db);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to revoke privileges on db: " + db + " from the user/role: " + forWho + "! The user/role does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to revoke privileges on db: " + db + " from the user/role: " + forWho + "! Some privilege in: " + privs + " does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to revoke privileges on db: " + db + " from the user/role: " + forWho + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the ROOT user has the privilege to revoke db privileges from the DBA user/role!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
					return getMSC().revoke_auth_on_db(byWho, forWho, privs, db);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to revoke privileges on db: " + db + " from the user/role: " + forWho + "! The user/role does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to revoke privileges on db: " + db + " from the user/role: " + forWho + "! Some privilege in: " + privs + " does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to revoke privileges on db: " + db + " from the user/role: " + forWho + "! Please check the log.");
				  }
		  }else{
			  throw new HiveException("Only DBAs have the privilege to revoke db privileges from the user/role!" );
		  }
	  }
	  
	  private boolean dropAuthOnDb(String byWho, String forWho) throws HiveException{
		  return dropAuthOnDb(byWho, forWho, MetaStoreUtils.DEFAULT_DATABASE_NAME);
	  }
	  
	  private boolean dropAuthOnDb(String byWho, String forWho, String db) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  forWho = forWho.trim().toLowerCase();
		  db = db.trim().toLowerCase();
		  
		  if(forWho.equals(HiveMetaStore.ROOT_USER)){
			  throw new HiveException("You have no privilege to drop privileges for the ROOT user!" );
		  }
		  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = false;
		  if(isAUser(forWho))
			  forDBA = isDBAUser(forWho);
		  else if(isARole(forWho))
			  forDBA = isDBARole(forWho);
		  else throw new HiveException("Fail to drop privileges on db: " + db + " for the user/role: " + forWho + "! The user/role does not exist!");
		  
		  if(forDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().drop_auth_on_db(byWho, forWho, db);  
				  }catch(Exception e){
					  throw new HiveException("Fail to drop privileges on db: " + db + " for the user/role: " + forWho + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the ROOT user has the privilege to drop db privileges for the DBA user/role!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
					return getMSC().drop_auth_on_db(byWho, forWho, db);  
				  }catch(Exception e){
					  throw new HiveException("Fail to drop privileges on db: " + db + " for the user/role: " + forWho + "! Please check the log.");
				  }
		  }else{
			  throw new HiveException("Only DBAs have the privilege to drop db privileges for the user/role!" );
		  }
	  }
	  
	  private boolean dropAuthInDb(String byWho, String forWho) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  forWho = forWho.trim().toLowerCase();
		  
		  if(forWho.equals(HiveMetaStore.ROOT_USER)){
			  throw new HiveException("You have no privilege to drop privileges for the ROOT user!" );
		  }
		  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = false;
		  if(isAUser(forWho))
			  forDBA = isDBAUser(forWho);
		  else if(isARole(forWho))
			  forDBA = isDBARole(forWho);
		  else throw new HiveException("Fail to drop db privileges for the user/role: " + forWho + "! The user/role does not exist!");
		  
		  if(forDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().drop_auth_in_db(byWho, forWho);  
				  }catch(Exception e){
					  throw new HiveException("Fail to drop db privileges for the user/role: " + forWho + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the ROOT user has the privilege to drop db privileges for the DBA user/role!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
					return getMSC().drop_auth_in_db(byWho, forWho);  
				  }catch(Exception e){
					  throw new HiveException("Fail to drop db privileges for the user/role: " + forWho + "! Please check the log.");
				  }
		  }else{
			  throw new HiveException("Only DBAs have the privilege to drop db privileges for the user/role!" );
		  }
	  }
	  
	  //grant auth on  table in default db
	  private boolean grantAuthOnTbl(String byWho, String forWho, List<String> privileges, 
			  String tbl) throws HiveException{
		  return grantAuthOnTbl(byWho, forWho, privileges,  MetaStoreUtils.DEFAULT_DATABASE_NAME,
				  tbl);
	  }
	   
	  //for tblpriv table
	  private boolean grantAuthOnTbl(String byWho, String forWho, List<String> privileges, String db, String tbl) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  forWho = forWho.trim().toLowerCase();
		  db = db.trim().toLowerCase();
		  tbl = tbl.trim().toLowerCase();
		  if(forWho.equals(HiveMetaStore.ROOT_USER)){
			  throw new HiveException("You have no privilege to grant privileges for the ROOT user!" );
		  }
		  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = false;
		  if(isAUser(forWho))
			  forDBA = isDBAUser(forWho);
		  else if(isARole(forWho))
			  forDBA = isDBARole(forWho);
		  else throw new HiveException("Fail to grant privileges on tbl: " + tbl + " in db: " + db + " to the user/role: " + forWho + "! The user/role does not exist!");
		  
		  List<String> privs = new ArrayList<String>();
		  for(String priv: privileges){
			  priv = priv.trim().toUpperCase();
			  privs.add(priv);
		  }
		  
		  if(forDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().grant_auth_on_tbl(byWho, forWho, privs, db, tbl);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to grant privileges on tbl: " + tbl + " in db: " + db + " to the user/role: " + forWho + "! The user/role does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to grant privileges on tbl: " + tbl + " in db: " + db + " to the user/role: " + forWho + "! Some privilege in: " + privs + " does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to grant privileges on tbl: " + tbl + " in db: " + db + " to the user/role: " + forWho + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the ROOT user has the privilege to grant table privileges for the DBA user/role!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
					return getMSC().grant_auth_on_tbl(byWho, forWho, privs, db, tbl);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to grant privileges on tbl: " + tbl + " in db: " + db + " to the user/role: " + forWho + "! The user/role does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to grant privileges on tbl: " + tbl + " in db: " + db + " to the user/role: " + forWho + "! Some privilege in: " + privs + " does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to grant privileges on tbl: " + tbl + " in db: " + db + " to the user/role: " + forWho + "! Please check the log.");
				  }
		  }else{
			  throw new HiveException("Only DBAs have the privilege to grant table privileges for the user/role!" );
		  }
	  }
	  
	  private TblPriv getAuthOnTbl(String byWho, String forWho, String tbl) throws HiveException{
		  return getAuthOnTbl(byWho, forWho, MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
	  }
							   
	  private TblPriv getAuthOnTbl(String byWho, String forWho, String db, String tbl) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  forWho = forWho.trim().toLowerCase();
		  db = db.trim().toLowerCase();
		  tbl = tbl.trim().toLowerCase();
		  try{
			  return getMSC().get_auth_on_tbl(byWho, forWho, db, tbl);
		  }catch(Exception e){
			  throw new HiveException("Fail to get the privileges on tbl: " + tbl + " in db: " + db + " for the user/role: " + forWho + "!");
		  }
	  }
	  
	  private List<TblPriv> getAuthOnTbls(String byWho, String forWho) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  forWho = forWho.trim().toLowerCase();
		  try{
			  return getMSC().get_auth_on_tbls(byWho, forWho);
		  }catch(NoSuchObjectException e){
			  throw new HiveException("Fail to get the privileges on all tbls for the user/role: " + forWho + "! The user/role does not exist!");
		  }catch(Exception e){
			  throw new HiveException("Fail to get the privileges on all tbls for the user/role: " + forWho + "!");
		  }
	  }
	  
	  private List<TblPriv> getTblAuth(String byWho, String tbl) throws HiveException{
		  return getTblAuth(byWho, MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
	  }
	  									   
	  private List<TblPriv> getTblAuth(String byWho, String db, String tbl) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  db = db.trim().toLowerCase();
		  tbl = tbl.trim().toLowerCase();
		  try{
			  return getMSC().get_tbl_auth(byWho, db, tbl);
		  }catch(Exception e){
			  throw new HiveException("Fail to get the privileges on tbl: " + tbl + " in db: " + db + " !");
		  } 
	  }
	  									   
	  private List<TblPriv> getTblAuthAll(String byWho) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  try{
			  return getMSC().get_tbl_auth_all(byWho);
		  }catch(Exception e){
			  throw new HiveException("Fail to get all the privileges on tables!");
		  }
	  }
	  
	  private boolean revokeAuthOnTbl(String byWho, String forWho, List<String> privileges, String tbl) throws HiveException{
		  return revokeAuthOnTbl(byWho, forWho, privileges, MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
	  }
	 
	  private boolean revokeAuthOnTbl(String byWho, String forWho, List<String> privileges, String db, String tbl) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  forWho = forWho.trim().toLowerCase();
		  db = db.trim().toLowerCase();
		  tbl = tbl.trim().toLowerCase();
		  if(forWho.equals(HiveMetaStore.ROOT_USER)){
			  throw new HiveException("You have no privilege to revoke privileges from the ROOT user!" );
		  }
		  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = false;
		  if(isAUser(forWho))
			  forDBA = isDBAUser(forWho);
		  else if(isARole(forWho))
			  forDBA = isDBARole(forWho);
		  else throw new HiveException("Fail to revoke privileges on tbl: " + tbl + " in db: " + db + " from the user/role: " + forWho + "! The user/role does not exist!");
		  
		  List<String> privs = new ArrayList<String>();
		  for(String priv: privileges){
			  priv = priv.trim().toUpperCase();
			  privs.add(priv);
		  }
		  
		  if(forDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().revoke_auth_on_tbl(byWho, forWho, privs, db, tbl);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to revoke privileges on tbl: " + tbl + " in db: " + db + " from the user/role: " + forWho + "! The user/role does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to revoke privileges on tbl: " + tbl + " in db: " + db + " from the user/role: " + forWho + "! Some privilege in: " + privs + " does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to revoke privileges on tbl: " + tbl + " in db: " + db + " from the user/role: " + forWho + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the ROOT user has the privilege to revoke table privileges from the DBA user/role!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
					return getMSC().revoke_auth_on_tbl(byWho, forWho, privs, db, tbl);  
				  }catch(NoSuchObjectException e){
					  throw new HiveException("Fail to revoke privileges on tbl: " + tbl + " in db: " + db + " from the user/role: " + forWho + "! The user/role does not exist!");
				  }catch(InvalidObjectException e){
					  throw new HiveException("Fail to revoke privileges on tbl: " + tbl + " in db: " + db + " from the user/role: " + forWho + "! Some privilege in: " + privs + " does not exist!");
				  }catch(Exception e){
					  throw new HiveException("Fail to revoke privileges on tbl: " + tbl + " in db: " + db + " from the user/role: " + forWho + "! Please check the log.");
				  }
		  }else{
			  throw new HiveException("Only DBAs have the privilege to revoke table privileges from the user/role!" );
		  }
	  }
	  
	  private boolean dropAuthOnTbl(String byWho, String forWho, String tbl) throws HiveException{
		  return dropAuthOnTbl(byWho, forWho, MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
	  }
	  
	  private boolean dropAuthOnTbl(String byWho, String forWho, String db, String tbl) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  forWho = forWho.trim().toLowerCase();
		  db = db.trim().toLowerCase();
		  tbl = tbl.trim().toLowerCase();
		  
		  if(forWho.equals(HiveMetaStore.ROOT_USER)){
			  throw new HiveException("You have no privilege to drop privileges for the ROOT user!" );
		  }
		  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = false;
		  if(isAUser(forWho))
			  forDBA = isDBAUser(forWho);
		  else if(isARole(forWho))
			  forDBA = isDBARole(forWho);
		  else throw new HiveException("Fail to drop privileges on tbl: " + tbl + " in db: "+ db + " for the user/role: " + forWho + "! The user/role does not exist!");
		  
		  if(forDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().drop_auth_on_tbl(byWho, forWho, db, tbl);  
				  }catch(Exception e){
					  throw new HiveException("Fail to drop privileges on tbl: " + tbl + " in db: "+ db + " for the user/role: " + forWho + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the ROOT user has the privilege to drop table privileges for the DBA user/role!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
					return getMSC().drop_auth_on_tbl(byWho, forWho, db, tbl);  
				  }catch(Exception e){
					  throw new HiveException("Fail to drop privileges on tbl: " + tbl + " in db: "+ db + " for the user/role: " + forWho + "! Please check the log.");
				  }
		  }else{
			  throw new HiveException("Only DBAs have the privilege to drop table privileges for the user/role!" );
		  }
	  }
	  
	  private boolean dropAuthInTbl(String byWho, String forWho) throws HiveException{
		  byWho = byWho.trim().toLowerCase();
		  forWho = forWho.trim().toLowerCase();
		  
		  if(forWho.equals(HiveMetaStore.ROOT_USER)){
			  throw new HiveException("You have no privilege to drop privileges for the ROOT user!" );
		  }
		  
		  boolean byDBA = isDBAUser(byWho);
		  boolean forDBA = false;
		  if(isAUser(forWho))
			  forDBA = isDBAUser(forWho);
		  else if(isARole(forWho))
			  forDBA = isDBARole(forWho);
		  else throw new HiveException("Fail to drop table privileges for the user/role: " + forWho + "! The user/role does not exist!");
		  
		  if(forDBA){
			  if(byWho.equals(HiveMetaStore.ROOT_USER)){
				  try{
					return getMSC().drop_auth_in_tbl(byWho, forWho);  
				  }catch(Exception e){
					  throw new HiveException("Fail to drop table privileges for the user/role: " + forWho + "! Please check the log.");
				  }
			  }else{
				  throw new HiveException("Only the ROOT user has the privilege to drop table privileges for the DBA user/role!" );
			  }
		  }
		  
		  if(byDBA){
			  try{
					return getMSC().drop_auth_in_tbl(byWho, forWho);  
				  }catch(Exception e){
					  throw new HiveException("Fail to drop table privileges for the user/role: " + forWho + "! Please check the log.");
				  }
		  }else{
			  throw new HiveException("Only DBAs have the privilege to drop table privileges for the user/role!" );
		  }
	  }
	  
	  public boolean hasAuth(String who, Privilege priv, String db, String table)throws HiveException{
		  if(db == null){
			  //LOG.info("test sys priv!");
			  return hasAuth(who, priv);
		  }
		  else if(table == null){
			  //LOG.info("test db priv!");
			  return hasAuthOnDb(who, priv, db);
		  }else{
			  //LOG.info("test tbl priv!");
			  return hasAuthOnTbl(who, priv, db, table);
		  }
	  }
	  
	  private boolean hasAuth(String who, Privilege priv) throws HiveException{
		  who = who.trim().toLowerCase();
		  //if root
		  if(who.equals(HiveMetaStore.ROOT_USER)) return true;
		  
		  //if DBA
		  User user =  getUser(HiveMetaStore.THE_SYSTEM, who);
		  /*LOG.info(who + " has " + " select: " + user.isSelectPriv() + " alter: " + user.isAlterPriv() + 
				  " insert: " + user.isInsertPriv() + " index: " + user.isIndexPriv() + 
				  " create: " + user.isCreatePriv() + " drop: " + user.isDropPriv() + 
				  " delete: " + user.isDeletePriv()+ " update: " + user.isUpdatePriv() +
				  " createview: " + user.isCreateviewPriv() + " showview: " + user.isShowviewPriv() +
				  " dba: " + user.isDbaPriv());*/
		  if(user == null) throw new HiveException("The user doesn't exist!");
		  if(user.isDbaPriv()) return true;
		  
		  
		  //test sys priv
		  switch(priv){
		  	case SELECT_PRIV:       if(user.isSelectPriv())     return true; break;
		  	case ALTER_PRIV:        if(user.isAlterPriv())      return true; break;
		  	case INSERT_PRIV:       if(user.isInsertPriv())     return true; break;
		  	case INDEX_PRIV:        if(user.isIndexPriv())      return true; break;
		  	case CREATE_PRIV:       if(user.isCreatePriv())     return true; break;
		  	case DROP_PRIV:         if(user.isDropPriv())       return true; break;
		  	case DELETE_PRIV:       if(user.isDeletePriv())     return true; break;
		  	case UPDATE_PRIV:       if(user.isUpdatePriv())     return true; break;
		  	case CREATE_VIEW_PRIV:  if(user.isCreateviewPriv())  return true; break;
		  	case SHOW_VIEW_PRIV:    if(user.isShowviewPriv())    return true; break;
		  	case DBA_PRIV:          if(user.isDbaPriv())        return true; break;
		  }
		  
		  //test role sys priv
		  List<String> roles = user.getPlayRoles();
		  if(!roles.isEmpty()){
			  List<Role>   rs1  = new ArrayList<Role>();
			  Role r1;
			  for(String s1: roles){
				  r1 = getRole(HiveMetaStore.THE_SYSTEM, s1);
				  switch(priv){
				  	case SELECT_PRIV:       if(r1.isSelectPriv())     return true; break;
				  	case ALTER_PRIV:        if(r1.isAlterPriv())      return true; break;
				  	case INSERT_PRIV:       if(r1.isInsertPriv())     return true; break;
				  	case INDEX_PRIV:        if(r1.isIndexPriv())      return true; break;
				  	case CREATE_PRIV:       if(r1.isCreatePriv())     return true; break;
				  	case DROP_PRIV:         if(r1.isDropPriv())       return true; break;
				  	case DELETE_PRIV:       if(r1.isDeletePriv())     return true; break;
				  	case UPDATE_PRIV:       if(r1.isUpdatePriv())     return true; break;
				  	case CREATE_VIEW_PRIV:  if(r1.isCreateviewPriv()) return true; break;
				  	case SHOW_VIEW_PRIV:    if(r1.isShowviewPriv())   return true; break;
				  	case DBA_PRIV:          if(r1.isDbaPriv())        return true; break;
				  }
				  rs1.add(r1);
			  }
		  
			  for(Role r2: rs1){
				  roles = r2.getPlayRoles();
				  Role r3;
				  for(String s2: roles){
					  r3 = getRole(HiveMetaStore.THE_SYSTEM, s2);
					  switch(priv){
					  	case SELECT_PRIV:       if(r3.isSelectPriv())     return true; break;
					  	case ALTER_PRIV:        if(r3.isAlterPriv())      return true; break;
					  	case INSERT_PRIV:       if(r3.isInsertPriv())     return true; break;
					  	case INDEX_PRIV:        if(r3.isIndexPriv())      return true; break;
					  	case CREATE_PRIV:       if(r3.isCreatePriv())     return true; break;
					  	case DROP_PRIV:         if(r3.isDropPriv())       return true; break;
					  	case DELETE_PRIV:       if(r3.isDeletePriv())     return true; break;
					  	case UPDATE_PRIV:       if(r3.isUpdatePriv())     return true; break;
					  	case CREATE_VIEW_PRIV:  if(r3.isCreateviewPriv()) return true; break;
					  	case SHOW_VIEW_PRIV:    if(r3.isShowviewPriv())   return true; break;
					  	case DBA_PRIV:          if(r3.isDbaPriv())        return true; break;
					  }
				  }
			  }
		  }
		  
		  return false;
	  }
	  
	  private boolean hasAuthOnDb(String who, Privilege priv, String db) throws HiveException{
		  if(hasAuth(who, priv))
			  return true;
		  
		  who = who.trim().toLowerCase();
		  db = db.trim().toLowerCase();
		  
		  //test db priv
		  DbPriv dbp;
		  try{
			  dbp = getAuthOnDb(HiveMetaStore.THE_SYSTEM, who, db);
		  }catch (Throwable e) { 
				//LOG.info(StringUtils.stringifyException(e));
				throw new RuntimeException(e) ;
		  }
		  
		  
		  if(dbp!=null){
			  /*LOG.info(who + " has " + " select: " + dbp.isSelectPriv() + " alter: " + dbp.isAlterPriv() + 
					  " insert: " + dbp.isInsertPriv() + " index: " + dbp.isIndexPriv() + 
					  " create: " + dbp.isCreatePriv() + " drop: " + dbp.isDropPriv() + 
					  " delete: " + dbp.isDeletePriv()+ " update: " + dbp.isUpdatePriv() +
					  " createview: " + dbp.isCreateviewPriv() + " showview: " + dbp.isShowviewPriv());*/
			  switch(priv){
			  	case SELECT_PRIV:       if(dbp.isSelectPriv())     return true; break;
			  	case ALTER_PRIV:        if(dbp.isAlterPriv())      return true; break;
			  	case INSERT_PRIV:       if(dbp.isInsertPriv())     return true; break;
			  	case INDEX_PRIV:        if(dbp.isIndexPriv())      return true; break;
			  	case CREATE_PRIV:       if(dbp.isCreatePriv())     return true; break;
			  	case DROP_PRIV:         if(dbp.isDropPriv())       return true; break;
			  	case DELETE_PRIV:       if(dbp.isDeletePriv())     return true; break;
			  	case UPDATE_PRIV:       if(dbp.isUpdatePriv())     return true; break;
			  	case CREATE_VIEW_PRIV:  if(dbp.isCreateviewPriv()) return true; break;
			  	case SHOW_VIEW_PRIV:    if(dbp.isShowviewPriv())   return true; break;
			  }
		  }
		  
		  //test role db priv
		  User user =  getUser(HiveMetaStore.THE_SYSTEM, who);
		  List<String> roles = user.getPlayRoles();
		  if(!roles.isEmpty()){
			  //LOG.info(who + "'s roles are not null! The first role: " + roles.get(0));
			  List<Role>   rs1  = new ArrayList<Role>();
			  Role r1;
			  for(String s1: roles){
				  if(dbp != null){
					  switch(priv){
					  	case SELECT_PRIV:       if(dbp.isSelectPriv())     return true; break;
					  	case ALTER_PRIV:        if(dbp.isAlterPriv())      return true; break;
					  	case INSERT_PRIV:       if(dbp.isInsertPriv())     return true; break;
					  	case INDEX_PRIV:        if(dbp.isIndexPriv())      return true; break;
					  	case CREATE_PRIV:       if(dbp.isCreatePriv())     return true; break;
					  	case DROP_PRIV:         if(dbp.isDropPriv())       return true; break;
					  	case DELETE_PRIV:       if(dbp.isDeletePriv())     return true; break;
					  	case UPDATE_PRIV:       if(dbp.isUpdatePriv())     return true; break;
					  	case CREATE_VIEW_PRIV:  if(dbp.isCreateviewPriv()) return true; break;
					  	case SHOW_VIEW_PRIV:    if(dbp.isShowviewPriv())   return true; break;
					  }
				  }
				  
				  r1 = getRole(HiveMetaStore.THE_SYSTEM, s1);
				  rs1.add(r1);
			  }
		  
			  for(Role r2: rs1){
				  roles = r2.getPlayRoles();
				  Role r3;
				  for(String s2: roles){
					  dbp = getAuthOnDb(HiveMetaStore.THE_SYSTEM, s2, db);
					  if(dbp != null){
						  switch(priv){
						  	case SELECT_PRIV:       if(dbp.isSelectPriv())     return true; break;
					  		case ALTER_PRIV:        if(dbp.isAlterPriv())      return true; break;
					  		case INSERT_PRIV:       if(dbp.isInsertPriv())     return true; break;
					  		case INDEX_PRIV:        if(dbp.isIndexPriv())      return true; break;
					  		case CREATE_PRIV:       if(dbp.isCreatePriv())     return true; break;
					  		case DROP_PRIV:         if(dbp.isDropPriv())       return true; break;
					  		case DELETE_PRIV:       if(dbp.isDeletePriv())     return true; break;
					  		case UPDATE_PRIV:       if(dbp.isUpdatePriv())     return true; break;
					  		case CREATE_VIEW_PRIV:  if(dbp.isCreateviewPriv()) return true; break;
					  		case SHOW_VIEW_PRIV:    if(dbp.isShowviewPriv())   return true; break;
						  }
					  }
					  
				  }
			  }
		  }
		  
		  return false;
	  }
	  
	  private boolean hasAuthOnTbl(String who, Privilege priv, String db, String table) throws HiveException{
		  if(hasAuthOnDb(who, priv, db))
			  return true;
		  
		  who = who.trim().toLowerCase();
		  db = db.trim().toLowerCase();
		  table = table.trim().toLowerCase();
		  
		  //test tbl priv
		  TblPriv tblp = getAuthOnTbl(HiveMetaStore.THE_SYSTEM, who, db, table);
		  if(tblp!=null){
			  switch(priv){
			  	case SELECT_PRIV:       if(tblp.isSelectPriv())     return true; break;
			  	case ALTER_PRIV:        if(tblp.isAlterPriv())      return true; break;
			  	case INSERT_PRIV:       if(tblp.isInsertPriv())     return true; break;
			  	case INDEX_PRIV:        if(tblp.isIndexPriv())      return true; break;
			  	case CREATE_PRIV:       if(tblp.isCreatePriv())     return true; break;
			  	case DROP_PRIV:         if(tblp.isDropPriv())       return true; break;
			  	case DELETE_PRIV:       if(tblp.isDeletePriv())     return true; break;
			  	case UPDATE_PRIV:       if(tblp.isUpdatePriv())     return true; break;
			  	}
		  }
		  
		  //test role tbl priv
		  User user =  getUser(HiveMetaStore.THE_SYSTEM, who);
		  if(user == null) throw new HiveException("The user doesn't exist!");
		  List<String> roles = user.getPlayRoles();
		  if(!roles.isEmpty()){
			  List<Role>   rs1  = new ArrayList<Role>();
			  Role r1;
			  for(String s1: roles){
				  tblp = getAuthOnTbl(HiveMetaStore.THE_SYSTEM, s1, db, table);
				  if(tblp != null){
					  switch(priv){
					  	case SELECT_PRIV:       if(tblp.isSelectPriv())     return true; break;
				  		case ALTER_PRIV:        if(tblp.isAlterPriv())      return true; break;
				  		case INSERT_PRIV:       if(tblp.isInsertPriv())     return true; break;
				  		case INDEX_PRIV:        if(tblp.isIndexPriv())      return true; break;
				  		case CREATE_PRIV:       if(tblp.isCreatePriv())     return true; break;
				  		case DROP_PRIV:         if(tblp.isDropPriv())       return true; break;
				  		case DELETE_PRIV:       if(tblp.isDeletePriv())     return true; break;
				  		case UPDATE_PRIV:       if(tblp.isUpdatePriv())     return true; break;
					  }
				  }
				  
				  r1 = getRole(HiveMetaStore.THE_SYSTEM, s1);
				  rs1.add(r1);
			  }
		  
			  for(Role r2: rs1){
				  roles = r2.getPlayRoles();
				  Role r3;
				  for(String s2: roles){
					  tblp = getAuthOnTbl(HiveMetaStore.THE_SYSTEM, s2, db, table);
					  if(tblp != null){
						  switch(priv){
						  	case SELECT_PRIV:       if(tblp.isSelectPriv())     return true; break;
						  	case ALTER_PRIV:        if(tblp.isAlterPriv())      return true; break;
						  	case INSERT_PRIV:       if(tblp.isInsertPriv())     return true; break;
						  	case INDEX_PRIV:        if(tblp.isIndexPriv())      return true; break;
						  	case CREATE_PRIV:       if(tblp.isCreatePriv())     return true; break;
						  	case DROP_PRIV:         if(tblp.isDropPriv())       return true; break;
						  	case DELETE_PRIV:       if(tblp.isDeletePriv())     return true; break;
						  	case UPDATE_PRIV:       if(tblp.isUpdatePriv())     return true; break;
						  }
					  }
					  
				  }
			  }
		  }
		  
		  return false;
	  }
	  
	 				   
	  // added by BrantZhang for authorization end
	  public List<String> getDatabases() throws HiveException{
		  try{
		  return getMSC().getDatabases();
		  }catch (Exception e){
			  throw new HiveException("get databases meta error : " + e.getMessage());
		  }
	  }
	  
	  // added by konten for index begin
      public boolean createIndex(Table table)throws HiveException
      {
          //db = db.trim().toLowerCase();
          //table = table.trim().toLowerCase();
          //name = name.trim().toLowerCase();
          
          try
          {
              IndexItem index = new IndexItem();
              index.setDb(table.getDbName());
              index.setTbl(table.getName());
              
              int indexNum = 0;
              String indexNumString = table.getParameters().get("indexNum");
              if(indexNumString != null && indexNumString.length() != 0)
              {
                  indexNum = Integer.valueOf(indexNumString);
              }   
              
             /*
              * create table中最多只有1个index, indexInfo格式固定:name;fieldList;type;
              * add index 也会调用此处,因此此处创建indexNum最大的那个索引.
              */
              if(indexNum != 0) 
              {                  
                  String indexInfoString = table.getParameters().get("index"+(indexNum-1));
                  indexInfoString = (indexInfoString == null ? "": indexInfoString);
                  
                  //System.out.println("indexInfoString:"+indexInfoString);
                  String[] indexInfoStrings = indexInfoString.split(";");
                  
                  if(indexInfoStrings != null && indexInfoStrings.length != 0)
                  {
                      index.setName(indexInfoStrings[0]);
                      index.setFieldList(indexInfoStrings[1]);                    
                      index.setType(Integer.valueOf(indexInfoStrings[2]));
                  }  
                  
                  /*
                  Set<String> priPartNames = null;
                  if (table.getTTable().getPriPartition() != null)
                  {
                      priPartNames = table.getTTable().getPriPartition().getParSpaces().keySet();
                      index.setPartPath(priPartNames);  
                      
                      Iterator<String> iter = priPartNames.iterator();
                      while(iter.hasNext())
                      {
                          System.out.println(":"+iter.next());
                      }
                  }
                  else 
                      System.out.println("part  null");
                  */
                  return getMSC().create_index(index);
              }   
          }          
          catch (Exception e)
          {
              e.printStackTrace();
              throw new HiveException("Fail to create the index: " + e.getMessage());
          }
          
          return true;
          //判断db或tbl是否存在? 判断index是否存在? 在哪里????             
      }   
      
      public boolean dropIndex(String db, String table, String name)throws HiveException
      {
          db = db.trim().toLowerCase();
          table = table.trim().toLowerCase();
          name = name.trim().toLowerCase();
          
          try
          {
              return getMSC().drop_index(db, table, name);
          }          
          catch (Exception e)
          {
              e.printStackTrace();
              throw new HiveException("Fail to drop the index: " + e.getMessage());
          }
      }
      
      public int getIndexNum(String db, String table)throws HiveException
      {
          db = db.trim().toLowerCase();
          table = table.trim().toLowerCase();
          
          try
          {
              return getMSC().get_index_num(db, table);
          }
          catch (Exception e)
          {
              throw new HiveException("Fail to get index num: " + e.getMessage());
          }
      }
      
      public int getIndexType(String db, String table, String name)throws HiveException
      {
          db = db.trim().toLowerCase();
          table = table.trim().toLowerCase();
          name = name.trim().toLowerCase();
          
          try
          {
              return getMSC().get_index_type(db, table, name);
          }
          catch (Exception e)
          {
              throw new HiveException("Fail to get index type: " + e.getMessage());
          }
      }
      
      public String getIndexField(String db, String table, String name)throws HiveException
      {
          db = db.trim().toLowerCase();
          table = table.trim().toLowerCase();
          name = name.trim().toLowerCase();
          
          try
          {
              return getMSC().get_index_field(db, table, name);
          }
          catch (Exception e)
          {
              throw new HiveException("Fail to get index field: " + e.getMessage());
          }
      }
      
      public String getIndexLocation(String db, String table, String name)throws HiveException
      {
          db = db.trim().toLowerCase();
          table = table.trim().toLowerCase();
          name = name.trim().toLowerCase();
          
          try
          {
              return getMSC().get_index_location(db, table, name);
          }
          catch (Exception e)
          {
              throw new HiveException("Fail to get index location: " + e.getMessage());
          }
      }
      
      public boolean setIndexLocation(String db, String table, String name, String location)throws HiveException
      {
          db = db.trim().toLowerCase();
          table = table.trim().toLowerCase();
          name = name.trim().toLowerCase();
          location = location.trim().toLowerCase();
          
          try
          {
              return getMSC().set_index_location(db, table, name, location);
          }
          catch (Exception e)
          {
              throw new HiveException("Fail to set index location: " + e.getMessage());
          }
      }
      
      public boolean setIndexStatus(String db, String table, String name, int status)throws HiveException
      {
          db = db.trim().toLowerCase();
          table = table.trim().toLowerCase();
          name = name.trim().toLowerCase();
                    
          try
          {
              return getMSC().set_index_status(db, table, name, status);
          }
          catch (Exception e)
          {
              throw new HiveException("Fail to set index status: " + e.getMessage());
          }
      }
      
      public List<IndexItem>  getAllIndexTable(String db, String table)throws HiveException
      {
          db = db.trim().toLowerCase();
          table = table.trim().toLowerCase();
          
          try
          {
              return getMSC().get_all_index_table(db, table);
          }
          catch (Exception e)
          {
              e.printStackTrace();
              throw new HiveException("Fail to get all index in table: " + e.getMessage());
          }          
      }
      
      public IndexItem getIndexInfo(String db, String table, String name)throws HiveException
      {
          db = db.trim().toLowerCase();
          table = table.trim().toLowerCase();
          name = name.trim().toLowerCase();
          
          try
          {
              return getMSC().get_index_info(db, table, name);
          }
          catch (Exception e)
          {
              throw new HiveException("Fail to get index info: " + e.getMessage());
          }      
      }
      
      public List<IndexItem> getAllIndexSys()throws HiveException
      {
          try
          {
              return getMSC().get_all_index_sys();
          }
          catch (Exception e)
          {
              throw new HiveException("Fail to get all index info in sys: " + e.getMessage());
          } 
      }
	  // added by konten for index end
};
