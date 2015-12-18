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

package org.apache.hadoop.hive.ql.exec;

import java.io.BufferedWriter;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.IndexItem;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.CheckResult;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveMetaStoreChecker;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.MsckDesc;
import org.apache.hadoop.hive.ql.plan.addDefaultPartitionDesc;
import org.apache.hadoop.hive.ql.plan.alterTableDesc;
import org.apache.hadoop.hive.ql.plan.createDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.createTableDesc;
import org.apache.hadoop.hive.ql.plan.createTableLikeDesc;
import org.apache.hadoop.hive.ql.plan.descFunctionDesc;
import org.apache.hadoop.hive.ql.plan.descTableDesc;
import org.apache.hadoop.hive.ql.plan.dropDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.dropTableDesc;
import org.apache.hadoop.hive.ql.plan.showDBDesc;
import org.apache.hadoop.hive.ql.plan.showFunctionsDesc;
import org.apache.hadoop.hive.ql.plan.showIndexDesc;
import org.apache.hadoop.hive.ql.plan.showPartitionsDesc;
import org.apache.hadoop.hive.ql.plan.showTablesDesc;
import org.apache.hadoop.hive.ql.plan.truncatePartitionDesc;
import org.apache.hadoop.hive.ql.plan.truncateTableDesc;
import org.apache.hadoop.hive.ql.plan.useDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.showIndexDesc.showIndexTypes;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.protobuf.ProtobufSerDe;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;

/**
 * DDLTask implementation
 * 
 **/
public class DDLTask extends Task<DDLWork> implements Serializable {
	private static final long serialVersionUID = 1L;
	static final private Log LOG = LogFactory.getLog("hive.ql.exec.DDLTask");

	transient HiveConf conf;
	static final private int separator  = Utilities.tabCode;
	static final private int terminator = Utilities.newLineCode;

	public void initialize(HiveConf conf) {
		super.initialize(conf);
		this.conf = conf;
	}

	public int execute() {

		// Create the db
		Hive db;
		try {
			db = Hive.get(conf);

			createTableDesc crtTbl = work.getCreateTblDesc();
			if (crtTbl != null) {
				return createTable(db, crtTbl);
			}
			
			truncatePartitionDesc tpd = work.getTruncatePartDesc();
			if(tpd != null){
				return truncatePartition(db,tpd);
			}

			createTableLikeDesc crtTblLike = work.getCreateTblLikeDesc();
			if (crtTblLike != null) {
				return createTableLike(db, crtTblLike);
			}

			dropTableDesc dropTbl = work.getDropTblDesc();
			if (dropTbl != null) {
				return dropTable(db, dropTbl);
			}

			truncateTableDesc truncTbl = work.getTruncTblDesc();
			if (truncTbl != null) {
				return truncateTable(db, truncTbl);
			}

			alterTableDesc alterTbl = work.getAlterTblDesc();
			if (alterTbl != null) {
				return alterTable(db, alterTbl);
			}

			AddPartitionDesc addPartitionDesc = work.getAddPartitionDesc();
			if (addPartitionDesc != null) {
				return addPartition(db, addPartitionDesc);
			}      

			MsckDesc msckDesc = work.getMsckDesc();
			if (msckDesc != null) {
				return msck(db, msckDesc);
			}      

			descTableDesc descTbl = work.getDescTblDesc();
			if (descTbl != null) {
				return describeTable(db, descTbl);
			}

			descFunctionDesc descFunc = work.getDescFunctionDesc();
			if (descFunc != null) {
				return describeFunction(descFunc);
			}

			showTablesDesc showTbls = work.getShowTblsDesc();
			if (showTbls != null) {
				return showTables(db, showTbls);
			}

			showFunctionsDesc showFuncs = work.getShowFuncsDesc();
			if (showFuncs != null) {
				return showFunctions(showFuncs);
			}

			showPartitionsDesc showParts = work.getShowPartsDesc();
			if (showParts != null) {
				return showPartitions(db, showParts);
			}
			
			addDefaultPartitionDesc addDefaultPart = work.getAddDefaultPartDesc();
			if(addDefaultPart != null){
				return addDefaultPartition(db,addDefaultPart);
			}
			
			createDatabaseDesc createDb = work.getCreateDbDesc();
			if(createDb != null){
				return createDatabase(db,createDb);
			}
			
			dropDatabaseDesc dropDb= work.getDropDbDesc();
			if(dropDb != null){
				return dropDatabase(db,dropDb);
			}
			
			useDatabaseDesc useDb = work.getUseDbDesc();
			if(useDb != null){
				return useDatabase(db,useDb);
			}
			
			showDBDesc showDb = work.getShowDbDesc();
			
			if(showDb != null){
				return showDatabases(db,showDb);
			}
			showIndexDesc showIndex = work.getShowIndexDesc();
            if(showIndex != null){
                return showIndex(db,showIndex);
			}

		} catch (InvalidTableException e) {
			console.printError("Table " + e.getTableName() + " does not exist");
			LOG.debug(StringUtils.stringifyException(e));
			return 1;
		} catch (HiveException e) {
			console.printError("FAILED: Error in metadata: " + e.getMessage(), "\n" + StringUtils.stringifyException(e));
			LOG.debug(StringUtils.stringifyException(e));
			return 1;
		} catch (Exception e) {
			console.printError("Failed with exception " +   e.getMessage(), "\n" + StringUtils.stringifyException(e));
			return (1);
		}
		assert false;
		return 0;
	}

	/**
	 * Add a partition to a table.
	 * @param db Database to add the partition to.
	 * @param addPartitionDesc Add this partition.
	 * @return Returns 0 when execution succeeds and above 0 if it fails.
	 * @throws HiveException 
	 */
	private int addPartition(Hive db, AddPartitionDesc addPartitionDesc) 
	throws HiveException,MetaException,TException,InvalidOperationException{

		try{
			if(!db.hasAuth(SessionState.get().getUserName(), Hive.Privilege.ALTER_PRIV, addPartitionDesc.getDbName(), addPartitionDesc.getTableName())){
				console.printError("user : " + SessionState.get().getUserName() + " do not have alter privilege on table : " +  addPartitionDesc.getDbName() + "." +  addPartitionDesc.getTableName());
			}
		Table tbl = db.getTable(addPartitionDesc.getDbName(), 
				addPartitionDesc.getTableName());

		if(!tbl.isPartitioned())
			throw new HiveException("DB: " + addPartitionDesc.getDbName() + " table: " + addPartitionDesc.getTableName() + "is not partitioned!");

		if(addPartitionDesc.getIsSubPartition() && !tbl.getHasSubPartition()){
			throw new HiveException("DB: " + addPartitionDesc.getDbName() + " table: " + addPartitionDesc.getTableName() + "do not have sub partition");
		}

		db.addPartitions(tbl,addPartitionDesc);

		/*

    if(addPartitionDesc.getLocation() == null) {
      db.createPartition(tbl, addPartitionDesc.getPartSpec());
    } else {
      //set partition path relative to table
      db.createPartition(tbl, addPartitionDesc.getPartSpec(), 
          new Path(tbl.getPath(), addPartitionDesc.getLocation()));
    }
		 */
		
		}catch(Exception e){
			console.printError("add partition error: " + e.getMessage());
			return 1;
		}
		return 0;
	}
	
	private int addDefaultPartition(Hive db,addDefaultPartitionDesc adpd)
	throws HiveException,MetaException,TException,InvalidOperationException{
		try{
		if(!db.hasAuth(SessionState.get().getUserName(), Hive.Privilege.ALTER_PRIV, adpd.getDbName(), adpd.getTblName())){
			console.printError("user : " + SessionState.get().getUserName() + " do not have privilege on table : " +  adpd.getDbName() + "." +  adpd.getTblName());
		}
		
		Table tbl = db.getTable(adpd.getDbName(), 
				adpd.getTblName());
		if(adpd.isSub()){
			if(tbl.getTTable().getSubPartition() == null){
				throw new HiveException("table : " + adpd.getDbName() + "." + adpd.getTblName() + " do not has subpartitions!" );
			}
			if(tbl.getTTable().getSubPartition().getParSpaces().containsKey("default")){
				throw new HiveException("table : " + adpd.getDbName() + "." + adpd.getTblName() + " already has a default subpartition!" );
			}
			
			db.addDefalutSubPartition(tbl);
			console.printInfo("add default sub partition OK!");
		}
		else{
			if(tbl.getTTable().getPriPartition() == null){
				throw new HiveException("table : " + adpd.getDbName() + "." + adpd.getTblName() + " do not has Pri partitions!" );
			}
			if(tbl.getTTable().getPriPartition().getParSpaces().containsKey("default")){
				throw new HiveException("table : " + adpd.getDbName() + "." + adpd.getTblName() + " already has a default pri partition!" );
			}
			
			db.addDefalutPriPartition(tbl);
			console.printInfo("add default pri partition OK!");
			
		}
		}
		catch(Exception e){
			console.printError("add default partition error : " + e.getMessage());
			return 1;
		}
		
		return 0;
	}

	private int truncatePartition(Hive db,truncatePartitionDesc tpd)
	throws HiveException,MetaException,TException,InvalidOperationException{
		try{
		if(!db.hasAuth(SessionState.get().getUserName(), Hive.Privilege.DELETE_PRIV, tpd.getDbName(), tpd.getTblName())){
			console.printError("user : " + SessionState.get().getUserName() + " do not have delete (or trancate) privilege on table : " +  tpd.getDbName() + "." +  tpd.getTblName());
		}
		Table tbl = db.getTable(tpd.getDbName(), 
				tpd.getTblName());
		if(!tbl.isPartitioned()){
			console.printError("table: " +  tpd.getDbName() + "." + tpd.getTblName() + " is not partitioned!");
			return 1;
		}
		if(tpd.getPriPartName() != null && !tbl.getTTable().getPriPartition().getParSpaces().containsKey(tpd.getPriPartName().toLowerCase())){
			console.printError("table: " +  tpd.getDbName() + "." + tpd.getTblName() + " do not contain pri partition : " + tpd.getPriPartName());
			return 2;
		}
		if(tpd.getSubPartName() != null && !tbl.getTTable().getSubPartition().getParSpaces().containsKey(tpd.getSubPartName().toLowerCase())){
			console.printError("table: " +  tpd.getDbName() + "." + tpd.getTblName() + " do not contain sub partition : " + tpd.getSubPartName());
			return 3;
		}
		
		db.truncatePartition(tbl,tpd.getPriPartName(),tpd.getSubPartName());
		}catch(Exception e){
			console.printError("truncate partition error: " + e.getMessage());
			return 1;
			
		}
		
		return 0;
		
		
	}

	/**
	 * MetastoreCheck, see if the data in the metastore matches
	 * what is on the dfs.
	 * Current version checks for tables and partitions that
	 * are either missing on disk on in the metastore.
	 * 
	 * @param db The database in question.
	 * @param msckDesc Information about the tables and partitions
	 * we want to check for.
	 * @return Returns 0 when execution succeeds and above 0 if it fails.
	 */
	private int msck(Hive db, MsckDesc msckDesc) {

		CheckResult result = new CheckResult();
		try {
			HiveMetaStoreChecker checker = new HiveMetaStoreChecker(db);
			checker.checkMetastore(
					SessionState.get().getDbName(), msckDesc.getTableName(), 
					msckDesc.getTrf(),
					result);
		} catch (HiveException e) {
			LOG.warn("Failed to run metacheck: ", e);
			return 1;
		} catch (IOException e) {
			LOG.warn("Failed to run metacheck: ", e);
			return 1;
		} finally {

			BufferedWriter resultOut = null;
			try {
				FileSystem fs = msckDesc.getResFile().getFileSystem(conf);
				resultOut = new BufferedWriter(
						new OutputStreamWriter(fs.create(msckDesc.getResFile())));

				boolean firstWritten = false;
				firstWritten |= writeMsckResult(result.getTablesNotInMs(), 
						"Tables not in metastore:", resultOut, firstWritten);
				firstWritten |= writeMsckResult(result.getTablesNotOnFs(), 
						"Tables missing on filesystem:", resultOut, firstWritten);      
				firstWritten |= writeMsckResult(result.getPartitionsNotInMs(), 
						"Partitions not in metastore:", resultOut, firstWritten);
				firstWritten |= writeMsckResult(result.getPartitionsNotOnFs(), 
						"Partitions missing from filesystem:", resultOut, firstWritten);      
			} catch (IOException e) {
				LOG.warn("Failed to save metacheck output: ", e);
				return 1;
			} finally {
				if(resultOut != null) {
					try {
						resultOut.close();
					} catch (IOException e) {
						LOG.warn("Failed to close output file: ", e);
						return 1;
					}
				}
			}
		}

		return 0;
	}

	/**
	 * Write the result of msck to a writer.
	 * @param result The result we're going to write
	 * @param msg Message to write.
	 * @param out Writer to write to
	 * @param wrote if any previous call wrote data
	 * @return true if something was written
	 * @throws IOException In case the writing fails
	 */
	private boolean writeMsckResult(List<? extends Object> result, String msg, 
			Writer out, boolean wrote) throws IOException {

		if(!result.isEmpty()) { 
			if(wrote) {
				out.write(terminator);
			}

			out.write(msg);
			for (Object entry : result) {
				out.write(separator);
				out.write(entry.toString());
			}
			return true;
		}

		return false;
	}

	/**
	 * Write a list of partitions to a file.
	 * 
	 * @param db The database in question.
	 * @param showParts These are the partitions we're interested in.
	 * @return Returns 0 when execution succeeds and above 0 if it fails.
	 * @throws HiveException Throws this exception if an unexpected error occurs.
	 */
	private int showPartitions(Hive db,
			showPartitionsDesc showParts) throws HiveException {
		// get the partitions for the table and populate the output
		String tabName = showParts.getTabName();
		Table tbl = null;
		ArrayList<ArrayList<String>> parts = null;

		tbl = db.getTable(SessionState.get().getDbName(), tabName);

		if (!tbl.isPartitioned()) {
			console.printError("Table " + tabName + " is not a partitioned table");
			return 1;
		}

		parts = db.getPartitionNames(tbl.getDbName(), tbl
				, Short.MAX_VALUE);

		// write the results in the file
		try {
			FileSystem fs = showParts.getResFile().getFileSystem(conf);
			DataOutput outStream = (DataOutput) fs.create(showParts.getResFile());
			
			Iterator<String> iterParts = parts.get(0).iterator();

			outStream.writeBytes("pri partitions:");
			outStream.write(terminator);
			while (iterParts.hasNext()) {
				// create a row per partition name
				outStream.writeBytes(iterParts.next());
				outStream.write(terminator);
			}
			
			if(parts.size() == 2 && parts.get(1) != null){
				iterParts = parts.get(1).iterator();
				outStream.writeBytes("sub partitions:");
				outStream.write(terminator);

				while (iterParts.hasNext()) {
					// create a row per partition name
					outStream.writeBytes(iterParts.next());
					outStream.write(terminator);
				}
			}		
			((FSDataOutputStream)outStream).close();
		} catch (FileNotFoundException e) {
			LOG.info("show partitions: " + StringUtils.stringifyException(e));
			throw new HiveException(e.toString());
		} catch (IOException e) {
			LOG.info("show partitions: " + StringUtils.stringifyException(e));
			throw new HiveException(e.toString());
		} catch (Exception e) {
			throw new HiveException(e.toString());
		}

		return 0;
	}

	/**
	 * Write a list of the tables in the database to a file.
	 * 
	 * @param db The database in question.
	 * @param showTbls These are the tables we're interested in.
	 * @return Returns 0 when execution succeeds and above 0 if it fails.
	 * @throws HiveException Throws this exception if an unexpected error occurs.
	 */
	private int showTables(Hive db, showTablesDesc showTbls)
	throws HiveException {
		// get the tables for the desired pattenn - populate the output stream
		List<String> tbls = null;
		if (showTbls.getPattern() != null) {
			LOG.info("pattern: " + showTbls.getPattern());
			tbls = db.getTablesByPattern(showTbls.getPattern());
			LOG.info("results : " + tbls.size());
		} else
			tbls = db.getAllTables();

		// write the results in the file
		try {
			FileSystem fs = showTbls.getResFile().getFileSystem(conf);
			DataOutput outStream = (DataOutput)fs.create(showTbls.getResFile());
			SortedSet<String> sortedTbls = new TreeSet<String>(tbls);
			Iterator<String> iterTbls = sortedTbls.iterator();

			while (iterTbls.hasNext()) {
				// create a row per table name
				outStream.writeBytes(iterTbls.next());
				outStream.write(terminator);
			}
			((FSDataOutputStream)outStream).close();
		} catch (FileNotFoundException e) {
			LOG.warn("show table: " + StringUtils.stringifyException(e));
			return 1;
		} catch (IOException e) {
			LOG.warn("show table: " + StringUtils.stringifyException(e));
			return 1;
		} catch (Exception e) {
			throw new HiveException(e.toString());
		}
		return 0;
	}

	/**
	 * Write a list of the user defined functions to a file.
	 * 
	 * @param showFuncs are the functions we're interested in.
	 * @return Returns 0 when execution succeeds and above 0 if it fails.
	 * @throws HiveException Throws this exception if an unexpected error occurs.
	 */
	private int showFunctions(showFunctionsDesc showFuncs)
	throws HiveException {
		// get the tables for the desired pattenn - populate the output stream
		Set<String> funcs = null;
		if (showFuncs.getPattern() != null) {
			LOG.info("pattern: " + showFuncs.getPattern());
			funcs = FunctionRegistry.getFunctionNames(showFuncs.getPattern());
			LOG.info("results : " + funcs.size());
		} else
			funcs = FunctionRegistry.getFunctionNames();

		// write the results in the file
		try {
			FileSystem fs = showFuncs.getResFile().getFileSystem(conf);
			DataOutput outStream = (DataOutput)fs.create(showFuncs.getResFile());
			SortedSet<String> sortedFuncs = new TreeSet<String>(funcs);
			Iterator<String> iterFuncs = sortedFuncs.iterator();

			while (iterFuncs.hasNext()) {
				// create a row per table name
				outStream.writeBytes(iterFuncs.next());
				outStream.write(terminator);
			}
			((FSDataOutputStream)outStream).close();
		} catch (FileNotFoundException e) {
			LOG.warn("show function: " + StringUtils.stringifyException(e));
			return 1;
		} catch (IOException e) {
			LOG.warn("show function: " + StringUtils.stringifyException(e));
			return 1;
		} catch (Exception e) {
			throw new HiveException(e.toString());
		}
		return 0;
	}

	/**
	 * Shows a description of a function.
	 * 
	 * @param descFunc is the function we are describing
	 * @throws HiveException
	 */
	private int describeFunction(descFunctionDesc descFunc)
	throws HiveException {
		String name = descFunc.getName();

		// write the results in the file
		try {
			FileSystem fs = descFunc.getResFile().getFileSystem(conf);
			DataOutput outStream = (DataOutput)fs.create(descFunc.getResFile());

			// get the function documentation
			description desc = null;
			FunctionInfo fi = FunctionRegistry.getFunctionInfo(name);
			if(fi.getUDFClass() != null) {
				desc = fi.getUDFClass().getAnnotation(description.class);
			} else if(fi.getGenericUDFClass() != null) {
				desc = fi.getGenericUDFClass().getAnnotation(description.class);
			}

			if (desc != null) {
				outStream.writeBytes(desc.value().replace("_FUNC_", name));
				if(descFunc.isExtended() && desc.extended().length()>0) {
					outStream.writeBytes("\n"+desc.extended().replace("_FUNC_", name));
				}
			} else {
				outStream.writeBytes("Function " + name + " does not exist or cannot" +
				" find documentation for it.");
			}

			outStream.write(terminator);

			((FSDataOutputStream)outStream).close();
		} catch (FileNotFoundException e) {
			LOG.warn("describe function: " + StringUtils.stringifyException(e));
			return 1;
		} catch (IOException e) {
			LOG.warn("describe function: " + StringUtils.stringifyException(e));
			return 1;
		} catch (Exception e) {
			throw new HiveException(e.toString());
		}
		return 0;
	}  

	/**
	 * Write the description of a table to a file.
	 * 
	 * @param db The database in question.
	 * @param descTbl This is the table we're interested in.
	 * @return Returns 0 when execution succeeds and above 0 if it fails.
	 * @throws HiveException Throws this exception if an unexpected error occurs.
	 */
	private int describeTable(Hive db, descTableDesc descTbl)
	throws HiveException {
		String colPath = descTbl.getTableName();
		String tableName = colPath.substring(0,
				colPath.indexOf('.') == -1 ? colPath.length() : colPath.indexOf('.'));

		// describe the table - populate the output stream
		Table tbl = db.getTable(SessionState.get().getDbName(), tableName, false);
		Partition part = null;
		try {
			if (tbl == null) {
				FileSystem fs = descTbl.getResFile().getFileSystem(conf);
				DataOutput outStream = (DataOutput) fs.open(descTbl.getResFile());
				String errMsg = "Table " + tableName + " does not exist";
				outStream.write(errMsg.getBytes("UTF-8"));
				((FSDataOutputStream) outStream).close();
				return 0;
			}
			if (descTbl.getPartName() != null) {//[TODO]now we do not support get partition info
				part = null;//db.getPartition(tbl, descTbl.getPartName(), false);
				if (part == null) {
					FileSystem fs = descTbl.getResFile().getFileSystem(conf);
					DataOutput outStream = (DataOutput) fs.open(descTbl.getResFile());
					String errMsg = "Partition " + descTbl.getPartName() + " for table "
					+ tableName + " does not exist";
					outStream.write(errMsg.getBytes("UTF-8"));
					((FSDataOutputStream) outStream).close();
					return 0;
				}
				tbl = part.getTable();
			}
		} catch (FileNotFoundException e) {
			LOG.info("describe table: " + StringUtils.stringifyException(e));
			return 1;
		} catch (IOException e) {
			LOG.info("describe table: " + StringUtils.stringifyException(e));
			return 1;
		}

		try {

			LOG.info("DDLTask: got data for " + tbl.getName());

			List<FieldSchema> cols = null;
			if (colPath.equals(tableName)) {
				cols = tbl.getCols();

				//partition column is also in table columns
				/*if (part != null) {
          cols = part.getTPartition().getSd().getCols();*/

			} else {
				cols = Hive.getFieldsFromDeserializer(colPath, tbl.getDeserializer());
			}
			FileSystem fs = descTbl.getResFile().getFileSystem(conf);
			DataOutput outStream = (DataOutput)fs.create(descTbl.getResFile());
			
			Iterator<FieldSchema> iterCols = cols.iterator();
			while (iterCols.hasNext()) {
				// create a row per column
				FieldSchema col = iterCols.next();
				outStream.writeBytes(col.getName());
				outStream.write(separator);
				outStream.writeBytes(col.getType());
				outStream.write(separator);
				outStream.writeBytes(col.getComment() == null ? "" : col.getComment());
				outStream.write(terminator);
			}

			//partition column is also in table columns
			/*

      if (tableName.equals(colPath)) {
        // also return the partitioning columns
        List<FieldSchema> partCols = tbl.getPartCols();
        Iterator<FieldSchema> iterPartCols = partCols.iterator();
        while (iterPartCols.hasNext()) {
          FieldSchema col = iterPartCols.next();
          outStream.writeBytes(col.getName());
          outStream.write(separator);
          outStream.writeBytes(col.getType());
          outStream.write(separator);
          outStream.writeBytes(col.getComment() == null ? "" : col.getComment());
          outStream.write(terminator);
        }
      }
			 */			
			// if extended desc table then show the complete details of the table
			if (descTbl.isExt()) {
				// add empty line
				outStream.write(terminator);
				outStream.writeBytes("Detailed Table Information");
				/*
				if (tbl.getTTable().getPriPartition() != null) {
					// show partition information
					outStream.writeBytes("Detailed Pri Partition Information");
					outStream.write(terminator);
					outStream.writeBytes(tbl.getTTable().getPriPartition().toString());
					outStream.write(terminator);
					if(tbl.getTTable().getSubPartition() != null){
						outStream.writeBytes("Detailed Sub Partition Information");
						outStream.write(terminator);
						outStream.writeBytes(tbl.getTTable().getSubPartition().toString());
						outStream.write(terminator);
					}
					
					outStream.writeBytes("inputFormat:" + tbl.getInputFormatClass().getName());
					outStream.write(terminator);
					outStream.writeBytes("outputFormat:" + tbl.getOutputFormatClass().getName());
					outStream.write(terminator);
					outStream.writeBytes("serializationLib:" + tbl.getSerializationLib());
					outStream.write(terminator);
					outStream.writeBytes("deSerializationLib:" + tbl.getDeserializer().getClass().getName());
					outStream.write(terminator);
					
					// comment column is empty					
				} else {
				*/
					// show table information
					
					outStream.write(separator);
					outStream.writeBytes(tbl.getTTable().toString());
					outStream.write(separator);
					// comment column is empty
					outStream.write(terminator);
				//}
			}

		LOG.info("DDLTask: written data for " + tbl.getName());
		((FSDataOutputStream) outStream).close();

	} catch (FileNotFoundException e) {
		LOG.info("describe table: " + StringUtils.stringifyException(e));
		return 1;
	} catch (IOException e) {
		LOG.info("describe table: " + StringUtils.stringifyException(e));
		return 1;
	} catch (Exception e) {
		throw new HiveException(e.toString());
	}

	return 0;
}

/**
 * Alter a given table.
 * 
 * @param db The database in question.
 * @param alterTbl This is the table we're altering.
 * @return Returns 0 when execution succeeds and above 0 if it fails.
 * @throws HiveException Throws this exception if an unexpected error occurs.
 */
private int alterTable(Hive db, alterTableDesc alterTbl) throws HiveException {
	
	if(!db.hasAuth(SessionState.get().getUserName(), Hive.Privilege.ALTER_PRIV, SessionState.get().getDbName(), alterTbl.getOldName())){
		console.printError("user : " + SessionState.get().getUserName() + " do not have privilege on table : " +  SessionState.get().getDbName()+ "." +  alterTbl.getOldName());
		return 1;
	}
	// alter the table
	Table tbl = db.getTable(/*MetaStoreUtils.DEFAULT_DATABASE_NAME*/SessionState.get().getDbName(), alterTbl.getOldName());
	if (alterTbl.getOp() == alterTableDesc.alterTableTypes.RENAME){
		tbl.getTTable().setTableName(alterTbl.getNewName());
		if(tbl.isPartitioned()){
			tbl.getTTable().getPriPartition().setTableName(alterTbl.getNewName());
			if(tbl.getTTable().getSubPartition() != null){
				tbl.getTTable().getSubPartition().setTableName(alterTbl.getNewName());
			}
		}
	}
	else if (alterTbl.getOp() == alterTableDesc.alterTableTypes.ADDCOLS) {
		List<FieldSchema> newCols = alterTbl.getNewCols();
		List<FieldSchema> oldCols = tbl.getCols();
		if (tbl.getSerializationLib().equals("org.apache.hadoop.hive.serde.thrift.columnsetSerDe")) {
			console
			.printInfo("Replacing columns for columnsetSerDe and changing to LazySimpleSerDe");
			tbl.setSerializationLib(LazySimpleSerDe.class.getName());
			tbl.getTTable().getSd().setCols(newCols);
		} else {
			// make sure the columns does not already exist
			Iterator<FieldSchema> iterNewCols = newCols.iterator();
			while (iterNewCols.hasNext()) {
				FieldSchema newCol = iterNewCols.next();
				String newColName = newCol.getName();
				Iterator<FieldSchema> iterOldCols = oldCols.iterator();
				while (iterOldCols.hasNext()) {
					String oldColName = iterOldCols.next().getName();
					if (oldColName.equalsIgnoreCase(newColName)) {
						console.printError("Column '" + newColName + "' exists");
						return 1;
					}
				}
				oldCols.add(newCol);
			}
			tbl.getTTable().getSd().setCols(oldCols);
		}
	} else if (alterTbl.getOp() == alterTableDesc.alterTableTypes.REPLACECOLS) {//TODO:rename a column
		// change SerDe to LazySimpleSerDe if it is columnsetSerDe
		if (tbl.getSerializationLib().equals("org.apache.hadoop.hive.serde.thrift.columnsetSerDe")) {
			console
			.printInfo("Replacing columns for columnsetSerDe and changing to LazySimpleSerDe");
			tbl.setSerializationLib(LazySimpleSerDe.class.getName());
		} else if (!tbl.getSerializationLib().equals(
				MetadataTypedColumnsetSerDe.class.getName())
				&& !tbl.getSerializationLib().equals(
						LazySimpleSerDe.class.getName())
						&& !tbl.getSerializationLib().equals(
								ColumnarSerDe.class.getName())
								&& !tbl.getSerializationLib().equals(
										DynamicSerDe.class.getName())) {
			console
			.printError("Replace columns is not supported for this table. SerDe may be incompatible.");
			return 1;
		}
		if(tbl.isPartitioned()){
			console
			.printError("Replace columns is not supported for this table. this table is partitioned.");
			return 1;
			
		}
		tbl.getTTable().getSd().setCols(alterTbl.getNewCols());
	} else if (alterTbl.getOp() == alterTableDesc.alterTableTypes.ADDPROPS) {
		tbl.getTTable().getParameters().putAll(alterTbl.getProps());
	} else if (alterTbl.getOp() == alterTableDesc.alterTableTypes.ADDSERDEPROPS) {
		tbl.getTTable().getSd().getSerdeInfo().getParameters().putAll(
				alterTbl.getProps());
	} else if (alterTbl.getOp() == alterTableDesc.alterTableTypes.ADDSERDE) {
		tbl.setSerializationLib(alterTbl.getSerdeName());
		if ((alterTbl.getProps() != null) && (alterTbl.getProps().size() > 0))
			tbl.getTTable().getSd().getSerdeInfo().getParameters().putAll(
					alterTbl.getProps());
		// since serde is modified then do the appropriate things to reset columns
		// etc
		tbl.reinitSerDe();
		tbl.setFields(Hive.getFieldsFromDeserializer(tbl.getName(), tbl
				.getDeserializer()));
	}
	else if(alterTbl.getOp() == alterTableDesc.alterTableTypes.ADDINDEX)
	{	    
	    // 判断是否外表、是否结构化表
	    String extenal = tbl.getProperty("EXTERNAL");
	    if(extenal != null && extenal.equalsIgnoreCase("TRUE"))
	    {
	        console.printError("add index not allow for extenal table");
            return 1;
	    }
	    
	    String type = tbl.getProperty("type");
	    if(type == null || (!type.equalsIgnoreCase("column") && !type.equalsIgnoreCase("format")) )
        {
            console.printError("add index allow for format/column storage only");
            return 1;
        }
	    
        // check index info, name is cheched at parse, check field only        
        // 是否重复字段
        Iterator<String> iterFieldCol = alterTbl.getIndexInfo().fieldList.iterator();
        List<String> tmpFieldNames = new ArrayList<String>();
        while (iterFieldCol.hasNext())
        {
            String colName = iterFieldCol.next();
            Iterator<String> iter = tmpFieldNames.iterator();
            while (iter.hasNext())
            {
                String oldColName = iter.next();
                if (colName.equalsIgnoreCase(oldColName))
                {    
                    console.printError("index field duplicate");
                    return 1; 
                }
            }
            tmpFieldNames.add(colName);
        }
        
        // 字段是否存在
        Iterator<String> iter = alterTbl.getIndexInfo().fieldList.iterator();
        while (iter.hasNext())
        {
            boolean exist = false;
            String fieldColName = iter.next();
            Iterator<FieldSchema> tblColIter = tbl.getCols().iterator();
            while (tblColIter.hasNext())
            {
                String colName = tblColIter.next().getName();
                if (colName.equalsIgnoreCase(fieldColName))
                {
                    exist = true;
                    break;
                }
            }
            if(!exist)
            {
                console.printError("index field not exist");
                return 1;                 
            }
        }      
	    int indexNum = 0;
	    String indexNumString = tbl.getParameters().get("indexNum"); 
	    if(indexNumString != null && indexNumString.length() != 0)
	    {
	        indexNum = Integer.valueOf(indexNumString);
	    }
	    String indexName = alterTbl.getIndexInfo().name;
	    if(indexName == null || indexName.length() == 0)
	    {
	        indexName = alterTbl.getIndexInfo().fieldList.get(0) + indexNum;
	    }	    
	    
	    // 判断名字是否重复
	    for(int i = 0; i < indexNum; i++)
	    {
	        String indexInfoString = tbl.getParameters().get("index"+i);
	        if(indexInfoString != null)
	        {
	            String[] indexInfo = indexInfoString.split(";");
	            if(indexInfo != null && indexName.equalsIgnoreCase(indexInfo[0]))
	            {
	                console.printError("index already exist");
	                return 1;
	            }
	        }
	    }
	    
	    // 修改param中的信息.
	    
	    String fieldListString = "";
        for(int i = 0; i < alterTbl.getIndexInfo().fieldList.size(); i++)
        {
            if(i == 0)
            {
                fieldListString = getFieldIndxByName(alterTbl.getIndexInfo().fieldList.get(i), tbl.getCols());
            }
            else
            {
                fieldListString += "," + getFieldIndxByName(alterTbl.getIndexInfo().fieldList.get(i), tbl.getCols());
            }
        }       
        String indexInfoString = indexName + ";" + fieldListString + ";"+ alterTbl.getIndexInfo().indexType;
        tbl.setProperty("index"+indexNum, indexInfoString);
        indexNum++;
        tbl.setProperty("indexNum", ""+indexNum);        
	}
	else if(alterTbl.getOp() == alterTableDesc.alterTableTypes.DROPINDEX)
	{
	    /* 此处仅删除tbl中元数据,要根据名字删除!!
	     * 另外,由于index0,index1的命名, 删除中间一个后,后续的需要重新命名.
	     */
	    int indexNum = 0;
        String indexNumString = tbl.getParameters().get("indexNum"); 
        if(indexNumString != null && indexNumString.length() != 0)
        {
            indexNum = Integer.valueOf(indexNumString);
        }      
        int pos = -1;
        for(int i = 0; i < indexNum; i++)
        {
            String indexInfoString = tbl.getParameters().get("index"+i);
            if(indexInfoString != null)
            {
                String[] indexInfo = indexInfoString.split(";");
                if(indexInfo != null && alterTbl.getIndexInfo().name.equalsIgnoreCase(indexInfo[0]))
                {   
                    pos = i;
                    break;
                }
            }
        }        
        for(int i = pos; i < indexNum - 1; i++)
        {
            String indexInfoString = tbl.getParameters().get("index"+(i+1));
            if(indexInfoString == null)
            {
                indexInfoString = "";
            }
            tbl.setProperty("index"+i, indexInfoString);
        }    
        if(indexNum > 0)
        {
            tbl.getParameters().remove("index"+(indexNum - 1));        
            tbl.setProperty("indexNum", ""+(indexNum - 1));
        }       
	}
	else {
		console.printError("Unsupported Alter commnad");
		return 1;
	}

	// set last modified by properties
	try {
		tbl.setProperty("last_modified_by", conf.getUser());
	} catch (IOException e) {
		console.printError("Unable to get current user: " + e.getMessage(), StringUtils.stringifyException(e));
		return 1;
	}
	tbl.setProperty("last_modified_time", Long.toString(System
			.currentTimeMillis() / 1000));

	try {
		tbl.checkValidity();
	} catch (HiveException e) {
		console.printError("Invalid table columns : " + e.getMessage(), StringUtils.stringifyException(e));
		return 1;
	}

	try {
		db.alterTable(alterTbl.getOldName(), tbl);
		
		// 只能在此处判断是否add index
		if(alterTbl.getOp() == alterTableDesc.alterTableTypes.ADDINDEX)
		{
		    db.createIndex(tbl);
		}
		else if(alterTbl.getOp() == alterTableDesc.alterTableTypes.DROPINDEX)
		{
		    db.dropIndex(tbl.getDbName(), tbl.getName(), alterTbl.getIndexInfo().name);
		}
		else
		{            
        }
	} catch (InvalidOperationException e) {
		console.printError("Invalid alter operation: " + e.getMessage());
		LOG.info("alter table: " + StringUtils.stringifyException(e));
		return 1;
	} catch (HiveException e) {
	    e.printStackTrace();
		return 1;
	}
	return 0;
}

/**
 * Drop a given table.
 * 
 * @param db The database in question.
 * @param dropTbl This is the table we're dropping.
 * @return Returns 0 when execution succeeds and above 0 if it fails.
 * @throws HiveException Throws this exception if an unexpected error occurs.
 */
private int dropTable(Hive db, dropTableDesc dropTbl) throws HiveException {
	if(!db.hasAuth(SessionState.get().getUserName(), Hive.Privilege.DROP_PRIV, SessionState.get().getDbName(), null)){
		console.printError("user : " + SessionState.get().getUserName() + " do not have privilege on DB : " +  SessionState.get().getDbName());
		return 1;
	}
	if (dropTbl.getIsDropPartition() == null || dropTbl.getIsDropPartition() == false) {
		// drop the table
		db.dropTable(SessionState.get().getDbName(), dropTbl.getTableName());
	} else {
		// drop partitions in the list
		Table tbl = db.getTable(SessionState.get().getDbName(), dropTbl.getTableName());
		Boolean isSub = dropTbl.getIsSub();

		org.apache.hadoop.hive.metastore.api.Partition LevelParts = null;
		if(isSub)
			LevelParts = tbl.getTTable().getSubPartition();
		else
			LevelParts = tbl.getTTable().getPriPartition();

		ArrayList<String> parts = new ArrayList<String>();
		for(String str: dropTbl.getPartitionNames()){
			if(LevelParts.getParSpaces().containsKey(str))
				parts.add(str);
			else
				console.printInfo("Partition " + str + " does not exist.");
		}

		dropTbl.setPartitionNames(parts);
		db.dropPartition(dropTbl,true);
	}
	return 0;
}


/**
 * Truncate a given table.
 * 
 * @param db The database in question.
 * @param truncTbl This is the table we're truncating.
 * @return Returns 0 when execution succeeds and above 0 if it fails.
 * @throws HiveException Throws this exception if an unexpected error occurs.
 */
private int truncateTable(Hive db, truncateTableDesc truncTbl) throws HiveException, MetaException {
	if(!db.hasAuth(SessionState.get().getUserName(), Hive.Privilege.DELETE_PRIV, SessionState.get().getDbName(), null)){
		console.printError("user : " + SessionState.get().getUserName() + " do not have privilege on DB : " +  SessionState.get().getDbName());
		return 1;
	}
	
	db.truncateTable(SessionState.get().getDbName(), truncTbl.getTableName());
	return 0;
}


/**
 * Check if the given serde is valid
 */
private void validateSerDe(String serdeName) throws HiveException {
	try {
		Deserializer d = SerDeUtils.lookupDeserializer(serdeName);
		if(d != null) {
			System.out.println("Found class for " + serdeName);
		}
	} catch (SerDeException e) {
		throw new HiveException ("Cannot validate serde: " + serdeName, e);
	}
}



/**
 * Create a new table.
 * 
 * @param db The database in question.
 * @param crtTbl This is the table we're creating.
 * @return Returns 0 when execution succeeds and above 0 if it fails.
 * @throws HiveException Throws this exception if an unexpected error occurs.
 */
private int createTable(Hive db, createTableDesc crtTbl) throws HiveException {
	if(!db.hasAuth(SessionState.get().getUserName(), Hive.Privilege.CREATE_PRIV, SessionState.get().getDbName(),null)){
		console.printError("user : " + SessionState.get().getUserName() + " do not have privilege on DB : " +  SessionState.get().getDbName());
		return 1;
	}
	// create the table
	Table tbl = new Table(crtTbl.getTableName());
	StorageDescriptor tblStorDesc = tbl.getTTable().getSd();
	if (crtTbl.getBucketCols() != null)
		tblStorDesc.setBucketCols(crtTbl.getBucketCols());
	if (crtTbl.getSortCols() != null)
		tbl.setSortCols(crtTbl.getSortCols());
	if (crtTbl.getPartdesc() != null)
		tbl.setPartitions(crtTbl.getPartdesc());
	if (crtTbl.getNumBuckets() != -1)
		tblStorDesc.setNumBuckets(crtTbl.getNumBuckets());

	if (crtTbl.getSerName() != null) {
		tbl.setSerializationLib(crtTbl.getSerName());
		if (crtTbl.getMapProp() != null) {
			Iterator<Entry<String, String>> iter = crtTbl.getMapProp()
			.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, String> m = (Entry<String, String>) iter.next();
				tbl.setSerdeParam(m.getKey(), m.getValue());
			}
		}
	} else {
		if (crtTbl.getFieldDelim() != null) {
			tbl.setSerdeParam(Constants.FIELD_DELIM, crtTbl.getFieldDelim());
			tbl.setSerdeParam(Constants.SERIALIZATION_FORMAT, crtTbl
					.getFieldDelim());
		}
		if (crtTbl.getFieldEscape() != null) {
			tbl.setSerdeParam(Constants.ESCAPE_CHAR, crtTbl.getFieldEscape());
		}

		if (crtTbl.getCollItemDelim() != null)
			tbl.setSerdeParam(Constants.COLLECTION_DELIM, 
					crtTbl.getCollItemDelim());
		if (crtTbl.getMapKeyDelim() != null)
			tbl.setSerdeParam(Constants.MAPKEY_DELIM, crtTbl.getMapKeyDelim());
		if (crtTbl.getLineDelim() != null)
			tbl.setSerdeParam(Constants.LINE_DELIM, crtTbl.getLineDelim());
	}

	/**
	 * We use LazySimpleSerDe by default.
	 * 
	 * If the user didn't specify a SerDe, and any of the columns are not simple types, 
	 * we will have to use DynamicSerDe instead.
	 */
	if (crtTbl.getSerName() == null) {
		LOG.info("Default to LazySimpleSerDe for table " + crtTbl.getTableName() );
		tbl.setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
	} else {
		// let's validate that the serde exists
		validateSerDe(crtTbl.getSerName());
	}

	if (crtTbl.getComment() != null)
		tbl.setProperty("comment", crtTbl.getComment());
	if (crtTbl.getLocation() != null)
		tblStorDesc.setLocation(crtTbl.getLocation());

	tbl.setInputFormatClass(crtTbl.getInputFormat());
	tbl.setOutputFormatClass(crtTbl.getOutputFormat());

	if (crtTbl.isExternal())
		tbl.setProperty("EXTERNAL", "TRUE");
	//add by allison for pb
	//TODO: need more design
	if(crtTbl.getSerName() != null && crtTbl.getSerName().equalsIgnoreCase(ProtobufSerDe.class.getName())){
		tbl.setProperty("PB_TABLE", "TRUE");
		
		tbl.setProperty("PB_FILE", crtTbl.getPb_file_path());
		tbl.setProperty(Constants.PB_MSG_NAME, tbl.getName());
		tbl.setProperty(Constants.PB_JAR_NAME, "");//TODO:path
		tbl.setProperty(Constants.PB_OUTER_CLASS_NAME, crtTbl.getPb_outer_name());
	}

	// If the sorted columns is a superset of bucketed columns, store this fact.
	// It can be later used to
	// optimize some group-by queries. Note that, the order does not matter as
	// long as it in the first
	// 'n' columns where 'n' is the length of the bucketed columns.
	if ((tbl.getBucketCols() != null) && (tbl.getSortCols() != null)) {
		List<String> bucketCols = tbl.getBucketCols();
		List<Order> sortCols = tbl.getSortCols();

		if ((sortCols.size() > 0) && (sortCols.size() >= bucketCols.size())) {
			boolean found = true;

			Iterator<String> iterBucketCols = bucketCols.iterator();
			while (iterBucketCols.hasNext()) {
				String bucketCol = iterBucketCols.next();
				boolean colFound = false;
				for (int i = 0; i < bucketCols.size(); i++) {
					if (bucketCol.equals(sortCols.get(i).getCol())) {
						colFound = true;
						break;
					}
				}
				if (colFound == false) {
					found = false;
					break;
				}
			}
			if (found)
				tbl.setProperty("SORTBUCKETCOLSPREFIX", "TRUE");
		}
	}

	try {
		tbl.setOwner(SessionState.get().getUserName());
	} catch (Exception e) {
		console.printError("Unable to get current user: " + e.getMessage(), StringUtils.stringifyException(e));
		return 1;
	}
	// set create time
	tbl.getTTable().setCreateTime((int) (System.currentTimeMillis() / 1000));

	if (crtTbl.getCols() != null) {
		tbl.setFields(crtTbl.getCols());
	}
	
	// add by konten
	if(crtTbl.getIfCompressed())
	{
	    tbl.setCompressed(true);
	}
	else
	{
	    tbl.setCompressed(false);
	}
		
	if(crtTbl.getTblType() == 0)
	{
	    tbl.setTableType("text");
	}
	else if(crtTbl.getTblType() == 1)
	{
	    tbl.setTableType("format");
    }
	else if(crtTbl.getTblType() == 2)
	{
	    tbl.setTableType("column");
	}
	else
	{
	    
	}
	
	if(crtTbl.getTblType() == 2)
	{
    	ArrayList< ArrayList<String> > projectionInfos = crtTbl.getProjectionInfos();	
    	
    	LOG.error("projectionInfos null:"+ (projectionInfos == null));
    	String projectionString = ""; //此处需要将projecton从name转换成index
    	if(projectionInfos != null)
    	{    	   
    	    ArrayList<String> allProjectionFields = new ArrayList<String>(10);
    	    for(int i = 0; i < projectionInfos.size(); i++)
    	    {
    	        ArrayList<String> subProjection = projectionInfos.get(i);
    	        
    	        String subProjectionString = "";
    	        for(int j = 0; j < subProjection.size(); j++)
    	        {
    	            if(j == 0)
    	            {
    	                subProjectionString = getFieldIndxByName(subProjection.get(j), crtTbl.getCols());
    	            }
    	            else
    	            {
    	                subProjectionString += "," + getFieldIndxByName(subProjection.get(j), crtTbl.getCols());
    	            }
    	            
    	            allProjectionFields.add(subProjection.get(j));
    	        }
    	        	        
    	        if(i == 0)
    	        {
    	            projectionString = subProjectionString;
    	        }
    	        else
    	        {
    	            projectionString += ";" + subProjectionString;   
    	        }
    	    }
    	    
    	    // 
    	    String remainFieldString = "";
    	    int counter = 0;
    	    List<FieldSchema> fields = crtTbl.getCols();
    	    if(fields != null)
    	    {
    	        for(int i = 0; i < fields.size(); i++)
    	        {
    	            boolean found = false;
    	            String fieldString = fields.get(i).getName();
    	            for(int j = 0; j < allProjectionFields.size(); j++)
    	            {
    	                if(fieldString.equalsIgnoreCase(allProjectionFields.get(j)))
    	                {
    	                    found = true;
    	                    break;
    	                }
    	            }
    	            
    	            if(!found)
    	            {
    	                if(counter == 0)
    	                {
    	                    remainFieldString = (""+i);
    	                    counter++;
    	                }
    	                else
    	                {
    	                    remainFieldString += "," + (""+i);
                        }
    	            }
    	        }
    	    }
    	    
    	    projectionString += ";" + remainFieldString;
    	}
    	else //
    	{
    	    List<FieldSchema> fields = crtTbl.getCols();
    	    if(fields != null)
    	    {
    	        for(int i = 0; i < fields.size(); i++)
    	        {
    	            if(i == 0)
    	            {
    	                projectionString = (""+i);
    	            }
    	            else
    	            {
    	                projectionString += ";" + (""+i);
    	            }
    	        }
    	    }    	    
    	}
    	
    	tbl.setProjection(projectionString);
	}	
	
	// set index info into Parameters. create 的时候只会有一个index. 总体的格式为:
	// IndexNum:num; Index1:name,field,type; Index2:name,field,type;...
	
	String indexName = ""; 
	int indexType = 1;
	if(crtTbl.getIndexInfo() != null)
	{
	    indexName = crtTbl.getIndexInfo().name;
	    List<String> fieldList = crtTbl.getIndexInfo().fieldList;
	    indexType = crtTbl.getIndexInfo().indexType;
	    
	    /*
	     * 匿名索引,内部指定名字.规则为: 
	     * 1,第一个字段名+索引个数的编号.如: 第一个匿名索引名字为 a0,第二个为a1, etc.
	     * 2,不考虑重复的索引. 可以创建多个相同索引字段的索引,不论是否匿名. 
	     */ 
	    if(indexName == null || indexName.length() == 0) 
	    {
	        indexName = fieldList.get(0)+"0";
	    }
	    
	    // 判断名字是否重复
	    int indexNum = 0;
        String indexNumString = tbl.getParameters().get("indexNum"); 
        if(indexNumString != null && indexNumString.length() != 0)
        {
            indexNum = Integer.valueOf(indexNumString);
        }
        for(int i = 0; i < indexNum; i++)
        {
            String indexInfoString = tbl.getParameters().get("index"+i);
            if(indexInfoString != null)
            {
                String[] indexInfo = indexInfoString.split(";");
                
                if(indexInfo != null && indexName.equalsIgnoreCase(indexInfo[0]))
                {
                    console.printError("index already exist");
                    return 1;
                }
            }
        }
        
	    String fieldListString = "";
        for(int i = 0; i < fieldList.size(); i++)
        {
            if(i == 0)
            {
                fieldListString = getFieldIndxByName(fieldList.get(i), crtTbl.getCols());
            }
            else
            {
                fieldListString += "," + getFieldIndxByName(fieldList.get(i), crtTbl.getCols());
            }
        }
        
        String indexInfoString = indexName + ";" + fieldListString + ";"+ indexType;
                
        tbl.setIndexNum(""+(indexNum+1));// 设置索引个数为1
        tbl.setIndexInfo(indexNum, indexInfoString);
        
        // 索引存储位置呢??? 在HiveMetaStore中设定???
        
     // create index konten
        db.createIndex(tbl);
	}
	
	// create the table
	db.createTable(tbl, crtTbl.getIfNotExists());
	
	Table retTbl = db.getTable(SessionState.get().getDbName(), tbl.getName());
	
//	if(tbl.getTTable().equals(retTbl.getTTable()) == false)
//		LOG.error("table not equal");
	
//	LOG.debug("insert: " + tbl.getTTable().toString());
	
//	LOG.debug("select: " + retTbl.getTTable().toString());
	
	return 0;
}

private String getFieldIndxByName(String name, List<FieldSchema> fields)
{    
    for(int i = 0; i < fields.size(); i++)
    {
        if(name.equalsIgnoreCase(fields.get(i).getName()))
        {
            return ""+i;
        }
    }
    
    return ""+ -1;
}
/**
 * Create a new table like an existing table.
 * 
 * @param db The database in question.
 * @param crtTbl This is the table we're creating.
 * @return Returns 0 when execution succeeds and above 0 if it fails.
 * @throws HiveException Throws this exception if an unexpected error occurs.
 */
private int createTableLike(Hive db, createTableLikeDesc crtTbl) throws HiveException {
	if(!db.hasAuth(SessionState.get().getUserName(), Hive.Privilege.CREATE_PRIV, SessionState.get().getDbName(), null)){
		console.printError("user : " + SessionState.get().getUserName() + " do not have privilege on DB : " +  SessionState.get().getDbName());
		return 1;
	}
	// Get the existing table
	Table tbl = db.getTable(SessionState.get().getDbName(), crtTbl.getLikeTableName());
	StorageDescriptor tblStorDesc = tbl.getTTable().getSd();

	tbl.getTTable().setTableName(crtTbl.getTableName());
	if(tbl.isPartitioned() && crtTbl.isExternal()){
		throw new SemanticException("exteranl table do not allow partitioned and table : " + tbl.getName() + " is partitioned" );
	}
	if(tbl.isPartitioned()){

		tbl.getTTable().getPriPartition().setTableName(crtTbl.getTableName());
		if(tbl.getTTable().getSubPartition() != null){

			tbl.getTTable().getSubPartition().setTableName(crtTbl.getTableName());
		}
	}

	if (crtTbl.isExternal()) {
		tbl.setProperty("EXTERNAL", "TRUE");
	} else {
		tbl.setProperty("EXTERNAL", "FALSE");
	}

	if (crtTbl.getLocation() != null) {
		tblStorDesc.setLocation(crtTbl.getLocation());
	} else {
		tblStorDesc.setLocation(null);
		tblStorDesc.unsetLocation();
	}

	tbl.setOwner(SessionState.get().getUserName());
	 db.createIndex(tbl);
	 
	// create the table
	db.createTable(tbl, crtTbl.getIfNotExists());
	return 0;
}

private int createDatabase(Hive db,createDatabaseDesc createDb){
	
	try{
		if(!db.hasAuth(SessionState.get().getUserName(), Hive.Privilege.CREATE_PRIV, null, null)){
			console.printError("user : " + SessionState.get().getUserName() + " do not have privilege to create DB!" );
			return 1;
		}
	db.createDatabase(createDb.getDbname());
	}
	catch(AlreadyExistsException e){
		console.printError("database : " + createDb.getDbname() + " already exist!");
		return 1;
	}
	catch(MetaException e){
		console.printError("create database : " + createDb.getDbname() + " meta error!");
		return 1;
	}
	catch(TException e){
		console.printError("create database TException : " + e.getMessage());
		return 1;
	}
	catch(HiveException e){
		console.printError("create database HiveException : " + e.getMessage());
		return 1;
	}
	return 0;
}
private int dropDatabase(Hive db,dropDatabaseDesc dropDb){
	if(dropDb.getDbname().equalsIgnoreCase(SessionState.get().getDbName())){
		console.printError("can't drop current database : " + dropDb.getDbname());
		return 2;
	}
	try{
		if(!db.hasAuth(SessionState.get().getUserName(), Hive.Privilege.DROP_PRIV, null, null)){
			console.printError("user : " + SessionState.get().getUserName() + " do not have privilege to drop DB!" );
			return 1;
		}
	db.dropDatabase(dropDb.getDbname());
	}
	catch(MetaException e){
		console.printError("drop database : " + dropDb.getDbname() + " meta error!");
		return 1;
	}
	catch(TException e){
		console.printError("drop database TException : " + e.getMessage());
		return 1;
	}
	catch(HiveException e){
		console.printError("drop database HiveException : " + e.getMessage());
		return 1;
	}
	return 0;
	
}
private int useDatabase(Hive db,useDatabaseDesc useDb){
	Object newDb = null;
	try{
		newDb = db.getDatabase(useDb.getChangeToDB());
	}catch(Exception e){
		//console.printError("error: " + e.getMessage());
		console.printError("error: can't ues database :" + useDb.getChangeToDB() + " ,it does not exisit!" );
		return 1;
	}
//	if(newDb == null){
//		console.printError("error: can't ues database :" + useDb.getChangeToDB() + " ,it does not exisit!" );
//		return 2;
//	}
	
	SessionState.get().setDbName(useDb.getChangeToDB());
	return 0;
}
private int showDatabases(Hive db,showDBDesc showDb) throws HiveException{
	List<String> dbs = db.getDatabases();
	// write the results in the file
	try {
		FileSystem fs = showDb.getResFile().getFileSystem(conf);
		DataOutput outStream = (DataOutput)fs.create(showDb.getResFile());
		SortedSet<String> sortedDBs = new TreeSet<String>(dbs);
		Iterator<String> iterDBs = sortedDBs.iterator();

		while (iterDBs.hasNext()) {
			// create a row per table name
			outStream.writeBytes(iterDBs.next());
			outStream.write(terminator);
		}
		((FSDataOutputStream)outStream).close();
	} catch (FileNotFoundException e) {
		LOG.warn("show databases: " + StringUtils.stringifyException(e));
		return 1;
	} catch (IOException e) {
		LOG.warn("show databases: " + StringUtils.stringifyException(e));
		return 1;
	} catch (Exception e) {
		throw new HiveException(e.toString());
	}
	return 0;
}

/**
 * Write a list of index to console.
 */
    private int showIndex(Hive db, showIndexDesc showIndexsDesc) throws HiveException
    {
        if(showIndexsDesc.getOP() == showIndexTypes.SHOWTABLEINDEX)
        {
            String tblName = showIndexsDesc.getTblName();
            String dbName = showIndexsDesc.getDBName();
            
            if(dbName.length() == 0)
            {
                dbName = SessionState.get().getDbName();
                showIndexsDesc.setDBName(dbName);
            }
            
            if(tblName == null || tblName.length() == 0 || dbName.length() == 0)
            {
                console.printError("Table name error");
                return 1;
            }
            
            List<IndexItem> indexInfo = db.getAllIndexTable(dbName, tblName);
            if(indexInfo == null || indexInfo.isEmpty())
            {
                System.out.println("No index found in table:"+tblName);
                return 0;
            }
            for(int i = 0; i < indexInfo.size(); i++)
            {
                System.out.println(i + ", fieldList:"+indexInfo.get(i).getFieldList()
                                   +", type:" + indexInfo.get(i).getType()
                                   +", location:"+indexInfo.get(i).getLocation()
                                   +", status:" + indexInfo.get(i).getStatus());
            }
            
            System.out.println(indexInfo.size() + " index found");
            return 0;            
        }
        else  // show all index
        {
            List<IndexItem> indexInfo = db.getAllIndexSys();
            if(indexInfo == null || indexInfo.isEmpty())
            {
                System.out.println("No index found in sys");
                return 0;
            }
            for(int i = 0; i < indexInfo.size(); i++)
            {
                System.out.println(i + ", db:"+indexInfo.get(i).getDb()
                                   + ", tbl:" + indexInfo.get(i).getTbl()
                                   + ", fieldList:"+indexInfo.get(i).getFieldList()
                                   +", type:" + indexInfo.get(i).getType()
                                   +", location:"+indexInfo.get(i).getLocation()
                                   +", status:" + indexInfo.get(i).getStatus());
            }
            
            
            System.out.println(indexInfo.size() + " index found");
            
            return 0;  
        }
    }
}
