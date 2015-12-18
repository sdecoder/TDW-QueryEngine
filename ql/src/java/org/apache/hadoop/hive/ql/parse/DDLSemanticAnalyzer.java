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

package org.apache.hadoop.hive.ql.parse;


import java.io.FileReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.antlr.runtime.ANTLRReaderStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
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
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.showDBDesc;
import org.apache.hadoop.hive.ql.plan.indexInfoDesc;
import org.apache.hadoop.hive.ql.plan.showFunctionsDesc;
import org.apache.hadoop.hive.ql.plan.showIndexDesc;
import org.apache.hadoop.hive.ql.plan.showPartitionsDesc;
import org.apache.hadoop.hive.ql.plan.showTablesDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.plan.truncatePartitionDesc;
import org.apache.hadoop.hive.ql.plan.truncateTableDesc;
import org.apache.hadoop.hive.ql.plan.useDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.alterTableDesc.alterTableTypes;
import org.apache.hadoop.hive.ql.plan.showIndexDesc.showIndexTypes;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.protobuf.ProtobufSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.datanucleus.store.rdbms.schema.IndexInfo;

public class DDLSemanticAnalyzer extends BaseSemanticAnalyzer {
	private static final Log LOG = LogFactory
			.getLog("hive.ql.parse.DDLSemanticAnalyzer");
	public static final Map<Integer, String> TokenToTypeName = new HashMap<Integer, String>();
	private String pb_msg_outer_name = null;
	static {
		TokenToTypeName
				.put(HiveParser.TOK_BOOLEAN, Constants.BOOLEAN_TYPE_NAME);
		TokenToTypeName
				.put(HiveParser.TOK_TINYINT, Constants.TINYINT_TYPE_NAME);
		TokenToTypeName.put(HiveParser.TOK_SMALLINT,
				Constants.SMALLINT_TYPE_NAME);
		TokenToTypeName.put(HiveParser.TOK_INT, Constants.INT_TYPE_NAME);
		TokenToTypeName.put(HiveParser.TOK_BIGINT, Constants.BIGINT_TYPE_NAME);
		TokenToTypeName.put(HiveParser.TOK_FLOAT, Constants.FLOAT_TYPE_NAME);
		TokenToTypeName.put(HiveParser.TOK_DOUBLE, Constants.DOUBLE_TYPE_NAME);
		TokenToTypeName.put(HiveParser.TOK_STRING, Constants.STRING_TYPE_NAME);
		TokenToTypeName.put(HiveParser.TOK_DATE, Constants.DATE_TYPE_NAME);
		TokenToTypeName.put(HiveParser.TOK_DATETIME,
				Constants.DATETIME_TYPE_NAME);
		TokenToTypeName.put(HiveParser.TOK_TIMESTAMP,
				Constants.TIMESTAMP_TYPE_NAME);
	}
	private static final String TEXTFILE_INPUT = TextInputFormat.class
			.getName();
	private static final String TEXTFILE_OUTPUT = IgnoreKeyTextOutputFormat.class
			.getName();
	private static final String SEQUENCEFILE_INPUT = SequenceFileInputFormat.class
			.getName();
	private static final String SEQUENCEFILE_OUTPUT = SequenceFileOutputFormat.class
			.getName();
	private static final String RCFILE_INPUT = RCFileInputFormat.class
			.getName();
	private static final String RCFILE_OUTPUT = RCFileOutputFormat.class
			.getName();

        	
	//allison
	//TODO : for pb inputformat and outputformat
	private static final String PB_INPUT = "protobuf.mapred.ProtobufInputFormat";//"error";
	private static final String PB_OUTPUT = "protobuf.mapred.HiveProtobufOutputFormat";//"error";
	private static final String PB_SERDE = ProtobufSerDe.class.getName();
    //end
	

	private static final String COLUMNAR_SERDE = ColumnarSerDe.class.getName();

	public static String getTypeName(int token) {
		return TokenToTypeName.get(token);
	}

	public DDLSemanticAnalyzer(HiveConf conf) throws SemanticException {
		super(conf);
	}

	@Override
	public void analyzeInternal(ASTNode ast) throws SemanticException {
		if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE)
			analyzeCreateTable(ast);
		else if (ast.getToken().getType() == HiveParser.TOK_DROPTABLE)
			analyzeDropTable(ast);
		else if (ast.getToken().getType() == HiveParser.TOK_TRUNCATETABLE)
			analyzeTruncateTable(ast);
		else if (ast.getToken().getType() == HiveParser.TOK_DESCTABLE) {
			ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
			analyzeDescribeTable(ast);
		} else if (ast.getToken().getType() == HiveParser.TOK_SHOWTABLES) {
			ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
			analyzeShowTables(ast);
		} else if (ast.getToken().getType() == HiveParser.TOK_SHOWFUNCTIONS) {
			ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
			analyzeShowFunctions(ast);
		} else if (ast.getToken().getType() == HiveParser.TOK_DESCFUNCTION) {
			ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
			analyzeDescFunction(ast);
		} else if (ast.getToken().getType() == HiveParser.TOK_MSCK) {
			ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
			analyzeMetastoreCheck(ast);
		} else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_RENAME)
			analyzeAlterTableRename(ast);
		else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDCOLS)
			analyzeAlterTableModifyCols(ast, alterTableTypes.ADDCOLS);
		else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_REPLACECOLS)
			analyzeAlterTableModifyCols(ast, alterTableTypes.REPLACECOLS);
		else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDPARTS) {
			analyzeAlterTableAddParts(ast,false);
		} else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDSUBPARTS) {
			analyzeAlterTableAddParts(ast,true);
		}else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_DROPPARTS)
			analyzeAlterTableDropParts(ast);
		else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_PROPERTIES)
			analyzeAlterTableProps(ast);
		else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES)
			analyzeAlterTableSerdeProps(ast);
		else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SERIALIZER)
			analyzeAlterTableSerde(ast);
		else if (ast.getToken().getType() == HiveParser.TOK_SHOWPARTITIONS) {
			ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
			analyzeShowPartitions(ast);
		}
		else if (ast.getToken().getType() == HiveParser.TOK_CREATE_DATABASE) {
				//ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
				analyzeCreateDatabase(ast);
		}
		else if (ast.getToken().getType() == HiveParser.TOK_DROP_DATABASE) {
					//ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
				analyzeDropDatabase(ast);
		}
		else if (ast.getToken().getType() == HiveParser.TOK_USE_DATABASE) {
			//ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
			analyzeUseDatabase(ast);
		}
		//TODO:show databases
		else if(ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDDEFAULTPARTITION){
			analyzeAddDefaultPartition(ast);
		}
		else if(ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_TRUNCATE_PARTITION){
			analyzeTruncatePartition(ast);
		}else if(ast.getToken().getType() == HiveParser.TOK_SHOW_DATABASES){
			ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
			analyzeShowDatabases(ast);
		}
		// konten add index begin
		else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDINDEX){		   
            analyzeAlterTableAddIndex(ast);
		}
		else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_DROPINDEX){
            analyzeAlterTableDropIndex(ast);
        }
		else if (ast.getToken().getType() == HiveParser.TOK_SHOWTABLEINDEXS){
            analyzeShowTableIndex(ast);
        }
		else if (ast.getToken().getType() == HiveParser.TOK_SHOWALLINDEXS){
		    analyzeShowAllIndex(ast);
        }
		// konten add index end
		else {
			throw new SemanticException("Unsupported DDL command.");
		}
	}

	private void analyzeAlterTableDropIndex(ASTNode ast)
    {
	    int childNum = ast.getChildCount();        
        String tableName = unescapeIdentifier(ast.getChild(0).getText());       
        
        indexInfoDesc indexInfo = new indexInfoDesc();
        indexInfo.name = unescapeIdentifier(ast.getChild(1).getChild(0).getText());               
                
        
        alterTableDesc alterTblDesc = new alterTableDesc(tableName, null, alterTableTypes.DROPINDEX);
        alterTblDesc.setIndexInfo(indexInfo);        
        rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf)); 
    }

    private void analyzeAlterTableAddIndex(ASTNode ast)
    {
        int childNum = ast.getChildCount();        
        String tableName = unescapeIdentifier(ast.getChild(0).getText());
        
        indexInfoDesc indexInfo = new indexInfoDesc();
        
        for(int i = 1; i < childNum; i++) //index_name and index_field
        {
            ASTNode child = (ASTNode)ast.getChild(i);
            if(child.getToken().getType() == HiveParser.TOK_INDEXNAME)
            {
                indexInfo.name = child.getChild(0).getText();                
            }
            
            if(child.getToken().getType() == HiveParser.TOK_INDEXFIELD)
            {                
                int fieldNum = child.getChildCount();
                
                for(int k = 0; k < fieldNum; k++)
                {
                    ASTNode fieldNode = (ASTNode)child.getChild(k);
                                     
                    indexInfo.fieldList.add(fieldNode.getText());                    
                }
            }
        }
        
        if(indexInfo.fieldList.size() > 1)
        {
            indexInfo.indexType = 2;
        }
        else
        {
            indexInfo.indexType = 1;
        }
        
        //String tblName = tableName;
        //List<FieldSchema> newCols = getColumns((ASTNode) ast.getChild(1));
        alterTableDesc alterTblDesc = new alterTableDesc(tableName, null, alterTableTypes.ADDINDEX);
        alterTblDesc.setIndexInfo(indexInfo);
        
        rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));        
    }

	private void analyzeCreateTable(ASTNode ast) throws SemanticException {
		String tableName = unescapeIdentifier(ast.getChild(0).getText());
		String likeTableName = null;
		List<FieldSchema> cols = null;
		List<FieldSchema> partCols = null;
		PartitionDesc partDesc = null;

		List<String> bucketCols = null;
		List<Order> sortCols = null;
		int numBuckets = -1;
		String fieldDelim = null;
		String fieldEscape = null;
		String collItemDelim = null;
		String mapKeyDelim = null;
		String lineDelim = null;
		String comment = null;
		String inputFormat = TEXTFILE_INPUT;
		String outputFormat = TEXTFILE_OUTPUT;
		String location = null;
		String serde = null;
		Map<String, String> mapProp = null;
		boolean ifNotExists = false;
		boolean isExt = false;
		
		ArrayList< ArrayList<String> > projectionInfos = null; 
		boolean compress = false;
		int tblType = -1; // 0,text; 1, format; 2, column
		
		indexInfoDesc indexInfo = null;
                //allison: for pb create table
		String pbDefFilePaht = null;

		if ("SequenceFile".equalsIgnoreCase(conf
				.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT))) {
			inputFormat = SEQUENCEFILE_INPUT;
			outputFormat = SEQUENCEFILE_OUTPUT;
		}

		LOG.info("Creating table : " + tableName);
		int numCh = ast.getChildCount();
		for (int num = 1; num < numCh; num++) {
			ASTNode child = (ASTNode) ast.getChild(num);
			switch (child.getToken().getType()) {
			case HiveParser.TOK_IFNOTEXISTS:
				ifNotExists = true;
				break;
			case HiveParser.KW_EXTERNAL:
				isExt = true;
				break;
			case HiveParser.TOK_LIKETABLE:
				if (child.getChildCount() > 0) {
					likeTableName = unescapeIdentifier(child.getChild(0)
							.getText());
				}
				break;
			case HiveParser.TOK_TABCOLLIST:
				cols = getColumns(child);
				break;
			case HiveParser.TOK_TABLECOMMENT:
				comment = unescapeSQLString(child.getChild(0).getText());
				break;
			case /* HiveParser.TOK_TABLEPARTCOLS */HiveParser.TOK_TABLEPARTITION:// table
																				// partition
																				// defined
				// partCols = getColumns((ASTNode)child.getChild(0));
				partDesc = getPartDesc((ASTNode) child,cols);
				
				partDesc.setTableName(tableName);
				if(partDesc.getSubPartition() != null)
					partDesc.getSubPartition().setTableName(tableName);

				break;
			case HiveParser.TOK_TABLEBUCKETS:
				bucketCols = getColumnNames((ASTNode) child.getChild(0));
				if (child.getChildCount() == 2)
					numBuckets = (Integer.valueOf(child.getChild(1).getText()))
							.intValue();
				else {
					sortCols = getColumnNamesOrder((ASTNode) child.getChild(1));
					numBuckets = (Integer.valueOf(child.getChild(2).getText()))
							.intValue();
				}
				break;
			case HiveParser.TOK_TABLEROWFORMAT:

				child = (ASTNode) child.getChild(0);
				int numChildRowFormat = child.getChildCount();
				for (int numC = 0; numC < numChildRowFormat; numC++) {
					ASTNode rowChild = (ASTNode) child.getChild(numC);
					switch (rowChild.getToken().getType()) {
					case HiveParser.TOK_TABLEROWFORMATFIELD:
						fieldDelim = unescapeSQLString(rowChild.getChild(0)
								.getText());
						if (rowChild.getChildCount() >= 2) {
							fieldEscape = unescapeSQLString(rowChild
									.getChild(1).getText());
						}
						break;
					case HiveParser.TOK_TABLEROWFORMATCOLLITEMS:
						collItemDelim = unescapeSQLString(rowChild.getChild(0)
								.getText());
						break;
					case HiveParser.TOK_TABLEROWFORMATMAPKEYS:
						mapKeyDelim = unescapeSQLString(rowChild.getChild(0)
								.getText());
						break;
					case HiveParser.TOK_TABLEROWFORMATLINES:
						lineDelim = unescapeSQLString(rowChild.getChild(0)
								.getText());
						break;
					default:
						assert false;
					}
				}
				break;
			case HiveParser.TOK_TABLESERIALIZER:

				child = (ASTNode) child.getChild(0);
				serde = unescapeSQLString(child.getChild(0).getText());
				if (child.getChildCount() == 2) {
					if(mapProp == null)
						mapProp = new HashMap<String, String>();
					ASTNode prop = (ASTNode) ((ASTNode) child.getChild(1))
							.getChild(0);
					for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
						String key = unescapeSQLString(prop.getChild(propChild)
								.getChild(0).getText());
						String value = unescapeSQLString(prop.getChild(
								propChild).getChild(1).getText());
						mapProp.put(key, value);
					}
				}
				break;
			case HiveParser.TOK_TBLSEQUENCEFILE:
				inputFormat = SEQUENCEFILE_INPUT;
				outputFormat = SEQUENCEFILE_OUTPUT;
				break;
			case HiveParser.TOK_TBLTEXTFILE:
				inputFormat = TEXTFILE_INPUT;
				outputFormat = TEXTFILE_OUTPUT;
				//tblType = 0;
				break;
			case HiveParser.TOK_TBLRCFILE:
				inputFormat = RCFILE_INPUT;
				outputFormat = RCFILE_OUTPUT;
				serde = COLUMNAR_SERDE;
				break;
                        				
				//allison: for STORED AS PBFILE USING 'a path'
			case HiveParser.TOK_PB_FILE:
				inputFormat = PB_INPUT;
				outputFormat = PB_OUTPUT;
				serde = PB_SERDE;
				pbDefFilePaht = unescapeSQLString(child.getChild(0).getText());	
				
				if (cols != null){
					throw new SemanticException("PB stroed table no not need columns!");
				}
				//get table define from pb file
				cols = getColsFromPBDef(tableName,pbDefFilePaht);
				
//				if(mapProp == null){
//					mapProp = new HashMap<String, String>();
//					
//				}
//				mapProp.put(Constants.PB_OUTER_CLASS_NAME,pb_msg_outer_name);
				
				break;
				//end
			case HiveParser.TOK_TABLEFILEFORMAT:
				inputFormat = unescapeSQLString(child.getChild(0).getText());
				outputFormat = unescapeSQLString(child.getChild(1).getText());
				break;
			case HiveParser.TOK_TABLELOCATION:

// Disallowing Table Location for internal table, Taojiang 2010.11 
				if (!isExt) {
					throw new SemanticException("Table Location disallowed for internal table");
				}
// END OF Disallowing...
				
				location = unescapeSQLString(child.getChild(0).getText());
				break;
				
				// konten add for 列存储 begin 
			 case HiveParser.TOK_TBLFORMATFILE:
			     if (isExt) 
			     {
			         throw new SemanticException("format storage disallowed for extended table");
	             }
			     
                tblType = 1;
                
                int formatChildNum = child.getChildCount();
                child = (ASTNode) child.getChild(0);
                if(formatChildNum != 0 && child.getToken().getType() == HiveParser.TOK_COMPRESS)
                {
                    LOG.error("compress set");
                    compress = true;
                }
                else                    
                {
                    LOG.error("not compress");
                }
                
             // set input and output format;
                inputFormat = "StorageEngineClient.FormatStorageInputFormat";
                outputFormat = "StorageEngineClient.FormatStorageHiveOutputFormat";
                serde = "StorageEngineClient.FormatStorageSerDe";
                break;
	                
			case HiveParser.TOK_TBLCOLUMNFILE:			    
			    
			    if (isExt) 
                {
                    throw new SemanticException("column storage disallowed for extended table");
                }
			    
			    tblType = 2;
			    
			    int columnChildNum = child.getChildCount();
			    if(columnChildNum == 0)
			    {
			        LOG.error("no compress, no project");
			    }
			    else if(columnChildNum == 1) // compress or projection
			    {
			        child = (ASTNode) child.getChild(0);
			        if(child.getToken().getType() == HiveParser.TOK_COMPRESS)
			        {
			            LOG.error("column compress");
			            compress = true;
			        }
			        else if(child.getToken().getType() == HiveParser.TOK_PROJECTION)
			        {   
			            if(projectionInfos == null)
			            {
			                projectionInfos = new ArrayList<ArrayList<String>>(10);
			            }
			            
			            getProjectionInfo(child, projectionInfos);
			        }
			        else
			        {
			            LOG.error("column, not compress or projection");
			        }
			    }
			    else //if(childNum == 2) // 0 compress and 1 project; 
			    {
			        LOG.error("child num:"+columnChildNum);
			        ASTNode tmpChild = (ASTNode) child.getChild(0);
			        if(tmpChild.getToken().getType() == HiveParser.TOK_COMPRESS)
			        {
			            compress = true;
			            LOG.error("compress set 0");
			        }
			        else
			        {
			            if(projectionInfos == null)
                        {
                            projectionInfos = new ArrayList<ArrayList<String>>(10);
                        }
			            getProjectionInfo(tmpChild, projectionInfos);
			        }
			        
			        tmpChild = (ASTNode) child.getChild(1);
			        if(tmpChild.getToken().getType() == HiveParser.TOK_COMPRESS)
                    {
                        compress = true;
                        LOG.error("compress set 1");
                    }
                    else
                    {
                        if(projectionInfos == null)
                        {
                            projectionInfos = new ArrayList<ArrayList<String>>(10);
                        }
                        getProjectionInfo(tmpChild, projectionInfos);
                    }
			        			        
			        if(!compress || projectionInfos == null)
			        {
			            throw new SemanticException("compress or projection error");
			        }
			    }
			    
			    // set input and output format;
                inputFormat = "StorageEngineClient.ColumnStorageInputFormat";
                outputFormat = "StorageEngineClient.ColumnStorageHiveOutputFormat";
                serde = "StorageEngineClient.FormatStorageSerDe";
			    break;

			 // konten add for format&column storage end
			    
			    // konten add for index begin
			case HiveParser.TOK_INDEX:       
			    
			    if (isExt) 
                {
                    throw new SemanticException("index disallowed for extended table");
                }
			    
			    indexInfo = new indexInfoDesc();
			    
			    int indexChildNum = child.getChildCount();			    
                for(int i = 0; i < indexChildNum; i++)
                {
                    ASTNode tmpChild = (ASTNode) child.getChild(i);
                    if(tmpChild.getToken().getType() == HiveParser.TOK_INDEXNAME)
                    {
                        indexInfo.name = tmpChild.getChild(0).getText();
                                                
                    }
                    
                    if(tmpChild.getToken().getType() == HiveParser.TOK_INDEXFIELD)
                    {
                        int indexFieldNum = tmpChild.getChildCount();
                        
                        for(int k = 0; k < indexFieldNum; k++)
                        {
                            ASTNode fieldNode = (ASTNode)tmpChild.getChild(k);
                            
                            indexInfo.fieldList.add(fieldNode.getText());
                        }
                    }                    
                }
                
                //判断索引类型.
                if(indexInfo.fieldList.size() > 1)
                {
                    indexInfo.indexType = 2;
                }
                else if(indexInfo.fieldList.size() == 1) // 判断是否primary index
                { 
                    String fieldName = indexInfo.fieldList.get(0);
                    
                    Iterator<FieldSchema> tblColIter = cols.iterator();
                    while (tblColIter.hasNext())
                    {
                        FieldSchema schema = tblColIter.next(); 
                        String colName = schema.getName();                        
                        String colType = schema.getType();
                        if (colName.equalsIgnoreCase(fieldName) && colType.equalsIgnoreCase(Constants.INT_TYPE_NAME))
                        {
                            indexInfo.indexType = 0;
                            break;
                        }
                    }
                }
                
			    break;
			    // konten add for index end
			default:
				assert false;
			}
		}		

		if(isExt && partDesc != null){
			throw new SemanticException("external table do not allow create partitions");
		}
		
		if(isExt && serde != null && serde.endsWith("StorageEngineClient.FormatStorageSerDe"))
        {
		    throw new SemanticException("external table do not allow format storage");
        }
		
		if(indexInfo != null && (serde == null || !serde.endsWith("StorageEngineClient.FormatStorageSerDe")) )
		{		    
		    throw new SemanticException("index only for format/column storage");
		}
		
		if (likeTableName == null) {
			createTableDesc crtTblDesc = new createTableDesc(tableName, isExt,
					cols, partDesc, bucketCols, sortCols, numBuckets,
					fieldDelim, fieldEscape, collItemDelim, mapKeyDelim,
					lineDelim, comment, inputFormat, outputFormat, location,
					serde, mapProp, ifNotExists, compress, tblType, projectionInfos, indexInfo,
					pbDefFilePaht,this.pb_msg_outer_name);

			validateCreateTable(crtTblDesc);
			rootTasks.add(TaskFactory.get(new DDLWork(crtTblDesc), conf));
		} else {
			createTableLikeDesc crtTblLikeDesc = new createTableLikeDesc(
					tableName, isExt, location, ifNotExists, likeTableName);
			rootTasks.add(TaskFactory.get(new DDLWork(crtTblLikeDesc), conf));
		}

	}

	// 从TOK_PROJECTION节点中获取各个projection的信息;
	private void getProjectionInfo(ASTNode child, ArrayList< ArrayList<String> > projectionInfos)
	{
	    int projectionNum = child.getChildCount();
        LOG.error("column projection, projection num:"+projectionNum);
        
        for(int i = 0; i < projectionNum; i++)
        {
            ASTNode subProjectionNode = (ASTNode) child.getChild(i);
            
            ArrayList<String> subProjectionList = new ArrayList<String>(10);
            int projFieldNum = subProjectionNode.getChildCount();
            for(int j = 0; j < projFieldNum; j++)
            {
                ASTNode subProjectionFieldNode = (ASTNode) subProjectionNode.getChild(j);
                subProjectionList.add(unescapeIdentifier(subProjectionFieldNode.getText()) );
            }
            
            projectionInfos.add(subProjectionList);
        }
        
        // show projectionInfos  
        for(int i = 0; i < projectionInfos.size(); i++)
        {            
            LOG.error("projection " + i + ":");
            
            String tmpString = "";
            ArrayList<String> iter = projectionInfos.get(i);
            for(int j = 0; j < iter.size(); j++)
            {
                tmpString += iter.get(j) + ",";
            }
            LOG.error(tmpString);
        }
	}
	
	private void validateCreateTable(createTableDesc crtTblDesc)
			throws SemanticException {
		// no duplicate column names
		// currently, it is a simple n*n algorithm - this can be optimized later
		// if need be
		// but it should not be a major bottleneck as the number of columns are
		// anyway not so big

		if ((crtTblDesc.getCols() == null)
				|| (crtTblDesc.getCols().size() == 0)) {
			// for now make sure that serde exists
			if (StringUtils.isEmpty(crtTblDesc.getSerName())
					|| SerDeUtils.isNativeSerDe(crtTblDesc.getSerName())) {
				throw new SemanticException(ErrorMsg.INVALID_TBL_DDL_SERDE
						.getMsg());
			}
			return;
		}

		try {
                    	LOG.info("load outputFormat class : " + crtTblDesc.getOutputFormat());
                        Class<?> origin = Class.forName(crtTblDesc.getOutputFormat()/*, true,
                                    JavaUtils.getClassLoader()*/);//TODO: use local classloader
			//JavaUtils;
			LOG.info("load outputFormat class OK!" );
			Class<? extends HiveOutputFormat> replaced = HiveFileFormatUtils
					.getOutputFormatSubstitute(origin);
                        LOG.info("load outputFormat replaced OK!" );
			if (replaced == null)
				throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE
						.getMsg());
		} catch (ClassNotFoundException e) {
			throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE
					.getMsg());
		}

		Iterator<FieldSchema> iterCols = crtTblDesc.getCols().iterator();
		List<String> colNames = new ArrayList<String>();
		while (iterCols.hasNext()) {
			String colName = iterCols.next().getName();
			Iterator<String> iter = colNames.iterator();
			while (iter.hasNext()) {
				String oldColName = iter.next();
				if (colName.equalsIgnoreCase(oldColName))
					throw new SemanticException(ErrorMsg.DUPLICATE_COLUMN_NAMES
							.getMsg() + " : " + oldColName);
			}
			colNames.add(colName);
		}

		if (crtTblDesc.getBucketCols() != null) {
			// all columns in cluster and sort are valid columns
			Iterator<String> bucketCols = crtTblDesc.getBucketCols().iterator();
			while (bucketCols.hasNext()) {
				String bucketCol = bucketCols.next();
				boolean found = false;
				Iterator<String> colNamesIter = colNames.iterator();
				while (colNamesIter.hasNext()) {
					String colName = colNamesIter.next();
					if (bucketCol.equalsIgnoreCase(colName)) {
						found = true;
						break;
					}
				}
				if (!found)
					throw new SemanticException(ErrorMsg.INVALID_COLUMN
							.getMsg());
			}
		}

		if (crtTblDesc.getSortCols() != null) {
			// all columns in cluster and sort are valid columns
			Iterator<Order> sortCols = crtTblDesc.getSortCols().iterator();
			while (sortCols.hasNext()) {
				String sortCol = sortCols.next().getCol();
				boolean found = false;
				Iterator<String> colNamesIter = colNames.iterator();
				while (colNamesIter.hasNext()) {
					String colName = colNamesIter.next();
					if (sortCol.equalsIgnoreCase(colName)) {
						found = true;
						break;
					}
				}
				if (!found)
					throw new SemanticException(ErrorMsg.INVALID_COLUMN
							.getMsg());
			}
		}

		/*
		 * do not need because in getOneLevelDesc we have check the parititon column
		if (crtTblDesc.getPartdesc() != null) {
			// there must be overlap between columns and partitioning columns
			PartitionDesc partdesc = crtTblDesc.getPartdesc();
			String priPartCol = partdesc.getPartitionColumn();
			ListIterator<FieldSchema> columnIter = crtTblDesc.getCols()
					.listIterator();
			while (columnIter.hasNext()) {
				String Colname = columnIter.next().getName();
				if (!Colname.equalsIgnoreCase(priPartCol))
					throw new SemanticException(
							ErrorMsg.PARTITIONING_COLS_SHOULD_IN_COLUMNS
									.getMsg());
			}
			if (partdesc.getSubPartition() != null) {
				String subPartCol = partdesc.getSubPartition()
						.getPartitionColumn();
				while (columnIter.hasPrevious()) {
					String Colname = columnIter.previous().getName();
					if (!Colname.equalsIgnoreCase(priPartCol))
						throw new SemanticException(
								ErrorMsg.PARTITIONING_COLS_SHOULD_IN_COLUMNS
										.getMsg());
				}
			}
		}
		*/
		
		
		
		
		if(crtTblDesc.getProjectionInfos() != null)
		{
		    // 1. 列簇的个数,不能超过20
    		if(crtTblDesc.getProjectionInfos().size() > 20)
            {
                throw new SemanticException(ErrorMsg.PROJECTION_TOO_MANY.getMsg());
            }  
    		
    		// 2. 各个列簇中的字段必须在表中有定义, 且不能有重复    		
    		ArrayList<String> allProjectionField = new ArrayList<String>(20);    		
    		
    		ArrayList<ArrayList<String>> projectionInfos = crtTblDesc.getProjectionInfos();
    		for(int i = 0; i < projectionInfos.size(); i++)
    		{
    		    ArrayList<String> projectField = projectionInfos.get(i);
    		    for(int j = 0; j < projectField.size(); j++)
    		    {
    		        allProjectionField.add(projectField.get(j));
    		    }
    		}
    		
    		Iterator<String> iterProjectionCol = allProjectionField.iterator();
            List<String> tmpFieldNames = new ArrayList<String>();
            while (iterProjectionCol.hasNext())
            {
                String colName = iterProjectionCol.next();
                Iterator<String> iter = tmpFieldNames.iterator();
                while (iter.hasNext())
                {
                    String oldColName = iter.next();
                    if (colName.equalsIgnoreCase(oldColName))
                    {                        
                        throw new SemanticException(ErrorMsg.PROJECTION_FIELD_DUP.getMsg());
                    }
                }
                tmpFieldNames.add(colName);
            }
                                    
            Iterator<String> iter = tmpFieldNames.iterator();
            while (iter.hasNext())
            {
                boolean exist = false;
                String projectionColName = iter.next();
                
                Iterator<FieldSchema> tblColIter = crtTblDesc.getCols().iterator();
                while (tblColIter.hasNext())
                {
                    String colName = tblColIter.next().getName();
                    if (colName.equalsIgnoreCase(projectionColName))
                    {
                        exist = true;
                        break;
                    }
                }

                if(!exist)
                {
                    throw new SemanticException(ErrorMsg.PROJECTION_FIELD_NOT_EXIST.getMsg());
                }
            }
		}
		
		// 3. 字段个数超过20个,必须定义列簇
		if(crtTblDesc.getTblType() == 2 && crtTblDesc.getCols().size() > 20 && crtTblDesc.getProjectionInfos() == null)
		{
		    throw new SemanticException(ErrorMsg.PROJECTION_NOT_DEFINE.getMsg());
		}
		
		// check index info
		if(crtTblDesc.getIndexInfo() != null)
		{
		    // name is cheched at parse, check field only
		    
		    // 是否重复字段
            Iterator<String> iterFieldCol = crtTblDesc.getIndexInfo().fieldList.iterator();
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
                        throw new SemanticException(ErrorMsg.INDEX_FIELD_DUP.getMsg());
                    }
                }
                tmpFieldNames.add(colName);
            }
		    
            // 字段是否存在
		    Iterator<String> iter = crtTblDesc.getIndexInfo().fieldList.iterator();
            while (iter.hasNext())
            {
                boolean exist = false;
                String fieldColName = iter.next();
                
                Iterator<FieldSchema> tblColIter = crtTblDesc.getCols().iterator();
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
                    throw new SemanticException(ErrorMsg.INDEX_FIELD_NOT_EXIST.getMsg());
                }
            }
		}		
	}

	private void analyzeTruncatePartition(ASTNode ast) throws SemanticException{
		String tableName = unescapeIdentifier(ast.getChild(0).getText());
		String pri = null;
		String sub = null;
		
		if(((ASTNode)(ast.getChild(1))).getToken().getType() == HiveParser.TOK_COMPPARTITIONREF){
			pri = unescapeIdentifier(ast.getChild(1).getChild(0).getText().toLowerCase());
			sub = unescapeIdentifier(ast.getChild(1).getChild(1).getText().toLowerCase());
		}
		else if(((ASTNode)(ast.getChild(1))).getToken().getType() == HiveParser.TOK_SUBPARTITIONREF){
			sub = unescapeIdentifier(ast.getChild(1).getChild(0).getText().toLowerCase());
		}
		else{
			pri = unescapeIdentifier(ast.getChild(1).getChild(0).getText().toLowerCase());
		}
		
		truncatePartitionDesc tpd = new truncatePartitionDesc(SessionState.get().getDbName(),tableName,pri,sub);
		rootTasks.add(TaskFactory.get(new DDLWork(tpd), conf));
		
	}
	private void analyzeDropTable(ASTNode ast) throws SemanticException {
		String tableName = unescapeIdentifier(ast.getChild(0).getText());
		dropTableDesc dropTblDesc = new dropTableDesc(tableName);
		rootTasks.add(TaskFactory.get(new DDLWork(dropTblDesc), conf));
	}

	private void analyzeTruncateTable(ASTNode ast) throws SemanticException {
		String tableName = unescapeIdentifier(ast.getChild(0).getText());
		truncateTableDesc truncTblDesc = new truncateTableDesc(tableName);
		rootTasks.add(TaskFactory.get(new DDLWork(truncTblDesc), conf));
	}

	private void analyzeAlterTableProps(ASTNode ast) throws SemanticException {
		String tableName = unescapeIdentifier(ast.getChild(0).getText());
		HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(1))
				.getChild(0));
		alterTableDesc alterTblDesc = new alterTableDesc(
				alterTableTypes.ADDPROPS);
		alterTblDesc.setProps(mapProp);
		alterTblDesc.setOldName(tableName);
		rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
	}

	private void analyzeAlterTableSerdeProps(ASTNode ast)
			throws SemanticException {
		String tableName = unescapeIdentifier(ast.getChild(0).getText());
		HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(1))
				.getChild(0));
		alterTableDesc alterTblDesc = new alterTableDesc(
				alterTableTypes.ADDSERDEPROPS);
		alterTblDesc.setProps(mapProp);
		alterTblDesc.setOldName(tableName);
		rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
	}

	private void analyzeAlterTableSerde(ASTNode ast) throws SemanticException {
		String tableName = unescapeIdentifier(ast.getChild(0).getText());
		String serdeName = unescapeSQLString(ast.getChild(1).getText());
		alterTableDesc alterTblDesc = new alterTableDesc(
				alterTableTypes.ADDSERDE);
		if (ast.getChildCount() > 2) {
			HashMap<String, String> mapProp = getProps((ASTNode) (ast
					.getChild(2)).getChild(0));
			alterTblDesc.setProps(mapProp);
		}
		alterTblDesc.setOldName(tableName);
		alterTblDesc.setSerdeName(serdeName);
		rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
	}

	private HashMap<String, String> getProps(ASTNode prop) {
		HashMap<String, String> mapProp = new HashMap<String, String>();
		for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
			String key = unescapeSQLString(prop.getChild(propChild).getChild(0)
					.getText());
			String value = unescapeSQLString(prop.getChild(propChild).getChild(
					1).getText());
			mapProp.put(key, value);
		}
		return mapProp;
	}

	private static String getTypeStringFromAST(ASTNode typeNode) {
		switch (typeNode.getType()) {
		case HiveParser.TOK_LIST:
			return Constants.LIST_TYPE_NAME + "<"
					+ getTypeStringFromAST((ASTNode) typeNode.getChild(0))
					+ ">";
		case HiveParser.TOK_MAP:
			return Constants.MAP_TYPE_NAME + "<"
					+ getTypeStringFromAST((ASTNode) typeNode.getChild(0))
					+ ","
					+ getTypeStringFromAST((ASTNode) typeNode.getChild(1))
					+ ">";
		default:
			return getTypeName(typeNode.getType());
		}
	}

       	private String getHivePrimitiveTypeFromPBPrimitiveType(String type){
		if(type.equalsIgnoreCase("double")){
			return Constants.DOUBLE_TYPE_NAME;
		}else
		if(type.equalsIgnoreCase("int32")){
			return Constants.INT_TYPE_NAME;
		}else
		if(type.equalsIgnoreCase("int64")){
			return Constants.BIGINT_TYPE_NAME;
		}else
		if(type.equalsIgnoreCase("uint32")){
			return Constants.INT_TYPE_NAME;
		}else
		if(type.equalsIgnoreCase("uint64")){//[TODO]java do not have unsigned type,so we use lager type
			return Constants.BIGINT_TYPE_NAME;
		}else
		if(type.equalsIgnoreCase("sint32")){
			return Constants.INT_TYPE_NAME;
		}else
		if(type.equalsIgnoreCase("sint64")){
			return Constants.BIGINT_TYPE_NAME;
		}else
		if(type.equalsIgnoreCase("fixed32")){
			return Constants.INT_TYPE_NAME;
		}else
		if(type.equalsIgnoreCase("fixed64")){
			return Constants.BIGINT_TYPE_NAME;
		}else
		if(type.equalsIgnoreCase("sfixed32")){
			return Constants.INT_TYPE_NAME;
		}else
		if(type.equalsIgnoreCase("sfixed64")){
			return Constants.BIGINT_TYPE_NAME;
		}else
		if(type.equalsIgnoreCase("bool")){
			return Constants.BOOLEAN_TYPE_NAME;
		}else
		if(type.equalsIgnoreCase("string")){
			return Constants.STRING_TYPE_NAME;
		}else
//		if(type.equalsIgnoreCase("bytes")){//[TODO] bytes support
//			return Constants.STRING_TYPE_NAME;
//		}
//		else
		if(type.equalsIgnoreCase("float")){
			return Constants.FLOAT_TYPE_NAME;
		}
		else{
			LOG.error("not hive type for pb type : " + type );
			return null;
		}
	}
	
	private String getHiveTypeFromPBType(String modifier,String type){
		String rt = null;
		if(modifier.equalsIgnoreCase("required") || modifier.equalsIgnoreCase("optional")){
			rt = getHivePrimitiveTypeFromPBPrimitiveType(type);
		}
		else
		if(modifier.equalsIgnoreCase("repeated")){
			rt = Constants.LIST_TYPE_NAME + "<"
			+  getHivePrimitiveTypeFromPBPrimitiveType(type)
			+ ">";
		}
		return rt;
	}

	/**
	 * ast.getType is TOK_PARTITION or TOK_SUBPARTITION
	 * 
	 * @param ast
	 * @return
	 */
	private PartitionDesc getOneLevelPartitionDesc(ASTNode ast,List<FieldSchema> cols)
			throws SemanticException {
		PartitionDesc pardesc = new PartitionDesc();
		
		pardesc.setDbName(SessionState.get().getDbName());
		
		Boolean isRange = false;
		Boolean isDefault = false;
		if (ast.getChild(0).getChild(0).getText().equalsIgnoreCase("range")) {
			pardesc.setPartitionType(PartitionType.RANGE_PARTITION);
			isRange = true;
		} else if (ast.getChild(0).getChild(0).getText().equalsIgnoreCase("list")) {
			pardesc.setPartitionType(PartitionType.LIST_PARTITION);
		//modified by Brantzhang for hash partition Begin
		} else if (ast.getChild(0).getChild(0).getText().equalsIgnoreCase("hashkey")) {
			pardesc.setPartitionType(PartitionType.HASH_PARTITION);
		} else
			throw new SemanticException("the partition type do not support now!");
		//modified by Brantzhang for hash partition End 

		String partCol = unescapeIdentifier(ast.getChild(0).getChild(1).getText());
		for(int i = 0; i < cols.size();++i){
			if(cols.get(i).getName().equalsIgnoreCase(partCol)){//should ignore case!
				pardesc.setPartColumn(cols.get(i));
				break;
			}
		}
		if(pardesc.getPartColumn() == null)
			throw new SemanticException("partition column : " + partCol + " do not present in the table columns");
		
		//Added by Brantzhang for Hash partition Begin
		if (ast.getChild(0).getChild(0).getText().equalsIgnoreCase("hashkey")){
			LinkedHashMap<String, List<String>> partitionSpaces = 
				new LinkedHashMap<String, List<String>>();
			for(int i = 0; i < this.numOfHashPar; i++)
				if(i < 10){
					partitionSpaces.put("Hash_" + "000" + i, null);
				}else if(i < 100)
    				partitionSpaces.put("Hash_" + "00" + i, null);
    			else if(i < 1000)
    				partitionSpaces.put("Hash_" + "0" + i, null);
    			else partitionSpaces.put("Hash_" + i, null);
				
			pardesc.setPartitionSpaces(partitionSpaces);
			return pardesc;
		}
		//modified by Brantzhang for Hash partition End
		
// Disallowing Map, List and Struct as partition column, Taojiang 2010.11 
		String type = pardesc.getPartColumn().getType();
		if (type.equalsIgnoreCase(Constants.MAP_TYPE_NAME)
		 || type.equalsIgnoreCase(Constants.LIST_TYPE_NAME)
		 || type.equalsIgnoreCase(Constants.STRUCT_TYPE_NAME)) {
			throw new SemanticException("Partition column [" + pardesc.getPartColumn().getName()
				+ "] is not of a primitive type");
		}
		
		if(type.equalsIgnoreCase(Constants.BOOLEAN_TYPE_NAME)){
			throw new SemanticException("Partition column [" + pardesc.getPartColumn().getName()
					+ "] is boolean type ,TDW forbid partititon by boolean type");
		}
		
		PrimitiveTypeInfo pti = new PrimitiveTypeInfo();
		pti.setTypeName(type);
// END OF Disallowing...
		
		ObjectInspector StringIO = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
		ObjectInspector ValueIO = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(pti.getPrimitiveCategory());	
		ObjectInspectorConverters.Converter converter1 = ObjectInspectorConverters.getConverter(StringIO, ValueIO);
		ObjectInspectorConverters.Converter converter2 = ObjectInspectorConverters.getConverter(StringIO, ValueIO);
		
		LinkedHashMap<String, List<String>> partitionSpaces = new LinkedHashMap<String, List<String>>();
		int nmuCh = ast.getChild(1).getChildCount();
		String tmpPartDef;
		for (int i = 0; i < nmuCh; ++i) {	
			//for default partition
			if(((ASTNode)(ast.getChild(1).getChild(i)
					.getChild(0))).getToken().getType() == HiveParser.TOK_DEFAULTPARTITION){		
				if(!isDefault){
					isDefault = true;
				}
				else
					throw new SemanticException(
							"duplicate default partition for table"
									);
				continue;
			}
			
			List<String> partDefine = new ArrayList<String>();
			String partName = unescapeIdentifier(ast.getChild(1).getChild(i)
					.getChild(0).getText()).toLowerCase();
				

			int numDefine = ast.getChild(1).getChild(i).getChild(1)
					.getChildCount();
			ASTNode parDefine = (ASTNode) ast.getChild(1).getChild(i).getChild(
					1);
			if ((parDefine.getToken().getType() == HiveParser.TOK_RANGEPARTITIONDEFINE && isRange)
					|| (parDefine.getToken().getType() == HiveParser.TOK_LISTPARTITIONDEFINE && !isRange)) {

				for (int j = 0; j < numDefine; ++j) {
					if(((ASTNode)(ast.getChild(1).getChild(i).getChild(1).getChild(j))).getToken().getType() == HiveParser.CharSetLiteral){
						tmpPartDef = BaseSemanticAnalyzer.charSetString(ast.getChild(1).getChild(i).getChild(1).getChild(j).getChild(0).getText(), ast.getChild(1).getChild(i).getChild(1).getChild(j).getChild(1).getText());
					}
					else if (((ASTNode)(ast.getChild(1).getChild(i).getChild(1).getChild(j))).getToken().getType() == HiveParser.StringLiteral){
						tmpPartDef = unescapeSQLString(ast.getChild(1).getChild(
							i).getChild(1).getChild(j).getText());
					}
					else{
						tmpPartDef = ast.getChild(1).getChild(
								i).getChild(1).getChild(j).getText();
						
					}
					
					if(partDefine.contains(tmpPartDef))
						throw new SemanticException("in partition define: " + partName + " Value :" + tmpPartDef + " duplicated");
					
					if(converter1.convert(tmpPartDef) == null){
						throw new SemanticException("value : " + tmpPartDef +" should be the type of " +  pardesc.getPartColumn().getType());
					}
					
					partDefine.add(tmpPartDef);
				}
				if (partitionSpaces.containsKey(partName.toLowerCase())) {
					throw new SemanticException(
							"duplicate partition name for partition: "
									+ partName);
				} else
					partitionSpaces.put(partName.toLowerCase(), partDefine);
			}
			else
				throw new SemanticException("partition define should be same with partition type!");
		}
		
		
		
		List<List<String> > checkDefineList = new ArrayList<List<String> >();
		checkDefineList.addAll(partitionSpaces.values());
		if(pardesc.getPartitionType() == PartitionType.RANGE_PARTITION){//if range partition ,we check that the partitions have less order
			for(int i = 0;i + 1 < checkDefineList.size();++i){
				Object o1 = converter1.convert(checkDefineList.get(i).get(0));
				Object o2 = converter2.convert(checkDefineList.get(i+1).get(0));
				
				if(((Comparable)o1).compareTo((Comparable)o2) >= 0) {
					LOG.error(o1.getClass() + " : " + o1 + ", " + o2.getClass() + ":" + o2);
					throw new SemanticException("range less than value: " + checkDefineList.get(i).get(0) + " should smaller than: " + checkDefineList.get(i+1).get(0));
				}
			}
		}
		
		if(pardesc.getPartitionType() == PartitionType.LIST_PARTITION){//if list partition,we check that no define values in mult partition
			for(int i = 0;i < checkDefineList.size() - 1;++i){
				for(int j = i + 1;j < checkDefineList.size();++j){
					for(int current = 0; current < checkDefineList.get(i).size();++current){
						if(checkDefineList.get(j).contains(checkDefineList.get(i).get(current)))
							throw new SemanticException("value: " + checkDefineList.get(i).get(current) + " found in mult List partition define!");
					}
				}
			}
		}
		
		if(isDefault){
			partitionSpaces.put("default", new ArrayList<String>());
		}
		if(partitionSpaces.size() > 65536){
			throw new SemanticException("TDW do not support partititon spaces more than 65536!");
		}
		pardesc.setPartitionSpaces(partitionSpaces);
		return pardesc;

	}

	private PartitionDesc getPartDesc(ASTNode ast,List<FieldSchema> cols) throws SemanticException {
		try {
			PartitionDesc partDesc = getOneLevelPartitionDesc((ASTNode) ast
					.getChild(0),cols);

			if (ast.getChildCount() == 2) {
				partDesc.setSubPartition(getOneLevelPartitionDesc((ASTNode) ast
						.getChild(1),cols));
				
				//Added by Brantzhang for Hash map join Begin
				if(partDesc.getPartitionType().equals(PartitionType.HASH_PARTITION) &&
						partDesc.getSubPartition().getPartitionType().equals(PartitionType.HASH_PARTITION))
					throw new SemanticException("The two partitions should not be hash type simultaneously!");
				else if(partDesc.getPartitionType().equals(PartitionType.HASH_PARTITION) &&
						!partDesc.getSubPartition().getPartitionType().equals(PartitionType.HASH_PARTITION))
					throw new SemanticException("Only the second partition can be hash type!");
				//Added by Brantzhang for Hash map join End
				
			}
			return partDesc;
		} catch (SemanticException e) {
			throw e;

		}

	}

	private List<FieldSchema> getColumns(ASTNode ast) {
		List<FieldSchema> colList = new ArrayList<FieldSchema>();
		int numCh = ast.getChildCount();
		for (int i = 0; i < numCh; i++) {
			FieldSchema col = new FieldSchema();
			ASTNode child = (ASTNode) ast.getChild(i);
			col.setName(unescapeIdentifier(child.getChild(0).getText()));
			ASTNode typeChild = (ASTNode) (child.getChild(1));
			col.setType(getTypeStringFromAST(typeChild));

			if (child.getChildCount() == 3)
				col.setComment(unescapeSQLString(child.getChild(2).getText()));
			colList.add(col);
		}
		return colList;
	}

	private List<String> getColumnNames(ASTNode ast) {
		List<String> colList = new ArrayList<String>();
		int numCh = ast.getChildCount();
		for (int i = 0; i < numCh; i++) {
			ASTNode child = (ASTNode) ast.getChild(i);
			colList.add(unescapeIdentifier(child.getText()));
		}
		return colList;
	}

	private List<Order> getColumnNamesOrder(ASTNode ast) {
		List<Order> colList = new ArrayList<Order>();
		int numCh = ast.getChildCount();
		for (int i = 0; i < numCh; i++) {
			ASTNode child = (ASTNode) ast.getChild(i);
			if (child.getToken().getType() == HiveParser.TOK_TABSORTCOLNAMEASC)
				colList.add(new Order(unescapeIdentifier(child.getChild(0)
						.getText()), 1));
			else
				colList.add(new Order(unescapeIdentifier(child.getChild(0)
						.getText()), 0));
		}
		return colList;
	}

	/**
	 * Get the fully qualified name in the ast. e.g. the ast of the form ^(DOT
	 * ^(DOT a b) c) will generate a name of the form a.b.c
	 * 
	 * @param ast
	 *            The AST from which the qualified name has to be extracted
	 * @return String
	 */
	private String getFullyQualifiedName(ASTNode ast) {
		if (ast.getChildCount() == 0) {
			return ast.getText();
		}

		return getFullyQualifiedName((ASTNode) ast.getChild(0)) + "."
				+ getFullyQualifiedName((ASTNode) ast.getChild(1));
	}

	/**
	 * Create a FetchTask for a given table and thrift ddl schema
	 * 
	 * @param tablename
	 *            tablename
	 * @param schema
	 *            thrift ddl
	 */
	private Task<? extends Serializable> createFetchTask(String schema) {
		Properties prop = new Properties();

		prop.setProperty(Constants.SERIALIZATION_FORMAT, "9");
		prop.setProperty(Constants.SERIALIZATION_NULL_FORMAT, " ");
		String[] colTypes = schema.split("#");
		prop.setProperty("columns", colTypes[0]);
		prop.setProperty("columns.types", colTypes[1]);
		LOG.debug(ctx.getResFile());
		fetchWork fetch = new fetchWork(ctx.getResFile().toString(),
				new tableDesc(LazySimpleSerDe.class, TextInputFormat.class,
						IgnoreKeyTextOutputFormat.class, prop), -1);
		fetch.setSerializationNullFormat(" ");
		return TaskFactory.get(fetch, this.conf);
	}

	private void analyzeDescribeTable(ASTNode ast) throws SemanticException {
		ASTNode tableTypeExpr = (ASTNode) ast.getChild(0);
		String tableName = getFullyQualifiedName((ASTNode) tableTypeExpr
				.getChild(0));

		String partSpec = null;
		// get partition metadata if partition specified
		if (tableTypeExpr.getChildCount() == 2) {//[TODO]//we now only support ext to print all partition msg
			/*ASTNode partspec = (ASTNode) tableTypeExpr.getChild(1);
			partSpec = new LinkedHashMap<String, String>();
			for (int i = 0; i < partspec.getChildCount(); ++i) {
				ASTNode partspec_val = (ASTNode) partspec.getChild(i);
				String val = stripQuotes(partspec_val.getChild(1).getText());
				partSpec.put(partspec_val.getChild(0).getText().toLowerCase(),
						val);
			}*/
		}

		boolean isExt = ast.getChildCount() > 1;
		descTableDesc descTblDesc = new descTableDesc(ctx.getResFile(),
				tableName, partSpec, isExt);
		rootTasks.add(TaskFactory.get(new DDLWork(descTblDesc), conf));
		setFetchTask(createFetchTask(descTblDesc.getSchema()));
		LOG.info("analyzeDescribeTable done");
	}

	private void analyzeShowPartitions(ASTNode ast) throws SemanticException {
		showPartitionsDesc showPartsDesc;
		String tableName = unescapeIdentifier(ast.getChild(0).getText());
		showPartsDesc = new showPartitionsDesc(tableName, ctx.getResFile());
		rootTasks.add(TaskFactory.get(new DDLWork(showPartsDesc), conf));
		setFetchTask(createFetchTask(showPartsDesc.getSchema()));
	}
	
	private void analyzeAddDefaultPartition(ASTNode ast) throws SemanticException{
		
		String tableName = unescapeIdentifier(ast.getChild(0).getText());
		boolean isSub = (((ASTNode)(ast.getChild(1))).getToken().getType() == HiveParser.TOK_DEFAULTSUBPARTITION ? true:false);
		
		addDefaultPartitionDesc adpd= new addDefaultPartitionDesc(SessionState.get().getDbName(), tableName, isSub);
		
		rootTasks.add(TaskFactory.get(new DDLWork(adpd), conf));
	}

	private void analyzeShowTables(ASTNode ast) throws SemanticException {
		showTablesDesc showTblsDesc;
		if (ast.getChildCount() == 1) {
			String tableNames = unescapeSQLString(ast.getChild(0).getText());
			showTblsDesc = new showTablesDesc(ctx.getResFile(), tableNames);
		} else {
			showTblsDesc = new showTablesDesc(ctx.getResFile());
		}
		rootTasks.add(TaskFactory.get(new DDLWork(showTblsDesc), conf));
		setFetchTask(createFetchTask(showTblsDesc.getSchema()));
	}

	/**
	 * Add the task according to the parsed command tree. This is used for the
	 * CLI command "SHOW FUNCTIONS;".
	 * 
	 * @param ast
	 *            The parsed command tree.
	 * @throws SemanticException
	 *             Parsin failed
	 */
	private void analyzeShowFunctions(ASTNode ast) throws SemanticException {
		showFunctionsDesc showFuncsDesc;
		if (ast.getChildCount() == 1) {
			String funcNames = unescapeSQLString(ast.getChild(0).getText());
			showFuncsDesc = new showFunctionsDesc(ctx.getResFile(), funcNames);
		} else {
			showFuncsDesc = new showFunctionsDesc(ctx.getResFile());
		}
		rootTasks.add(TaskFactory.get(new DDLWork(showFuncsDesc), conf));
		setFetchTask(createFetchTask(showFuncsDesc.getSchema()));
	}

	/**
	 * Add the task according to the parsed command tree. This is used for the
	 * CLI command "DESCRIBE FUNCTION;".
	 * 
	 * @param ast
	 *            The parsed command tree.
	 * @throws SemanticException
	 *             Parsing failed
	 */
	private void analyzeDescFunction(ASTNode ast) throws SemanticException {
		String funcName;
		boolean isExtended;

		if (ast.getChildCount() == 1) {
			funcName = ast.getChild(0).getText();
			isExtended = false;
		} else if (ast.getChildCount() == 2) {
			funcName = ast.getChild(0).getText();
			isExtended = true;
		} else {
			throw new SemanticException(
					"Unexpected Tokens at DESCRIBE FUNCTION");
		}

		descFunctionDesc descFuncDesc = new descFunctionDesc(ctx.getResFile(),
				funcName, isExtended);
		rootTasks.add(TaskFactory.get(new DDLWork(descFuncDesc), conf));
		setFetchTask(createFetchTask(descFuncDesc.getSchema()));
	}

	private void analyzeAlterTableRename(ASTNode ast) throws SemanticException {
		alterTableDesc alterTblDesc = new alterTableDesc(unescapeIdentifier(ast
				.getChild(0).getText()), unescapeIdentifier(ast.getChild(1)
				.getText()));
		rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
	}

	private void analyzeAlterTableModifyCols(ASTNode ast,
			alterTableTypes alterType) throws SemanticException {
		String tblName = unescapeIdentifier(ast.getChild(0).getText());
		List<FieldSchema> newCols = getColumns((ASTNode) ast.getChild(1));
		alterTableDesc alterTblDesc = new alterTableDesc(tblName, newCols,
				alterType);
		rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
	}

	private void analyzeAlterTableDropParts(ASTNode ast)
			throws SemanticException {
		String tblName = unescapeIdentifier(ast.getChild(0).getText());
		// get table metadata
		
		ArrayList<String> partNames = new ArrayList<String>();
		Boolean isSub = ((ASTNode)ast.getChild(1)).getToken().getType() == HiveParser.TOK_SUBPARTITIONREF ? true : false;
		partNames.add(unescapeIdentifier(ast.getChild(1).getChild(0).getText()).toLowerCase());
		for(int i = 2;i < ast.getChildCount();++i){
			if( ((ASTNode)ast.getChild(i - 1)).getToken().getType() !=  ((ASTNode)ast.getChild(i)).getToken().getType())
				throw new SemanticException("only support one level partition drop once,do not inclde pri and sub partition in one statement!");
			String partName = unescapeIdentifier(ast.getChild(i).getChild(0).getText());
			if(partNames.contains(partName.toLowerCase())){
				throw new SemanticException("find duplicate partition names: " + partName);
			}
			partNames.add(partName.toLowerCase());
		}
		
		
		
		dropTableDesc dropTblDesc = new dropTableDesc(SessionState.get().getDbName(),tblName,isSub,partNames,true);
		rootTasks.add(TaskFactory.get(new DDLWork(dropTblDesc), conf));
	}

	
	/**
	 * add by allison
	 * get the partition define value,e.g: values less than ("xyz"),return the xyz string
	 * if we support more type,date or time for example,just change this func 
	 * @param partdef
	 * @return
	 */
	private static String getPartitionDefineValue(ASTNode partdef) throws SemanticException{
		String tmpPartDef = null;
		
		if(partdef.getToken().getType() == HiveParser.CharSetLiteral){
			tmpPartDef = BaseSemanticAnalyzer.charSetString(partdef.getChild(0).getText(), partdef.getChild(1).getText());
		}
		else if (partdef.getToken().getType() == HiveParser.StringLiteral){
		tmpPartDef = unescapeSQLString(partdef.getText());
		}
		else{
			tmpPartDef = partdef.getText();
			
		}
		
		return tmpPartDef;
	}
	/**
	 * Add one or more partitions to a table. Useful when the data has been
	 * copied to the right location by some other process.
	 * 
	 * @param ast
	 *            The parsed command tree.
	 * @throws SemanticException
	 *             Parsin failed
	 */
	private void analyzeAlterTableAddParts(CommonTree ast,Boolean isSubPartition)
			throws SemanticException {
		

		String tblName = unescapeIdentifier(ast.getChild(0).getText());
		
		PartitionDesc partDesc = new PartitionDesc();
		
		LinkedHashMap< String,List< String > > partSpace= new LinkedHashMap< String,List< String > >();
		
		String parName = unescapeIdentifier(ast.getChild(1).getChild(0).getText());
		List<String> partDef = new ArrayList<String>();
		
		Boolean isRange = false;
		ASTNode partDefTree = (ASTNode)ast.getChild(1).getChild(1);
		
		if(partDefTree.getToken().getType() ==  HiveParser.TOK_LISTPARTITIONDEFINE){
			LOG.info("add to list partition");
			partDesc.setPartitionType(PartitionType.LIST_PARTITION);
			for(int i = 0;i < partDefTree.getChildCount();++i)
				partDef.add(getPartitionDefineValue((ASTNode)(partDefTree.getChild(i))));
		}
		else if(partDefTree.getToken().getType() ==  HiveParser.TOK_RANGEPARTITIONDEFINE)
		{
			LOG.info("add to range partition");
			isRange = true;
			partDesc.setPartitionType(PartitionType.RANGE_PARTITION);
			partDef.add(getPartitionDefineValue((ASTNode)(partDefTree.getChild(0))));
			
		}
		else
			throw new SemanticException("add unknow partition type!");
		
		partSpace.put(parName.toLowerCase(), partDef);
		
		if(ast.getChildCount() > 2 && isRange){
			for(int i = 2;i < ast.getChildCount();++i){
				partDef = new ArrayList<String>();
				partDef.add(getPartitionDefineValue((ASTNode)(ast.getChild(i).getChild(1).getChild(0))));
				if(partSpace.containsKey(unescapeIdentifier(ast.getChild(i).getChild(0).getText()).toLowerCase())){
					throw new SemanticException("duplicate partition name : "  + unescapeIdentifier(ast.getChild(i).getChild(0).getText()));
				}
				partSpace.put(unescapeIdentifier(ast.getChild(i).getChild(0).getText()).toLowerCase(),partDef);
			}
		
		}else if(ast.getChildCount() > 2 && !isRange){
			for(int i = 2;i < ast.getChildCount();++i){
				partDef = new ArrayList<String>();
				partDefTree = (ASTNode)ast.getChild(i).getChild(1);
				for(int j = 0;j < partDefTree.getChildCount();++j){
					partDef.add(getPartitionDefineValue((ASTNode)(partDefTree.getChild(j))));
				}
				if(partSpace.containsKey(unescapeIdentifier(ast.getChild(i).getChild(0).getText()).toLowerCase())){
					throw new SemanticException("duplicate partition name : " + unescapeIdentifier(ast.getChild(i).getChild(0).getText()));
				}
				partSpace.put(unescapeIdentifier(ast.getChild(i).getChild(0).getText()).toLowerCase(),partDef);
			}
			
		}
		
		
		partDesc.setPartitionSpaces(partSpace);
		if (true) {
			AddPartitionDesc addPartitionDesc = new AddPartitionDesc(SessionState.get().getDbName(),
					tblName,partDesc,isSubPartition);
			rootTasks.add(TaskFactory.get(new DDLWork(addPartitionDesc), conf));
		}
		
	}

	/**
	 * Verify that the information in the metastore matches up with the data on
	 * the fs.
	 * 
	 * @param ast
	 *            Query tree.
	 * @throws SemanticException
	 */
	private void analyzeMetastoreCheck(CommonTree ast) throws SemanticException {
		String tableName = null;
		
		QB.tableRef trf = null;
		
		if (ast.getChildCount() >= 1) {
			tableName = unescapeIdentifier(ast.getChild(0).getText());
		
			if(ast.getChildCount() == 2){
				
				String pri = null;
				String sub = null;
				if(((ASTNode)(ast.getChild(1))).getToken().getType() == HiveParser.TOK_PARTITIONREF)
					pri = unescapeIdentifier(ast.getChild(1).getChild(0).getText());
				else
					sub = unescapeIdentifier(ast.getChild(1).getChild(0).getText());
				//TODO:does msck can only check the session db?
				trf = new QB.tableRef(SessionState.get().getDbName(),tableName, null, pri, sub);
			}
		}
		
		//List<Map<String, String>> specs = getPartitionSpecs(ast);
		MsckDesc checkDesc = new MsckDesc(tableName, trf, ctx.getResFile());
		rootTasks.add(TaskFactory.get(new DDLWork(checkDesc), conf));
	}

	/**
	 * Get the partition specs from the tree.
	 * 
	 * @param ast
	 *            Tree to extract partitions from.
	 * @return A list of partition name to value mappings.
	 * @throws SemanticException
	 */
	private List<Map<String, String>> getPartitionSpecs(CommonTree ast)
			throws SemanticException {
		List<Map<String, String>> partSpecs = new ArrayList<Map<String, String>>();
		int childIndex = 0;
		// get partition metadata if partition specified
		/*for (childIndex = 1; childIndex < ast.getChildCount(); childIndex++) {
			Tree partspec = ast.getChild(childIndex);
			// sanity check
			if (partspec.getType() == HiveParser.TOK_PARTSPEC) {
				Map<String, String> partSpec = new LinkedHashMap<String, String>();
				for (int i = 0; i < partspec.getChildCount(); ++i) {
					CommonTree partspec_val = (CommonTree) partspec.getChild(i);
					String val = stripQuotes(partspec_val.getChild(1).getText());
					partSpec.put(partspec_val.getChild(0).getText()
							.toLowerCase(), val);
				}
				partSpecs.add(partSpec);
			}
		}*/
		return partSpecs;
	}
	
	private void analyzeCreateDatabase(ASTNode ast){
		createDatabaseDesc cdd = new createDatabaseDesc(unescapeIdentifier(ast.getChild(0).getText()));
		rootTasks.add(TaskFactory.get(new DDLWork(cdd), conf));
	}
	private void analyzeDropDatabase(ASTNode ast){
		dropDatabaseDesc ddd = new dropDatabaseDesc(unescapeIdentifier(ast.getChild(0).getText()));
		rootTasks.add(TaskFactory.get(new DDLWork(ddd), conf));
	}
	
	private void analyzeUseDatabase(ASTNode ast){
		useDatabaseDesc udd= new useDatabaseDesc(unescapeIdentifier(ast.getChild(0).getText()));
		rootTasks.add(TaskFactory.get(new DDLWork(udd), conf));
	}
	
	
	

        	//allison:convert pb msg define to a table columns
	private List<FieldSchema> getColsFromPBDef(String msgName,String path) throws SemanticException{
		ANTLRReaderStream rs = null;
		try{
			LOG.info("will parse pb file : " + path);
		 rs = new ANTLRReaderStream(new FileReader(path));
		 
		}
		catch(Exception e){
			throw new SemanticException("protobuf define file : " + path + " read error: " + e.getMessage());
		}
		
		LOG.info( "file content : " + rs.toString());
		
		protobufLexer lexer = new protobufLexer(rs);
		
		
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		
		System.out.println("token string : " + tokens.toString());
		
		protobufParser parser = new protobufParser(tokens);

		protobufParser.proto_return r = null;
		
		try{
		r = parser.proto();
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}
		
		CommonTree ct = (CommonTree)r.getTree();
		
		LOG.info("pb parser tree : " + ct.toStringTree());
		
//		 while ((ct.getToken() == null) && (ct.getChildCount() > 0)) {
//		        ct = (CommonTree) ct.getChild(0);
//		      }
		
		 //do some check
		 
		 CommonTree msg = null;
		 boolean java_package = false;
		 boolean outer_class = false;
		 
		for(int j = 0;j < ct.getChildCount();++j){
			//LOG.info("pb node ct.getChild(j).getText() : " + ct.getChild(j).getText());
			if(ct.getChild(j).getText().equals("TOK_IMPORT")){
				throw new SemanticException("TDW do not support pb import now !");
			}
			else 
			if (ct.getChild(j).getText().equals("TOK_OPTION")){
				
				if(ct.getChild(j).getChild(0).getChild(0).getText().equals("java_package")){
					//LOG.info("optin key : " + ct.getChild(j).getChild(0).getChild(0).getText());
					//LOG.info("optin value : " + unescapeSQLString(ct.getChild(j).getChild(0).getChild(1).getText()));
					if(! unescapeSQLString(ct.getChild(j).getChild(0).getChild(1).getText()).equals("tdw"))
						throw new SemanticException("must define the pb msg in tdw package!");
					else
						java_package = true;
				}	
				else if(ct.getChild(j).getChild(0).getChild(0).getText().equals("java_outer_classname")){
					//LOG.info("optin key : " + ct.getChild(j).getChild(0).getChild(0).getText());
					//LOG.info("optin value : " + unescapeSQLString(ct.getChild(j).getChild(0).getChild(1).getText()));
					if(! unescapeSQLString(ct.getChild(j).getChild(0).getChild(1).getText()).equals(SessionState.get().getDbName()+ "_" + msgName))
						throw new SemanticException("the msg class' outer class name must be 'databaseName_msgName' !");
					outer_class= true;
				}
			}else
			if(ct.getChild(j).getText().equals("TOK_PACKAGE")){
					;//do nothing
				}
			else if(ct.getChild(j).getText().equals("TOK_MSG")){
				if(msg != null)
					throw new SemanticException("TDW do not support muti msg define in a file!");
				
				msg = (CommonTree)(ct.getChild(j));
			}
			else
				throw new SemanticException("do not support this protobuf functiong : " + ct.getChild(j).getLine());
			
			
		}
		 
		 
		 if(!java_package){
			 throw new SemanticException("pb define file should have a java_package=tdw option!");
		 }
		 if(!outer_class)
			 throw new SemanticException("pb define file should have a java_outer_classname=<databaseName_msgName> option!");
		 
		 
		 pb_msg_outer_name= SessionState.get().getDbName()+ "_" + msgName;
		 
		 //LOG.info("pb_msg_outer_name : " + pb_msg_outer_name);
		 
		 
		 
		//LOG.info("msg tree : " + ct.toStringTree());
		
		//LOG.info("msg child cunt: " + msg.getChildCount());
		
		//LOG.info("msg name : " + msgName + ":" + msg.getChild(0).getText());
		if(msg.getChild(0).getText().compareToIgnoreCase(msgName) != 0){
			throw new SemanticException("table name do not match the pb message name!");
		}
		
		CommonTree msgBody = (CommonTree)msg.getChild(1);
		
		List<FieldSchema> colList = new ArrayList<FieldSchema>();
		
		int numCh = msgBody.getChildCount();
		for (int i = 0; i < numCh; i++) {
			FieldSchema col = new FieldSchema();
			CommonTree child = (CommonTree) msgBody.getChild(i);
			col.setName(unescapeIdentifier(child.getChild(2).getText()));
			col.setType(getHiveTypeFromPBType(child.getChild(0).getText(), child.getChild(1).getText()));
			colList.add(col);
		}
		return colList;
		
	}
	//end
	private void analyzeShowDatabases(ASTNode ast){
		showDBDesc showDbDesc = new showDBDesc(ctx.getResFile());
		LOG.debug("resFile : " + ctx.getResFile());
		rootTasks.add(TaskFactory.get(new DDLWork(showDbDesc), conf));
		setFetchTask(createFetchTask(showDbDesc.getSchema()));
	}
	
	
	private void analyzeShowTableIndex(ASTNode ast)
    {
	    String tblName = ast.getChild(0).getText();
	    showIndexDesc showIndexsDesc = new showIndexDesc("", tblName, showIndexTypes.SHOWTABLEINDEX);
	    
        rootTasks.add(TaskFactory.get(new DDLWork(showIndexsDesc), conf));
    }

    private void analyzeShowAllIndex(ASTNode ast)
    {        
        showIndexDesc showIndexsDesc = new showIndexDesc("", "", showIndexTypes.SHOWALLINDEX);

        rootTasks.add(TaskFactory.get(new DDLWork(showIndexsDesc), conf));
    }
	

}


