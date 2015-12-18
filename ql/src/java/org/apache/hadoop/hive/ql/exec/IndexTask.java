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

import java.io.DataOutput;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.IndexQueryInfo;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.IndexValue;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.indexWork;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;

import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;

import Comm.ConstVar;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;
import StorageEngineClient.FormatStorageSerDe;
import IndexService.Indexer;

/**
 * FetchTask implementation
 **/
public class IndexTask extends Task<indexWork> implements Serializable
{
    private static final long serialVersionUID = 1L;

    private int maxRows = 100;
   // private FetchOperator ftOp;
    private LazySimpleSerDe mSerde;
    private FormatStorageSerDe formatSerde;
    private InputFormat inputFormat;
    private IgnoreKeyTextOutputFormat outputFormat;
    private int totalRows;
    private JobConf job;
    
    static final private int separator  = Utilities.ctrlaCode;
    static final private int terminator = Utilities.newLineCode;    
    static final private int MAX_OUTPUT_RECORD = 10000;
    
    public void initialize(HiveConf conf)
    {
        super.initialize(conf);

        try
        {
            // Create a file system handle
            job = new JobConf(conf, ExecDriver.class);
            
            mSerde = new LazySimpleSerDe();
            Properties mSerdeProp = new Properties();
            mSerdeProp.put(Constants.SERIALIZATION_FORMAT, ""
                            + Utilities.tabCode);
            mSerdeProp.put(Constants.SERIALIZATION_NULL_FORMAT,
                            ((indexWork) work).getSerializationNullFormat());
            mSerde.initialize(job, mSerdeProp);
            mSerde.setUseJSONSerialize(true);
            
            formatSerde = new FormatStorageSerDe();
            formatSerde.initialize(job, work.getProperties());
            
            inputFormat = new TextInputFormat();
            outputFormat = new IgnoreKeyTextOutputFormat();

            //ftOp = new FetchOperator(work, job);
        }
        catch (Exception e)
        {
            // Bail out ungracefully - we should never hit
            // this here - but would have hit it in SemanticAnalyzer
            LOG.error(StringUtils.stringifyException(e));
            throw new RuntimeException(e);
        }
    }

    // 执行通过index查询记录,并将记录写在res文件中.
    public int execute()
    {
        try
        {
            IndexQueryInfo indexQueryInfo = work.getIndexQueryInfo(); 
          
            
            List<Record> resultRecord = queryRecordByIndex(indexQueryInfo);
            if(resultRecord == null)
            {
                System.out.println("0 result return");
                return 0;
            }
            
            //System.out.println("result.size:"+resultRecord.size()+",res:"+work.getResFile().toString());
            
            DataOutput outStream = work.getResFile().getFileSystem(conf).create(work.getResFile());
            //DataOutput outStream = (DataOutput) fs.create(showParts.getResFile());
              
            //outStream.writeBytes("1"); outStream.write(separator); outStream.writeBytes("2");
           // outStream.write(terminator);
            
            for(int i = 0; i < resultRecord.size(); i++)
            {
                Record record = resultRecord.get(i);
                
                try
                {
                   // System.out.println("in output, i :"+i);
                    outputResult(record, outStream);
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                    System.out.println("output record " + i + ", fail:"+e.getMessage());
                }
                //record.show();
            }
            ((FSDataOutputStream)outStream).close();             
          
            return (0);
        }
        catch (Exception e)
        {
            console.printError("Failed with exception " + e.getMessage(), "\n"
                            + StringUtils.stringifyException(e));
            return (1);
        }
    }
    
    private void outputResult(Record record, DataOutput out) throws Exception
    {   
        String selectFieldString = work.getIndexQueryInfo().fieldList; 
       // System.out.println("selectFieldString:"+selectFieldString+", fieldValues.size:"+record.fieldValues().size());
        if(selectFieldString == null || selectFieldString.length() == 0) // 缺省全量字段.
        {
            int num = record.fieldNum();
            for(int i = 0; i < num; i++)
            {
                if(i == 0)
                {
                    out.writeBytes(record.fieldValues().get(i).toString());
                }
                else
                {
                    out.write(separator);
                                 
                    out.writeBytes(record.fieldValues().get(i).toString());             
                }
            }                   
            out.write(terminator);      
        }
        
        else
        {
            String[] seletcFields = selectFieldString.split(",");
            for(int i = 0; i < seletcFields.length; i++)
            {                
                int idx = Integer.valueOf(seletcFields[i]); 
                if(i == 0)
                {
                    out.writeBytes(record.fieldValues().get(idx).toString());
                }
                else
                {
                    out.write(separator);
                                 
                    out.writeBytes(record.fieldValues().get(idx).toString());             
                }
            }            
            out.write(terminator); 
        }  
        
    }
    
    private List<Record> queryRecordByIndex(IndexQueryInfo indexQueryInfo)
    {                
        String dbName = indexQueryInfo.dbName;
        String tblName = indexQueryInfo.tblName;
        String indexName = indexQueryInfo.indexName;
        String location = indexQueryInfo.location;
        String selectList = indexQueryInfo.fieldList;
        List<String> partList = indexQueryInfo.partList;
        List<IndexValue>  values = indexQueryInfo.values;
        int limitNum = indexQueryInfo.limit;
        int fieldNum = indexQueryInfo.fieldNum;
        
        try
        {
            if(values.isEmpty())
            {
                return null;
            }
            
            int size = values.size();            
            ArrayList<FieldValue> fieldValues = new ArrayList<FieldValue>(size);
            for(int i = 0; i < values.size(); i++)
            {
                fieldValues.add(IndexValue2FieldValue(values.get(i)));
            }             
           
            Indexer index = new Indexer(location, partList, fieldValues, selectList, fieldNum);              
            
            // 
            List<Record> result = new ArrayList<Record>();
            int rnum = index.recordsnum(location, partList, fieldValues); 
            if(limitNum < 0) 
            {
                limitNum = rnum;
            }
            
            if(limitNum > MAX_OUTPUT_RECORD)
            {
                System.out.println("Result num:"+limitNum+", execeed MAX OUTPUT RECORD:"+MAX_OUTPUT_RECORD+". set limit less than 10000");
                return null;
            }
            
            //考虑到大量记录返回时候的随机读性能, 最大返回1w条记录.
           // if(rnum > 10000) // next方式获取
            {
                
                /*
                System.out.println("get next");
                int count = 0;
                while(index.hasNext())
                {                    
                    if(++count > limitNum)
                    {
                        break;
                    }
                    
                    result.add(index.next());
                    
                    System.out.println("get next, count:"+count);
                } 
                */               
            }           
            //else // 获取全部
            {
                //System.out.println("get all");
                result = index.get(location, partList, fieldValues, limitNum, selectList, fieldNum); 
            } 
                     
            return result;
        }
        catch(Exception e)
        {
            e.printStackTrace();            
        }
        return null;
    }

    private FieldValue IndexValue2FieldValue(IndexValue indexValue)
    {
        FieldValue fieldValue = null;
        String type = indexValue.type;        
        
        if(type.equalsIgnoreCase(Constants.TINYINT_TYPE_NAME))
        {
            fieldValue = new FieldValue(((Byte)indexValue.value).byteValue(), (short) -1);
        }
        else if(type.equalsIgnoreCase(Constants.SMALLINT_TYPE_NAME))
        {
            fieldValue = new FieldValue(((Short)indexValue.value).shortValue(), (short) -1);         
        }
        else if(type.equalsIgnoreCase(Constants.INT_TYPE_NAME))
        {
            fieldValue = new FieldValue(((Integer)indexValue.value).intValue(), (short) -1);         
        }
        else if(type.equalsIgnoreCase(Constants.BIGINT_TYPE_NAME))
        {
            fieldValue = new FieldValue(((Long)indexValue.value).longValue(), (short) -1);            
        }
        else if(type.equalsIgnoreCase(Constants.FLOAT_TYPE_NAME))
        {
            fieldValue = new FieldValue(((Float)indexValue.value).floatValue(), (short) -1);         
        }
        else if(type.equalsIgnoreCase(Constants.DOUBLE_TYPE_NAME))
        {
            fieldValue = new FieldValue(((Double)indexValue.value).doubleValue(), (short) -1);         
        }
        else if(type.equalsIgnoreCase(Constants.STRING_TYPE_NAME))
        {            
            fieldValue = new FieldValue((String)indexValue.value, (short) -1);          
        }
        else
        {
            return null;
        }        
        
        return fieldValue;
    }       
}
