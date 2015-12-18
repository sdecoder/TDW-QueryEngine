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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
//import java.io.IOException;  //Removed by Brantzhang for patch HIVE-963
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import java.util.Properties;  //By Brantzhang for patch HIVE-968
//import java.util.Random;      //By Brantzhang for patch HIVE-968

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.mapJoinDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
//import org.apache.hadoop.hive.ql.util.jdbm.RecordManager;            //By Brantzhang for patch HIVE-968
//import org.apache.hadoop.hive.ql.util.jdbm.RecordManagerFactory;     //By Brantzhang for patch HIVE-968
//import org.apache.hadoop.hive.ql.util.jdbm.RecordManagerOptions;     //By Brantzhang for patch HIVE-968
//import org.apache.hadoop.hive.ql.util.jdbm.htree.HTree;              //By Brantzhang for patch HIVE-968
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectKey;    //Added by Brantzhang for patch HIVE-963
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectValue;  //Added by Brantzhang for patch HIVE-963
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;      //Added by Brantzhang for patch HIVE-963
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;        //Added by Brantzhang for patch HIVE-963
import org.apache.hadoop.hive.ql.exec.persistence.SortedKeyValueList;

/**
 * Map side Join operator implementation.
 */
public class MapJoinOperator extends CommonJoinOperator<mapJoinDesc> implements Serializable {
  private static final long serialVersionUID = 1L;
  static final private Log LOG = LogFactory.getLog(MapJoinOperator.class.getName());

  /**
   * The expressions for join inputs's join keys.
   */
  transient protected Map<Byte, List<ExprNodeEvaluator>> joinKeys;//每个表对应的要参与连接计算的列，第一个表为ExprNodeFuncEvaluator类型，第二个表为ExprNodeColumnEvaluator类型
  /**
   * The ObjectInspectors for the join inputs's join keys.
   */
  transient protected Map<Byte, List<ObjectInspector>> joinKeysObjectInspectors;
  /**
   * The standard ObjectInspectors for the join inputs's join keys.
   */
  transient protected Map<Byte, List<ObjectInspector>> joinKeysStandardObjectInspectors;

  transient private int posBigTable;       // one of the tables that is not in memory
  transient int mapJoinRowsKey;            // rows for a given key
  
  //Removed by Brantzhang for patch HIVE-968 Begin
  //transient protected Map<Byte, HTree> mapJoinTables;
  //RecordManager  recman = null;             //Added by Brantzhang for patch HIVE-865
  //Removed by Brantzhang for patch HIVE-968 End
  transient protected Map<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>> mapJoinTables;  //Added by Brantzhang for patch HIVE-968
  
  transient protected Map<Byte, SortedKeyValueList<ArrayList<Object>, MapJoinObjectValue>> mapJoinLists; //Added by Brantzhang for Sorted map join
  
  transient private boolean isSortedMergeJoin = false; //Added by Brantzhang for sorted merge join
  transient private int rowBucketSize;     //Added by Brantzhang for sorted merge join
  transient private boolean firsttime = true; //Added by Brantzhang for sorted merge join
  
  public static class MapJoinObjectCtx {
    ObjectInspector standardOI;
    SerDe      serde;
    tableDesc tblDesc;   //Added by Brantzhang for patch HIVE-1158
    Configuration conf;  //Added by Brantzhang for patch HIVE-1158
    
    /**
     * @param standardOI
     * @param serde
     */
    public MapJoinObjectCtx(ObjectInspector standardOI, SerDe serde) {
      //Added by Brantzhang for patch HIVE-1158 Begin
      this(standardOI, serde, null, null);
    }
    	    
    public MapJoinObjectCtx(ObjectInspector standardOI, SerDe serde,
      tableDesc tblDesc, Configuration conf) {
      //Added by Brantzhang for patch HIVE-1158 End
      this.standardOI = standardOI;
      this.serde = serde;
      this.tblDesc = tblDesc;  //Added by Brantzhang for patch HIVE-1158
      this.conf = conf;        //Added by Brantzhang for patch HIVE-1158
    }
    
    /**
     * @return the standardOI
     */
    public ObjectInspector getStandardOI() {
      return standardOI;
    }

    /**
     * @return the serde
     */
    public SerDe getSerDe() {
      return serde;
    }
    
    //Added by Brantzhang for patch HIVE-1158 Begin
    public tableDesc getTblDesc() {
      return tblDesc;
    }
    	
    public Configuration getConf() {
      return conf;
    }
    //Added by Brantzhang for patch HIVE-1158 End

  }

  transient static Map<Integer, MapJoinObjectCtx> mapMetadata = new HashMap<Integer, MapJoinObjectCtx>();
  transient static int nextVal = 0;
  
  static public Map<Integer, MapJoinObjectCtx> getMapMetadata() {
    return mapMetadata;
  }
  
  transient boolean firstRow;
  
  //transient int   metadataKeyTag; //Removed by Brantzhang for Sorted Merge Join
  public static int metadataKeyTag;        //Added by Brantzhang for Sorted Merge Join
  transient int[] metadataValueTag;
  transient List<File> hTables;
  transient int      numMapRowsRead;
  transient int      heartbeatInterval;
  
  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    numMapRowsRead = 0;
    
    this.isSortedMergeJoin = hconf.getBoolean("Sorted.Merge.Map.Join", false); //Added by Brantzhang for sorted merge join
  
    firstRow = true;
    //Removed by Brantzhang for patch HIVE-968 Begin
    /*
    try {
      heartbeatInterval = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVESENDHEARTBEAT);

      joinKeys  = new HashMap<Byte, List<ExprNodeEvaluator>>();
      
      populateJoinKeyValue(joinKeys, conf.getKeys());
      joinKeysObjectInspectors = getObjectInspectorsFromEvaluators(joinKeys, inputObjInspectors);
      joinKeysStandardObjectInspectors = getStandardObjectInspectors(joinKeysObjectInspectors); 
        
      // all other tables are small, and are cached in the hash table
      posBigTable = conf.getPosBigTable();//需要流式读的数据表
      
      metadataValueTag = new int[numAliases];
      for (int pos = 0; pos < numAliases; pos++)
        metadataValueTag[pos] = -1;
      
      mapJoinTables = new HashMap<Byte, HTree>();
      hTables = new ArrayList<File>();
      
      // initialize the hash tables for other tables为除流式表外的其他所有表创建hash表
      for (int pos = 0; pos < numAliases; pos++) {
        if (pos == posBigTable)//如果是流式表则跳过
          continue;
        
        Properties props = new Properties();
        props.setProperty(RecordManagerOptions.CACHE_SIZE, 
          String.valueOf(HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEMAPJOINCACHEROWS)));
        
        Random rand = new Random();//为表创建随机文件
        File newDir = new File("/tmp/" + rand.nextInt());
        String newDirName = null;
        while (true) {
          if (newDir.mkdir()) {
            newDirName = newDir.getAbsolutePath();
            hTables.add(newDir);
            break;
          }
          newDir = new File("/tmp" + rand.nextInt());
          */
        //Removed by Brantzhang for patch HIVE-968 End
        //Added by Brantzhang for patch HIVE-968 Begin
        heartbeatInterval = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVESENDHEARTBEAT);
        
        joinKeys  = new HashMap<Byte, List<ExprNodeEvaluator>>();
        
        populateJoinKeyValue(joinKeys, conf.getKeys());
        joinKeysObjectInspectors = getObjectInspectorsFromEvaluators(joinKeys, inputObjInspectors);
        joinKeysStandardObjectInspectors = getStandardObjectInspectors(joinKeysObjectInspectors);
        
        // all other tables are small, and are cached in the hash table
        posBigTable = conf.getPosBigTable();
        
        metadataValueTag = new int[numAliases];
        for (int pos = 0; pos < numAliases; pos++)
          metadataValueTag[pos] = -1;
        
        mapJoinTables = new HashMap<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>>();
        hTables = new ArrayList<File>();
        
        //Added by Brantzhang for sorted merge join Begin
        isSortedMergeJoin = hconf.getBoolean("Sorted.Merge.Map.Join", false);
        if(isSortedMergeJoin){
          mapJoinLists = new HashMap<Byte, SortedKeyValueList<ArrayList<Object>, MapJoinObjectValue>>(); 
         
        rowBucketSize = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEMAPJOINBUCKETCACHESIZE);   
          // initialize the sorted key/value lists for other tables
          for (int pos = 0; pos < numAliases; pos++) {
            if (pos == posBigTable)
              continue;
            
            int listSize = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEMAPJOINCACHEROWS);
            SortedKeyValueList<ArrayList<Object>, MapJoinObjectValue> kvList = 
            	new SortedKeyValueList<ArrayList<Object>, MapJoinObjectValue>(listSize);
            mapJoinLists.put(Byte.valueOf((byte)pos), kvList);            
          }
        }
        //Added by Brantzhang for sorted merge join End
        
        // initialize the hash tables for other tables
        for (int pos = 0; pos < numAliases; pos++) {
          if (pos == posBigTable)
            continue;
          
          int cacheSize = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEMAPJOINCACHEROWS);
          HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue> hashTable = 
            new HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>(cacheSize);
          
          mapJoinTables.put(Byte.valueOf((byte)pos), hashTable);
        }
        
        //storage.put((byte)posBigTable, new ArrayList<ArrayList<Object>>());  //Removed by Brantzhang for patch HIVE-963
        storage.put((byte)posBigTable, new RowContainer());               //Added by Brantzhang for patch HIVE-963
        
        mapJoinRowsKey = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEMAPJOINROWSIZE);
        
        List<? extends StructField> structFields = ((StructObjectInspector)outputObjInspector).getAllStructFieldRefs();
        if (conf.getOutputColumnNames().size() < structFields.size()) {
          List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
          for (Byte alias : order) {
            int sz = conf.getExprs().get(alias).size();
            List<Integer> retained = conf.getRetainList().get(alias);
            for (int i = 0; i < sz; i++) {
              int pos = retained.get(i);
              structFieldObjectInspectors.add(structFields.get(pos)
                                              .getFieldObjectInspector());
        //Added by Brantzhang for patch HIVE-968 End
        }
        
        //Removed by Brantzhang for patch HIVE-968 Begin
        /*
        //RecordManager recman = RecordManagerFactory.createRecordManager(newDirName + "/" + pos, props );  //Removed by Brantzhang for patch HIVE-865
        //Added by Brantzhang for patch HIVE-865 Begin
        // we don't need transaction since atomicity is handled at the higher level
        props.setProperty(RecordManagerOptions.DISABLE_TRANSACTIONS, "true" );
        recman = RecordManagerFactory.createRecordManager(newDirName + "/" + pos, props );
        //Added by Brantzhang for patch HIVE-865 End
        HTree hashTable = HTree.createInstance(recman);//创建hash表
        
        mapJoinTables.put(Byte.valueOf((byte)pos), hashTable);
        */
        //Removed by Brantzhang for patch HIVE-968 End
      }
      //Removed by Brantzhang for patch HIVE-968 Begin
      /*
      storage.put((byte)posBigTable, new ArrayList<ArrayList<Object>>());
      
      mapJoinRowsKey = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEMAPJOINROWSIZE);
      
      List<? extends StructField> structFields = ((StructObjectInspector)outputObjInspector).getAllStructFieldRefs();
      if (conf.getOutputColumnNames().size() < structFields.size()) {
        List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
        for (Byte alias : order) {
          int sz = conf.getExprs().get(alias).size();
          List<Integer> retained = conf.getRetainList().get(alias);
          for (int i = 0; i < sz; i++) {
            int pos = retained.get(i);
            structFieldObjectInspectors.add(structFields.get(pos)
                .getFieldObjectInspector());
          }
        }
        outputObjInspector = ObjectInspectorFactory
            .getStandardStructObjectInspector(conf.getOutputColumnNames(),
                structFieldObjectInspectors);
      }
      initializeChildren(hconf);
    } catch (IOException e) {
      throw new HiveException(e);
      */
      //Removed by Brantzhang for patch HIVE-968 End
      //Added by Brantzhang for patch HIVE-968 Begin      
      outputObjInspector = ObjectInspectorFactory
            .getStandardStructObjectInspector(conf.getOutputColumnNames(),
                                             structFieldObjectInspectors);
      //Added by Brantzhang for patch HIVE-968 End
    }
    initializeChildren(hconf);      //Added by Brantzhang for patch HIVE-968
  }
  
  @Override
  public void process(Object row, int tag) throws HiveException {
	
	//Added by Brantzhang for sorted merge join Begin
	//If this is a sorted merge join
	if(this.isSortedMergeJoin){
		try {
	      // 获知该行所属的表
	      alias = (byte)tag;
	      	      
	      if ((lastAlias == null) || (!lastAlias.equals(alias)))
	        nextSz = joinEmitInterval;
	      
	      //get the key/value pair
	      ArrayList<Object> key   = computeValues(row, joinKeys.get(alias), 
	    		                                  joinKeysObjectInspectors.get(alias));//连接键
	      boolean isNull = true;
	      for(Object keyo: key){
	    	  if(keyo != null)
	    		  isNull = false;
	      }
	      if(isNull)
	    	  return;
	      
	      ArrayList<Object> value = computeValues(row, joinValues.get(alias), 
	    		                                  joinValuesObjectInspectors.get(alias));//需要输出的连接值
	      row = null;
	      
	      // does this source need to be stored in the key/value list
	      if (tag != posBigTable) {
	        if (firstRow) {
	          metadataKeyTag = nextVal++;
	          
	          tableDesc keyTableDesc = conf.getKeyTblDesc();
	          SerDe keySerializer = (SerDe)ReflectionUtils.newInstance(keyTableDesc.getDeserializerClass(), null);
	          keySerializer.initialize(null, keyTableDesc.getProperties());

	          mapMetadata.put(Integer.valueOf(metadataKeyTag), 
	                          new MapJoinObjectCtx(
	                              ObjectInspectorUtils.getStandardObjectInspector(keySerializer.getObjectInspector(),
	                              ObjectInspectorCopyOption.WRITABLE),
	                          keySerializer, keyTableDesc, hconf));  //Added by Brantzhang for patch HIVE-1158
	          firstRow = false;
	        }
	        
	        // Send some status periodically 
	        numMapRowsRead++;
	        if (((numMapRowsRead % heartbeatInterval) == 0) && (reporter != null))
	          reporter.progress();

	        SortedKeyValueList<ArrayList<Object>, MapJoinObjectValue> kvList =  mapJoinLists.get(alias); 
	        RowContainer res = null; 
	        boolean isNewKey = !kvList.equalLastPutKey(key);
	        
	        if ( isNewKey ) {
	          //This is a new key
	          res = new RowContainer(rowBucketSize);  
	          res.add(value);  
	        }else {
	          //The key equals with the last key
	          kvList.saveToLastPutValue(value);
	        }
	        
	        if (metadataValueTag[tag] == -1) {
	          metadataValueTag[tag] = nextVal++;
	                    
	          tableDesc valueTableDesc = conf.getValueTblDescs().get(tag);
	          SerDe valueSerDe = (SerDe)ReflectionUtils.newInstance(valueTableDesc.getDeserializerClass(), null);
	          valueSerDe.initialize(null, valueTableDesc.getProperties());
	 
	          mapMetadata.put(Integer.valueOf(metadataValueTag[tag]),
	              new MapJoinObjectCtx(
	        			ObjectInspectorUtils.getStandardObjectInspector(valueSerDe.getObjectInspector(),
	        			  ObjectInspectorCopyOption.WRITABLE),
	        			valueSerDe, valueTableDesc, hconf)); 
	        }
	        
	        if ( isNewKey ) {
	           MapJoinObjectValue valueObj = new MapJoinObjectValue(metadataValueTag[tag], res);  
	           valueObj.setConf(hconf);  
	        	 
	           kvList.put(key, valueObj); 
	           if(numMapRowsRead%100000==0)
	        	   LOG.info("Used memory " + (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()) 
	        			    + " for " + numMapRowsRead + " rows!");
	        }
	        return;
	      }
	      
	      //close persistent file
	      if(firsttime){
	    	  for (Byte pos : order) {
	  	        if (pos.intValue() != tag) {
	  	          SortedKeyValueList<ArrayList<Object>, MapJoinObjectValue> kvList  = mapJoinLists.get(pos);
	  	                  
	  	          kvList.closePFile();
	  	        }
	  	      } 
	    	  
	    	  firsttime = false;
	      }
	    	  
	      // Add the value to the ArrayList
	      storage.get(alias).add(value);//存储流式表的该值
	      for (Byte pos : order) {
	        if (pos.intValue() != tag) {
	          MapJoinObjectValue o = (MapJoinObjectValue)mapJoinLists.get(pos).get(key);
	                  
	          if (o == null) {
	            storage.put(pos, dummyObjVectors[pos.intValue()]);     
	          }
	          else {
	            storage.put(pos, o.getObj());
	          }
	        }
	      }
	      
	      // generate the output records
	      checkAndGenObject();
	    
	      // done with the row
	      storage.get(alias).clear();

	      for (Byte pos : order)
	        if (pos.intValue() != tag)
	          storage.put(pos, null);
	    
	    } catch (SerDeException e) {
	      e.printStackTrace();
	      throw new HiveException(e);
	    }
	    
	}else{ 
	//Added by Brantzhang for sorted merge join End
    try {
      // 获知该行所属的表
      alias = (byte)tag;
      
      if ((lastAlias == null) || (!lastAlias.equals(alias)))
        nextSz = joinEmitInterval;
      
      // compute keys and values as StandardObjects  
      ArrayList<Object> key   = computeValues(row, joinKeys.get(alias), joinKeysObjectInspectors.get(alias));//连接键
      
      //Added by Brantzhang for hash map join Begin
      boolean isNull = true;
      for(Object keyo: key){
    	  if(keyo != null)
    		  isNull = false;
      }
      if(isNull)
    	  return;
      //Added by Brantzhang for hash map join End
      
      ArrayList<Object> value = computeValues(row, joinValues.get(alias), joinValuesObjectInspectors.get(alias));//需要输出的连接值
      
      row = null;
      
      // does this source need to be stored in the hash map
      if (tag != posBigTable) {
        if (firstRow) {
          metadataKeyTag = nextVal++;
          
          tableDesc keyTableDesc = conf.getKeyTblDesc();
          SerDe keySerializer = (SerDe)ReflectionUtils.newInstance(keyTableDesc.getDeserializerClass(), null);
          keySerializer.initialize(null, keyTableDesc.getProperties());

          mapMetadata.put(Integer.valueOf(metadataKeyTag), 
              new MapJoinObjectCtx(
                  ObjectInspectorUtils.getStandardObjectInspector(keySerializer.getObjectInspector(),
                      ObjectInspectorCopyOption.WRITABLE),
                  //keySerializer)); //Removed by Brantzhang for patch HIVE-1158
                  keySerializer, keyTableDesc, hconf));  //Added by Brantzhang for patch HIVE-1158
          firstRow = false;
        }
        
        // Send some status perodically 
        numMapRowsRead++;
        if (((numMapRowsRead % heartbeatInterval) == 0) && (reporter != null))
          reporter.progress();

        //HTree hashTable = mapJoinTables.get(alias);  //Removed by Brantzhang for patch HIVE-968
        HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue> hashTable =  mapJoinTables.get(alias);    //Added by Brantzhang for patch HIVE-968
        MapJoinObjectKey keyMap = new MapJoinObjectKey(metadataKeyTag, key);
        //MapJoinObjectValue o = (MapJoinObjectValue)hashTable.get(keyMap);  //Removed by Brantzhang for patch HIVE-968
        MapJoinObjectValue o = hashTable.get(keyMap);      //Added by Brantzhang for patch HIVE-968      
        //ArrayList<ArrayList<Object>> res = null;      //Removed by Brantzhang for patch HIVE-963
        RowContainer res = null;                   //Added by Brantzhang for patch HIVE-963
        
        boolean needNewKey = true;    //Added by Brantzhang for patch HIVE-968
        if (o == null) {
          //res = new ArrayList<ArrayList<Object>>();
          //res = new RowContainer();        //Added by Brantzhang for patch HIVE-963  //Removed by Brantzhang for patch HIVE-1158
          //res.add(value);         //Added by Brantzhang for patch HIVE-968  //Removed by Brantzhang for patch HIVE-1158
          int bucketSize = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEMAPJOINBUCKETCACHESIZE); //Added by Brantzhang for patch HIVE-1158
          res = new RowContainer(bucketSize);  //Added by Brantzhang for patch HIVE-1158
          res.add(value);  //Added by Brantzhang for patch HIVE-1158
        }
        else {
          res = o.getObj();
          //Added by Brantzhang for patch HIVE-968 Begin
          res.add(value);
          // If key already exists, HashMapWrapper.get() guarantees it is already in main memory HashMap
          // cache. So just replacing the object value should update the HashMapWrapper. This will save
          // the cost of constructing the new key/object and deleting old one and inserting the new one.
          if ( hashTable.cacheSize() > 0) {
            o.setObj(res);
            needNewKey = false;
          }
          //Added by Brantzhang for patch HIVE-968 End
        }
        
        //res.add(value);  //Removed by Brantzhang for patch HIVE-968
  
        if (metadataValueTag[tag] == -1) {
          metadataValueTag[tag] = nextVal++;
                    
          tableDesc valueTableDesc = conf.getValueTblDescs().get(tag);
          SerDe valueSerDe = (SerDe)ReflectionUtils.newInstance(valueTableDesc.getDeserializerClass(), null);
          valueSerDe.initialize(null, valueTableDesc.getProperties());
 
          mapMetadata.put(Integer.valueOf(metadataValueTag[tag]),
              //Removed by Brantzhang for patch HIVE-968 Begin
        		  /*
              new MapJoinObjectCtx(
                  ObjectInspectorUtils.getStandardObjectInspector(valueSerDe.getObjectInspector(),
                      ObjectInspectorCopyOption.WRITABLE),
              valueSerDe));
              */
              //Removed by Brantzhang for patch HIVE-968 End
        	  //Added by Brantzhang  for patch HIVE-968 Begin
        	  new MapJoinObjectCtx(
        			ObjectInspectorUtils.getStandardObjectInspector(valueSerDe.getObjectInspector(),
        			  ObjectInspectorCopyOption.WRITABLE),
        			//valueSerDe)); //Removed by Brantzhang for patch HIVE-1158
        			valueSerDe, valueTableDesc, hconf)); //Added by Brantzhang for patch HIVE-1158
        	  //Added by Brantzhang  for patch HIVE-968 End  
        }
        
        // Construct externalizable objects for key and value
        //Removed by Brantzhang for patch HIVE-968 Begin
        /*
        MapJoinObjectKey keyObj = new MapJoinObjectKey(metadataKeyTag, key);
        MapJoinObjectValue valueObj = new MapJoinObjectValue(metadataValueTag[tag], res);
        
        if (res.size() > 1)
          hashTable.remove(keyObj);

        // This may potentially increase the size of the hashmap on the mapper
        if (res.size() > mapJoinRowsKey) {
          //LOG.warn("Number of values for a given key " + keyObj + " are " + res.size());    //Removed by Brantzhang for patch HIVE-865
          //LOG.warn("used memory " + Runtime.getRuntime().totalMemory());                    //Removed by Brantzhang for patch HIVE-865
          //Added by Brantzhang for patch HIVE-865 Begin
          if ( res.size() % 100 == 0 ) {
        	LOG.warn("Number of values for a given key " + keyObj + " are " + res.size());
        	LOG.warn("used memory " + Runtime.getRuntime().totalMemory());
        	*/
            //Removed by Brantzhang for patch HIVE-968 End
         //Added by Brantzhang  for patch HIVE-968 Begin
         if ( needNewKey ) {
           MapJoinObjectKey keyObj = new MapJoinObjectKey(metadataKeyTag, key);
           //MapJoinObjectValue valueObj = new MapJoinObjectValue(metadataValueTag[tag], res);  //Removed by Brantzhang for patch HIVE-1158
           MapJoinObjectValue valueObj = new MapJoinObjectValue(metadataValueTag[tag], res);  //Added by Brantzhang for patch HIVE-1158
           valueObj.setConf(hconf);  //Added by Brantzhang for patch HIVE-1158
        	 
           // This may potentially increase the size of the hashmap on the mapper
           if (res.size() > mapJoinRowsKey) {
        	 if ( res.size() % 100 == 0 ) {
        	   LOG.warn("Number of values for a given key " + keyObj + " are " + res.size());
        	   LOG.warn("used memory " + Runtime.getRuntime().totalMemory());
        	}
          //Added by Brantzhang  for patch HIVE-968 End 
          }
          //Added by Brantzhang for patch HIVE-865 End
           hashTable.put(keyObj, valueObj); //Added by Brantzhang  for patch HIVE-968
           if(numMapRowsRead%100000==0)
        	   LOG.info("used memory " + (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()) + " for " + numMapRowsRead + " rows!");///////////////
        }
        //Removed by Brantzhang for patch HIVE-968 Begin
        /*
        hashTable.put(keyObj, valueObj);
        
        //Added by Brantzhang for patch HIVE-865 Begin
        // commit every 100 rows to prevent Out-of-memory exception
        if ( (res.size() % 100 == 0) && recman != null ) {
          recman.commit();
        }
        //Added by Brantzhang for patch HIVE-865 End
        */
        //Removed by Brantzhang for patch HIVE-968 End
        return;
      }

      // Add the value to the ArrayList
      storage.get(alias).add(value);//存储流式表的该值
      for (Byte pos : order) {
        if (pos.intValue() != tag) {
          MapJoinObjectKey keyMap = new MapJoinObjectKey(metadataKeyTag, key);
          MapJoinObjectValue o = (MapJoinObjectValue)mapJoinTables.get(pos).get(keyMap);
          
          if (o == null) {
            //storage.put(pos, new ArrayList<ArrayList<Object>>());//By Brantzhang for patch HIVE-926
        	storage.put(pos, dummyObjVectors[pos.intValue()]);     //By Brantzhang for patch HIVE-926
          }
          else {
            storage.put(pos, o.getObj());
          }
        }
      }
      //LOG.info("The number of objs to be joined is: " + n);//////////////
      
      // generate the output records
      checkAndGenObject();
    
      // done with the row
      storage.get(alias).clear();

      for (Byte pos : order)
        if (pos.intValue() != tag)
          storage.put(pos, null);
    
    } catch (SerDeException e) {
      e.printStackTrace();
      throw new HiveException(e);
    //} catch (IOException e) {  //Removed by Brantzhang for patch HIVE-968
      //e.printStackTrace();     //Removed by Brantzhang for patch HIVE-968
      //throw new HiveException(e);  //Removed by Brantzhang for patch HIVE-968
    }
	}//Added by Brantzhang for Sorted Merge Join
  }
  
  //Added by Brantzhang for patch HIVE-968 Begin
  public void closeOp(boolean abort) throws HiveException {
	for (HashMapWrapper hashTable: mapJoinTables.values()) {
	  hashTable.close();
	}
  }
  //Added by Brantzhang for patch HIVE-968 End
  
  /**
   * Implements the getName function for the Node Interface.
   * @return the name of the operator
   */
  public String getName() {
    return "MAPJOIN";
  }
  
  //Removed by Brantzhang for patch HIVE-968 Begin
  /*
  public void closeOp(boolean abort) throws HiveException {
    for (File hTbl : hTables) {
      deleteDir(hTbl);
    }
  }
  
  private void deleteDir(File dir) {
    if (dir.isDirectory()) {
      String[] children = dir.list();
      for (int i = 0; i < children.length; i++) {
        deleteDir(new File(dir, children[i]));
      }
    }

    dir.delete();
  }
  */
  //Removed by Brantzhang for patch HIVE-968
}
