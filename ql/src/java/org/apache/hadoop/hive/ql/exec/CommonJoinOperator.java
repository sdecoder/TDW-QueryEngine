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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;         //Added by Brantzhang for patch HIVE-591
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
//import java.util.Stack;         //Removed by Brantzhang for patch HIVE-963
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;  //Added by Brantzhang for patch HIVE-1137
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.joinCond;
import org.apache.hadoop.hive.ql.plan.joinDesc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.tableDesc;                       //Added by Brantzhang for patch HIVE-963
import org.apache.hadoop.hive.serde2.SerDe;                            //Added by Brantzhang for patch HIVE-963
import org.apache.hadoop.hive.serde2.SerDeException;                   //Added by Brantzhang for patch HIVE-963
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;       //Added by Brantzhang for patch HIVE-963
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;        //Added by Brantzhang for patch HIVE-963
import org.apache.hadoop.mapred.SequenceFileInputFormat;               //Added by Brantzhang for patch HIVE-1137
import org.apache.hadoop.util.ReflectionUtils;                         //Added by Brantzhang for patch HIVE-963

/**
 * Join operator implementation.
 */
public abstract class CommonJoinOperator<T extends joinDesc> extends Operator<T> implements Serializable {
  private static final long serialVersionUID = 1L;
  static final protected Log LOG = LogFactory.getLog(CommonJoinOperator.class.getName());

  public static class IntermediateObject {
    ArrayList<Object>[] objs;
    int curSize;

    public IntermediateObject(ArrayList<Object>[] objs, int curSize) {
      this.objs = objs;
      this.curSize = curSize;
    }

    public ArrayList<Object>[] getObjs() {
      return objs;
    }

    public int getCurSize() {
      return curSize;
    }

    //public void pushObj(ArrayList<Object> obj) {     //Removed by Brantzhang for patch HIVE-963
      //objs[curSize++] = obj;                         //Removed by Brantzhang for patch HIVE-963
    public void pushObj(ArrayList<Object> newObj) {    //Added by Brantzhang for patch HIVE-963
      objs[curSize++] = newObj;                        //Added by Brantzhang for patch HIVE-963
    }

    public void popObj() {
      curSize--;
    }
    
    //Added by Brantzhang for patch HIVE-870 Begin
    public Object topObj() {
      return objs[curSize-1];
    }
    //Added by Brantzhang for patch HIVE-870 End

  }

  transient protected int numAliases; // number of aliases
  /**
   * The expressions for join outputs.
   */
  transient protected Map<Byte, List<ExprNodeEvaluator>> joinValues; //每个表要输出的连接结果列
  /**
   * The ObjectInspectors for the join inputs.
   */
  transient protected Map<Byte, List<ObjectInspector>> joinValuesObjectInspectors;
  /**
   * The standard ObjectInspectors for the join inputs.
   */
  transient protected Map<Byte, List<ObjectInspector>> joinValuesStandardObjectInspectors; 
  
  transient static protected Byte[] order; // order in which the results should be output
  transient protected joinCond[] condn;
  transient protected boolean noOuterJoin;
  transient private Object[] dummyObj; // for outer joins, contains the
                                       // potential nulls for the concerned
                                       // aliases
  //transient private ArrayList<ArrayList<Object>>[] dummyObjVectors;  //Removed by Brantzhang for patch HIVE-926
  //transient protected ArrayList<ArrayList<Object>>[] dummyObjVectors;  //Added by Brantzhang for patch HIVE-926   //Removed by Brantzhang for patch HIVE-963 
  transient protected RowContainer<ArrayList<Object>>[] dummyObjVectors; // empty rows for each table       //Added by Brantzhang for patch HIVE-963
  //transient private Stack<Iterator<ArrayList<Object>>> iterators;      //Removed by Brantzhang for patch HIVE-870
  transient protected int totalSz; // total size of the composite object，连接计算的最后要输出的组合对象的列数
  
  // keys are the column names. basically this maps the position of the column in 
  // the output of the CommonJoinOperator to the input columnInfo.
  transient private Map<Integer, Set<String>> posToAliasMap;
  
  transient LazyBinarySerDe[] spillTableSerDe;       //Added by Brantzhang for patch HIVE-963
  transient protected Map<Byte, tableDesc> spillTableDesc; // spill tables are used if the join input is too large to fit in memory  //Added by Brantzhang for patch HIVE-1037
  
  //HashMap<Byte, ArrayList<ArrayList<Object>>> storage;  //Removed by Brantzhang for patch HIVE-963
  HashMap<Byte, RowContainer<ArrayList<Object>>> storage; // map b/w table alias to RowContainer      //Added by Brantzhang for patch HIVE-963
  int joinEmitInterval = -1;
  int joinCacheSize = 0;       //Added by Brantzhang for patch HIVE-963
  int nextSz = 0;
  transient Byte lastAlias = null;
  
  protected int populateJoinKeyValue(Map<Byte, List<ExprNodeEvaluator>> outMap,
      Map<Byte, List<exprNodeDesc>> inputMap) {

    int total = 0;

    Iterator<Map.Entry<Byte, List<exprNodeDesc>>> entryIter = inputMap.entrySet().iterator();
    while (entryIter.hasNext()) {
      Map.Entry<Byte, List<exprNodeDesc>> e = (Map.Entry<Byte, List<exprNodeDesc>>) entryIter.next();
      //Byte key = (Byte) e.getKey();  //Removed by Brantzhang for patch HIVE-853
      Byte key = order[e.getKey()];    //Added by Brantzhang for patch HIVE-853
      
      List<exprNodeDesc> expr = (List<exprNodeDesc>) e.getValue();
      int sz = expr.size();
      total += sz;

      List<ExprNodeEvaluator> valueFields = new ArrayList<ExprNodeEvaluator>();

      for (int j = 0; j < sz; j++)
        valueFields.add(ExprNodeEvaluatorFactory.get(expr.get(j)));

      outMap.put(key, valueFields);
    }
    
    return total;
  }

  protected static HashMap<Byte, List<ObjectInspector>> getObjectInspectorsFromEvaluators(
      Map<Byte, List<ExprNodeEvaluator>> exprEntries, ObjectInspector[] inputObjInspector)
      throws HiveException {
    HashMap<Byte, List<ObjectInspector>> result = new HashMap<Byte, List<ObjectInspector>>();
    for(Entry<Byte, List<ExprNodeEvaluator>> exprEntry : exprEntries.entrySet()) {
      Byte alias = exprEntry.getKey();
      List<ExprNodeEvaluator> exprList = exprEntry.getValue();
      ArrayList<ObjectInspector> fieldOIList = new ArrayList<ObjectInspector>();
      for (int i=0; i<exprList.size(); i++) {
        fieldOIList.add(exprList.get(i).initialize(inputObjInspector[alias]));
      }
      result.put(alias, fieldOIList);
    }
    return result;
  }
  
  protected static HashMap<Byte, List<ObjectInspector>> getStandardObjectInspectors(
      Map<Byte, List<ObjectInspector>> aliasToObjectInspectors) {
    HashMap<Byte, List<ObjectInspector>> result = new HashMap<Byte, List<ObjectInspector>>();
    for(Entry<Byte, List<ObjectInspector>> oiEntry: aliasToObjectInspectors.entrySet()) {
      Byte alias = oiEntry.getKey();
      List<ObjectInspector> oiList = oiEntry.getValue();
      ArrayList<ObjectInspector> fieldOIList = new ArrayList<ObjectInspector>(oiList.size());
      for (int i=0; i<oiList.size(); i++) {
        fieldOIList.add(ObjectInspectorUtils.getStandardObjectInspector(oiList.get(i), 
            ObjectInspectorCopyOption.WRITABLE));
      }
      result.put(alias, fieldOIList);
    }
    return result;
    
  }
  
  protected static <T extends joinDesc> ObjectInspector getJoinOutputObjectInspector(Byte[] order,
      Map<Byte, List<ObjectInspector>> aliasToObjectInspectors, T conf) {
    ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
    for (Byte alias : order) {
      List<ObjectInspector> oiList = aliasToObjectInspectors.get(alias);
      structFieldObjectInspectors.addAll(oiList);
    }
    
    StructObjectInspector joinOutputObjectInspector = ObjectInspectorFactory
      .getStandardStructObjectInspector(conf.getOutputColumnNames(), structFieldObjectInspectors);
    return joinOutputObjectInspector;
  }
  
  Configuration hconf; //Added by Brantzhang for patch HIVE-1158
  
  protected void initializeOp(Configuration hconf) throws HiveException {
    LOG.info("COMMONJOIN " + ((StructObjectInspector)inputObjInspectors[0]).getTypeName());   
    totalSz = 0;
    this.hconf = hconf; //Added by Brantzhang for patch HIVE-1158
    // Map that contains the rows for each alias
    //storage = new HashMap<Byte, ArrayList<ArrayList<Object>>>();   //Removed by Brantzhang for patch HIVE-963
    storage = new HashMap<Byte, RowContainer<ArrayList<Object>>>();  //Added by Brantzhang for patch HIVE-963

    numAliases = conf.getExprs().size();
    
    joinValues = new HashMap<Byte, List<ExprNodeEvaluator>>();

    if (order == null) {
      //order = new Byte[numAliases];        //Removed by Brantzhang for patch HIVE-853
      //for (int i = 0; i < numAliases; i++) //Removed by Brantzhang for patch HIVE-853
        //order[i] = (byte) i;               //Removed by Brantzhang for patch HIVE-853
      order = conf.getTagOrder();            //Added by Brantzhang for patch HIVE-853
    }
    condn = conf.getConds();
    noOuterJoin = conf.getNoOuterJoin();

    totalSz = populateJoinKeyValue(joinValues, conf.getExprs());

    joinValuesObjectInspectors = getObjectInspectorsFromEvaluators(joinValues, inputObjInspectors);
    joinValuesStandardObjectInspectors = getStandardObjectInspectors(joinValuesObjectInspectors);
      
    dummyObj = new Object[numAliases];
    //dummyObjVectors = new ArrayList[numAliases];      //Removed by Brantzhang for patch HIVE-963
    dummyObjVectors = new RowContainer[numAliases];     //Added by Brantzhang for patch HIVE-963

    //int pos = 0;  //Removed by Brantzhang for patch HIVE-963
    //Added by Brantzhang for patch HIVE-963 Begin
    joinEmitInterval = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEJOINEMITINTERVAL);
    joinCacheSize    = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEJOINCACHESIZE);
       
    // construct dummy null row (indicating empty table) and 
    // construct spill table serde which is used if input is too 
    // large to fit into main memory.
    byte pos = 0;
    //Added by Brantzhang for patch HIVE-963 End
    for (Byte alias : order) {
      int sz = conf.getExprs().get(alias).size();
      ArrayList<Object> nr = new ArrayList<Object>(sz);

      for (int j = 0; j < sz; j++)
        nr.add(null);
      dummyObj[pos] = nr;
      //ArrayList<ArrayList<Object>> values = new ArrayList<ArrayList<Object>>();   //Removed by Brantzhang for patch HIVE-963
      // there should be only 1 dummy object in the RowContainer                          //Added by Brantzhang for patch HIVE-963
      //RowContainer<ArrayList<Object>> values = new RowContainer<ArrayList<Object>>(1);    //Added by Brantzhang for patch HIVE-963  //Removed by Brantzhang for patch HIVE-1158
      RowContainer<ArrayList<Object>> values = getRowContainer(hconf, pos, alias, 1); //Added by Brantzhang for patch HIVE-1158
      values.add((ArrayList<Object>) dummyObj[pos]);
      dummyObjVectors[pos] = values;
      
      //Added by Brantzhang for patch HIVE-963 Begin
      // if serde is null, the input doesn't need to be spilled out 
      // e.g., the output columns does not contains the input table
      //Removed by Brantzhang for patch HIVE-1158 Begin
      /*
      SerDe serde = getSpillSerDe(pos);
      RowContainer rc = new RowContainer(joinCacheSize);  //Added by Brantzhang for patch HIVE-1037
      if ( serde != null ) {
        //RowContainer rc = new RowContainer(joinCacheSize);  //Removed by Brantzhang for patch HIVE-1037
        
        // arbitrary column names used internally for serializing to spill table
        List<String> colList = new ArrayList<String>();
        for ( int i = 0; i < sz; ++i )
          colList.add(alias + "_VAL_" + i);
             
          // object inspector for serializing input tuples
          StructObjectInspector rcOI = 
          ObjectInspectorFactory.getStandardStructObjectInspector(
                                colList,
                                joinValuesStandardObjectInspectors.get(pos));
              
          rc.setSerDe(serde, rcOI);
          //storage.put(pos, rc);  //Removed by Brantzhang for patch HIVE-1037
       //} else {   //Removed by Brantzhang for patch HIVE-1037
         //storage.put(pos, new RowContainer(1)); // empty row container just for place holder   //Removed by Brantzhang for patch HIVE-1037
      }
      //Added by Brantzhang for patch HIVE-963 End
       */
      //Removed by Brantzhang for patch HIVE-1158 End
      RowContainer rc = getRowContainer(hconf, pos, alias, joinCacheSize); //Added by Brantzhang for patch HIVE-1158
      storage.put(pos, rc);  //Added by Brantzhang for patch HIVE-1037
      pos++;
    }

    //iterators = new Stack<Iterator<ArrayList<Object>>>();    //Removed by Brantzhang for patch HIVE-870
    
    //joinEmitInterval = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEJOINEMITINTERVAL);         //Removed by Brantzhang for patch HIVE-963
    
    forwardCache = new Object[totalSz];
    
    outputObjInspector = getJoinOutputObjectInspector(order, joinValuesStandardObjectInspectors, conf);
    LOG.info("JOIN " + ((StructObjectInspector)outputObjInspector).getTypeName() + " totalsz = " + totalSz);
  }
  
  //Added by Brantzhang for patch HIVE-963 Begin
  //private SerDe getSpillSerDe(int pos) {  //Removed by Brantzhang for patch HIVE-1037
	//tableDesc desc = conf.getSpillTableDesc(pos);  //Removed by Brantzhang for patch HIVE-1037
  private SerDe getSpillSerDe(byte pos) {  //Added by Brantzhang for patch HIVE-1037
	tableDesc desc = getSpillTableDesc(pos);  //Added by Brantzhang for patch HIVE-1037
	if ( desc == null )
	  return null;
	SerDe sd = (SerDe) ReflectionUtils.newInstance(desc.getDeserializerClass(),  null);
	try {
	  sd.initialize(null, desc.getProperties());
	    } catch (SerDeException e) {
	       e.printStackTrace();
	       return null;
	    }
	return sd;
  }
  //Added by Brantzhang for patch HIVE-963 End
  
  //Added by Brantzhang for patch HIVE-1037 Begin
  private void initSpillTables() {
	Map<Byte, List<exprNodeDesc>> exprs = conf.getExprs();
	/////////////////
	System.out.println("size: " + exprs.size());
	for(int tag = 0; tag < exprs.size(); tag++){
		System.out.println("table: " +tag);
		List<exprNodeDesc> valueCols = exprs.get((byte)tag);
		System.out.println("Column number: " + valueCols.size());
		for(exprNodeDesc end: valueCols){
			System.out.println(end);
			System.out.println(end.getTypeString());
		}
			
	}
	
	////////////////////
	spillTableDesc = new HashMap<Byte, tableDesc>(exprs.size());
	for (int tag = 0; tag < exprs.size(); tag++) {
	  List<exprNodeDesc> valueCols = exprs.get((byte)tag);
	  int columnSize = valueCols.size();
	  StringBuffer colNames = new StringBuffer();
	  StringBuffer colTypes = new StringBuffer();
	  if ( columnSize <= 0 )
	    continue;
	  for (int k = 0; k < columnSize; k++) {
	     String newColName = tag + "_VALUE_" + k; // any name, it does not matter.
	     colNames.append(newColName);
	     colNames.append(',');
	     colTypes.append(valueCols.get(k).getTypeString());
	     colTypes.append(',');
	  }
	  // remove the last ','
	  colNames.setLength(colNames.length()-1);
	  colTypes.setLength(colTypes.length()-1);
	  tableDesc tblDesc = 
	    new tableDesc(LazyBinarySerDe.class,
	           SequenceFileInputFormat.class,
	           HiveSequenceFileOutputFormat.class, 
	           Utilities.makeProperties(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "" + Utilities.ctrlaCode,
	                 org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS, colNames.toString(),
	                 org.apache.hadoop.hive.serde.Constants.LIST_COLUMN_TYPES, colTypes.toString()));
	  spillTableDesc.put((byte)tag, tblDesc);
	}
  }
	    
  public tableDesc getSpillTableDesc(Byte alias) {
	if(spillTableDesc == null || spillTableDesc.size() == 0)
	  initSpillTables();
	return spillTableDesc.get(alias);
  }
  //Added by Brantzhang for patch HIVE-1037 End

  public void startGroup() throws HiveException {
    LOG.trace("Join: Starting new group");
    //storage.clear();                  //Removed by Brantzhang for patch HIVE-963
    //for (Byte alias : order)          //Removed by Brantzhang for patch HIVE-963
      //storage.put(alias, new ArrayList<ArrayList<Object>>());   //Removed by Brantzhang for patch HIVE-963 
    for (RowContainer<ArrayList<Object>> alw: storage.values()) {  //Added by Brantzhang for patch HIVE-963
      alw.clear();       //Added by Brantzhang for patch HIVE-963
    }        //Added by Brantzhang for patch HIVE-963
  }

  protected int getNextSize(int sz) {
    // A very simple counter to keep track of join entries for a key
    if (sz >= 100000)
      return sz + 100000;
    
    return 2 * sz;
  }

  transient protected Byte alias;
  
  /**
   * Return the value as a standard object.
   * StandardObject can be inspected by a standard ObjectInspector.
   */
  protected static ArrayList<Object> computeValues(Object row,
    List<ExprNodeEvaluator> valueFields, List<ObjectInspector> valueFieldsOI) throws HiveException {
    
    // Compute the values
    ArrayList<Object> nr = new ArrayList<Object>(valueFields.size());
    for (int i=0; i<valueFields.size(); i++) {
      //nr.add(ObjectInspectorUtils.copyToStandardObject(  //Removed by Brantzhang for patch HIVE-963
      nr.add((Object) ObjectInspectorUtils.copyToStandardObject(  //Added by Brantzhang for patch HIVE-963
          valueFields.get(i).evaluate(row),
          valueFieldsOI.get(i),
          ObjectInspectorCopyOption.WRITABLE));
    }
    
    return nr;
  }
  
  transient Object[] forwardCache;
  
  private void createForwardJoinObject(IntermediateObject intObj,
      boolean[] nullsArr) throws HiveException {
    int p = 0;
    for (int i = 0; i < numAliases; i++) {
      Byte alias = order[i];
      int sz = joinValues.get(alias).size();
      if (nullsArr[i]) {
        for (int j = 0; j < sz; j++) {
          forwardCache[p++] = null;
        }
      } else {
        ArrayList<Object> obj = intObj.getObjs()[i];
        for (int j = 0; j < sz; j++) {
          forwardCache[p++] = obj.get(j);
        }
      }
    }
    forward(forwardCache, outputObjInspector);
  }

  private void copyOldArray(boolean[] src, boolean[] dest) {
    for (int i = 0; i < src.length; i++)
      dest[i] = src[i];
  }

  private ArrayList<boolean[]> joinObjectsInnerJoin(ArrayList<boolean[]> resNulls,
      ArrayList<boolean[]> inputNulls, ArrayList<Object> newObj,
      IntermediateObject intObj, int left, boolean newObjNull) {
    if (newObjNull)
      return resNulls;
    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];
      if (!oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = false;
        resNulls.add(newNulls);
      }
    }
    return resNulls;
  }
  
  //Added by Brantzhang for patch HIVE-870 Begin
  /**
   * Implement semi join operator.
   */
  private ArrayList<boolean[]> joinObjectsLeftSemiJoin(ArrayList<boolean[]> resNulls,
                                                       ArrayList<boolean[]> inputNulls, 
                                                       ArrayList<Object> newObj,
                                                       IntermediateObject intObj, 
                                                       int left, 
                                                       boolean newObjNull) {
    if (newObjNull)
      return resNulls;
    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];
      if (!oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = false;
        resNulls.add(newNulls);
      }
    }
    return resNulls;
  }
  //Added by Brantzhang for patch HIVE-870 End

  private ArrayList<boolean[]> joinObjectsLeftOuterJoin(
      ArrayList<boolean[]> resNulls, ArrayList<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int left,
      boolean newObjNull) {
    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      copyOldArray(oldNulls, newNulls);
      if (oldObjNull)
        newNulls[oldNulls.length] = true;
      else
        newNulls[oldNulls.length] = newObjNull;
      resNulls.add(newNulls);
    }
    return resNulls;
  }

  private ArrayList<boolean[]> joinObjectsRightOuterJoin(
      ArrayList<boolean[]> resNulls, ArrayList<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int left,
      boolean newObjNull, boolean firstRow) {
    if (newObjNull)
      return resNulls;

    if (inputNulls.isEmpty() && firstRow) {
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      for (int i = 0; i < intObj.getCurSize() - 1; i++)
        newNulls[i] = true;
      newNulls[intObj.getCurSize()-1] = newObjNull;
      resNulls.add(newNulls);
      return resNulls;
    }

    boolean allOldObjsNull = firstRow;

    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      if (!oldNulls[left]) {
        allOldObjsNull = false;
        break;
      }
    }

    nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];

      if (!oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
      } else if (allOldObjsNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        for (int i = 0; i < intObj.getCurSize() - 1; i++)
          newNulls[i] = true;
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
        return resNulls;
      }
    }
    return resNulls;
  }

  private ArrayList<boolean[]> joinObjectsFullOuterJoin(
      ArrayList<boolean[]> resNulls, ArrayList<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int left,
      boolean newObjNull, boolean firstRow) {
    if (newObjNull) {
      Iterator<boolean[]> nullsIter = inputNulls.iterator();
      while (nullsIter.hasNext()) {
        boolean[] oldNulls = nullsIter.next();
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
      }
      return resNulls;
    }

    if (inputNulls.isEmpty() && firstRow) {
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      for (int i = 0; i < intObj.getCurSize() - 1; i++)
        newNulls[i] = true;
      newNulls[intObj.getCurSize()-1] = newObjNull;
      resNulls.add(newNulls);
      return resNulls;
    }

    boolean allOldObjsNull = firstRow;

    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      if (!oldNulls[left]) {
        allOldObjsNull = false;
        break;
      }
    }
    boolean rhsPreserved = false;

    nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];

      if (!oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
      } else if (oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = true;
        resNulls.add(newNulls);

        if (allOldObjsNull && !rhsPreserved) {
          newNulls = new boolean[intObj.getCurSize()];
          for (int i = 0; i < oldNulls.length; i++)
            newNulls[i] = true;
          newNulls[oldNulls.length] = false;
          resNulls.add(newNulls);
          rhsPreserved = true;
        }
      }
    }
    return resNulls;
  }

  /*
   * The new input is added to the list of existing inputs. Each entry in the
   * array of inputNulls denotes the entries in the intermediate object to be
   * used. The intermediate object is augmented with the new object, and list of
   * nulls is changed appropriately. The list will contain all non-nulls for a
   * inner join. The outer joins are processed appropriately.
   */
  private ArrayList<boolean[]> joinObjects(ArrayList<boolean[]> inputNulls,
                                        ArrayList<Object> newObj, IntermediateObject intObj, 
                                        int joinPos, boolean firstRow) {
    ArrayList<boolean[]> resNulls = new ArrayList<boolean[]>();
    boolean newObjNull = newObj == dummyObj[joinPos] ? true : false;
    if (joinPos == 0) {
      if (newObjNull)
        return null;
      boolean[] nulls = new boolean[1];
      nulls[0] = newObjNull;
      resNulls.add(nulls);
      return resNulls;
    }

    int left = condn[joinPos - 1].getLeft();
    int type = condn[joinPos - 1].getType();

    // process all nulls for RIGHT and FULL OUTER JOINS
    if (((type == joinDesc.RIGHT_OUTER_JOIN) || (type == joinDesc.FULL_OUTER_JOIN))
        && !newObjNull && (inputNulls == null) && firstRow) {
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      for (int i = 0; i < newNulls.length - 1; i++)
        newNulls[i] = true;
      newNulls[newNulls.length - 1] = false;
      resNulls.add(newNulls);
      return resNulls;
    }

    if (inputNulls == null)
      return null;

    if (type == joinDesc.INNER_JOIN)
      return joinObjectsInnerJoin(resNulls, inputNulls, newObj, intObj, left,
          newObjNull);
    else if (type == joinDesc.LEFT_OUTER_JOIN)
      return joinObjectsLeftOuterJoin(resNulls, inputNulls, newObj, intObj,
          left, newObjNull);
    else if (type == joinDesc.RIGHT_OUTER_JOIN)
      return joinObjectsRightOuterJoin(resNulls, inputNulls, newObj, intObj,
                                       left, newObjNull, firstRow);
    else if (type == joinDesc.LEFT_SEMI_JOIN)      //Added by Brantzhang for patch HIVE-870
      return joinObjectsLeftSemiJoin(resNulls, inputNulls, newObj, intObj,    //Added by Brantzhang for patch HIVE-870
                                       left, newObjNull);      //Added by Brantzhang for patch HIVE-870
    
    assert (type == joinDesc.FULL_OUTER_JOIN);
    return joinObjectsFullOuterJoin(resNulls, inputNulls, newObj, intObj, left,
                                    newObjNull, firstRow);
  }

  /*
   * genObject is a recursive function. For the inputs, a array of bitvectors is
   * maintained (inputNulls) where each entry denotes whether the element is to
   * be used or not (whether it is null or not). The size of the bitvector is
   * same as the number of inputs under consideration currently. When all inputs
   * are accounted for, the output is forwared appropriately.
   */
  private void genObject(ArrayList<boolean[]> inputNulls, int aliasNum,
                         IntermediateObject intObj, boolean firstRow) throws HiveException {
    boolean childFirstRow = firstRow;
    boolean skipping = false;        //Added by Brantzhang for patch HIVE-870
    
    if (aliasNum < numAliases) {
      //Iterator<ArrayList<Object>> aliasRes = storage.get(order[aliasNum])  //Removed by Brantzhang for patch HIVE-870
          //.iterator();                                                     //Removed by Brantzhang for patch HIVE-870
      //iterators.push(aliasRes);                                            //Removed by Brantzhang for patch HIVE-870
      
      // search for match in the rhs table                                            //Added by Brantzhang for patch HIVE-870	
      //Iterator<ArrayList<Object>> aliasRes = storage.get(order[aliasNum]).iterator(); //Added by Brantzhang for patch HIVE-870  //Removed by Brantzhang for patch HIVE-963	
      //while (aliasRes.hasNext()) {    //Removed by Brantzhang for patch HIVE-963  
      //Added by Brantzhang for patch HIVE-963 Begin
      RowContainer<ArrayList<Object>> aliasRes = storage.get(order[aliasNum]);
    	      
      for (ArrayList<Object> newObj = aliasRes.first(); 
    	           newObj != null; 
    	           newObj = aliasRes.next()) {
      //Added by Brantzhang for patch HIVE-963 End
        //ArrayList<Object> newObj = aliasRes.next(); //Removed by Brantzhang for patch HIVE-963
        
        //Added by Brantzhang for patch HIVE-870 Begin
        // check for skipping in case of left semi join
        if (aliasNum > 0 &&
          condn[aliasNum - 1].getType() ==  joinDesc.LEFT_SEMI_JOIN &&
               newObj != dummyObj[aliasNum] ) { // successful match
          skipping = true;
        }
        //Added by Brantzhang for patch HIVE-870 End
        
        intObj.pushObj(newObj);
        //ArrayList<boolean[]> newNulls = joinObjects(inputNulls, newObj, intObj,   //Removed by Brantzhang for patch HIVE-870
        //                                         aliasNum, childFirstRow);        //Removed by Brantzhang for patch HIVE-870
        
        //Added by Brantzhang for patch HIVE-870 Begin
        // execute the actual join algorithm
        ArrayList<boolean[]> newNulls =  joinObjects(inputNulls, newObj, intObj,
                                                     aliasNum, childFirstRow);
                
        // recursively call the join the other rhs tables
        //Added by Brantzhang for patch HIVE-870 End
        genObject(newNulls, aliasNum + 1, intObj, firstRow);
        
        intObj.popObj();
        firstRow = false;
        
        //Added by Brantzhang for patch HIVE-870 Begin
        // if left-semi-join found a match, skipping the rest of the rows in the rhs table of the semijoin
        if ( skipping ) {
          break;
        }
        //Added by Brantzhang for patch HIVE-870 End
      }
      //iterators.pop();    //Removed by Brantzhang for patch HIVE-870
    } else {
      if (inputNulls == null)
        return;
      Iterator<boolean[]> nullsIter = inputNulls.iterator();
      while (nullsIter.hasNext()) {
        boolean[] nullsVec = nullsIter.next();
        createForwardJoinObject(intObj, nullsVec);
      }
    }
  }

  /**
   * Forward a record of join results.
   * 
   * @throws HiveException
   */
  public void endGroup() throws HiveException {
    LOG.trace("Join Op: endGroup called: numValues=" + numAliases);
    checkAndGenObject();
  }
  
  //Added by Brantzhang for patch HIVE-591 Begin
  private void genUniqueJoinObject(int aliasNum, IntermediateObject intObj)
                 throws HiveException {
    if (aliasNum == numAliases) {
      int p = 0;
      for (int i = 0; i < numAliases; i++) {
        int sz = joinValues.get(order[i]).size();
        ArrayList<Object> obj = intObj.getObjs()[i];
        for (int j = 0; j < sz; j++) {
          forwardCache[p++] = obj.get(j);
        }
      }
        
      forward(forwardCache, outputObjInspector);
      return;
    }
      
    //Iterator<ArrayList<Object>> alias = storage.get(order[aliasNum]).iterator(); //Removed by Brantzhang for patch HIVE-963
    //while (alias.hasNext()) {  //Removed by Brantzhang for patch HIVE-963
      //intObj.pushObj(alias.next());  //Removed by Brantzhang for patch HIVE-963
    //Added by Brantzhang for patch HIVE-963 Begin
    RowContainer<ArrayList<Object>> alias = storage.get(order[aliasNum]);
    for (ArrayList<Object> row = alias.first();
      row != null;
      row = alias.next() ) {
      intObj.pushObj(row);
    //Added by Brantzhang for patch HIVE-963 End
      genUniqueJoinObject(aliasNum+1, intObj);
      intObj.popObj();
    }
  }
  //Added by Brantzhang for patch HIVE-591 End
  
  protected void checkAndGenObject() throws HiveException {
	//Removed by Brantzhang for patch HIVE-591 Begin
	  /*
    // does any result need to be emitted
    for (int i = 0; i < numAliases; i++) {
      Byte alias = order[i];
      if (storage.get(alias).iterator().hasNext() == false) { 
        if (noOuterJoin) {
          LOG.trace("No data for alias=" + i);
          return;
        } else {
      */
    //Removed by Brantzhang for patch HIVE-591 End
	//Added by Brantzhang for patch HIVE-591 Begin
	if (condn[0].getType() == joinDesc.UNIQUE_JOIN) {
      IntermediateObject intObj = 
		                        new IntermediateObject(new ArrayList[numAliases], 0);
		        
	  // Check if results need to be emitted.
	  // Results only need to be emitted if there is a non-null entry in a table
	  // that is preserved or if there are no non-null entries
	  boolean preserve = false; // Will be true if there is a non-null entry
		                        // in a preserved table
      boolean hasNulls = false; // Will be true if there are null entries
	  for (int i = 0; i < numAliases; i++) {
		Byte alias = order[i];
		//Iterator<ArrayList<Object>> aliasRes = storage.get(alias).iterator(); //Removed by Brantzhang for patch HIVE-963
		//if (aliasRes.hasNext() == false) {  //Removed by Brantzhang for patch HIVE-963
	//Added by Brantzhang for patch HIVE-591 End
          //storage.put(alias, dummyObjVectors[i]);  //Removed by Brantzhang for patch HIVE-963
		RowContainer<ArrayList<Object>> alw =  storage.get(alias);  //Added by Brantzhang for patch HIVE-963
		if ( alw.size() == 0) {  //Added by Brantzhang for patch HIVE-963
		  alw.add((ArrayList<Object>)dummyObj[i]); //Added by Brantzhang for patch HIVE-963
          //Added by Brantzhang for patch HIVE-591 Begin
          hasNulls = true;
        } else if(condn[i].getPreserved()) {
            preserve = true;
        //Added by Brantzhang for patch HIVE-591 End
        }
      }
	  //Added by Brantzhang for patch HIVE-591 Begin
	  if (hasNulls && !preserve) {
		return;
	  }
		  
	  LOG.trace("calling genUniqueJoinObject");
	  genUniqueJoinObject(0, new IntermediateObject(new ArrayList[numAliases], 0));
	  LOG.trace("called genUniqueJoinObject");
	} else {
	  // does any result need to be emitted
	  for (int i = 0; i < numAliases; i++) {
		Byte alias = order[i];
		//if (storage.get(alias).iterator().hasNext() == false) {  //Removed by Brantzhang for patch HIVE-963
		RowContainer<ArrayList<Object>> alw =  storage.get(alias); //Added by Brantzhang for patch HIVE-963
		if (alw.size() == 0) {  //Added by Brantzhang for patch HIVE-963
		  if (noOuterJoin) {
		    LOG.trace("No data for alias=" + i);
		    return;
		  } else {
		    //storage.put(alias, dummyObjVectors[i]); //Removed by Brantzhang for patch HIVE-963
			alw.add((ArrayList<Object>)dummyObj[i]);  //Added by Brantzhang for patch HIVE-963
		  }
		}
	 }
		        
     genObject(null, 0, new IntermediateObject(new ArrayList[numAliases], 0), true);
	  //Added by Brantzhang for patch HIVE-591 End
    }
    
	//Removed by Brantzhang for patch HIVE-591 Begin
    /*LOG.trace("calling genObject");
    genObject(null, 0, new IntermediateObject(new ArrayList[numAliases], 0), true);
    LOG.trace("called genObject");*/
	//Removed by Brantzhang for patch HIVE-591 End
  }

  /**
   * All done
   * 
   */
  public void closeOp(boolean abort) throws HiveException {
    LOG.trace("Join Op close");
    for ( RowContainer<ArrayList<Object>> alw: storage.values() )   //Added by Brantzhang for patch HIVE-963
      alw.clear(); // clean up the temp files                       //Added by Brantzhang for patch HIVE-963
      storage.clear();                                              //Added by Brantzhang for patch HIVE-963
  }

  @Override
  public String getName() {
    return "JOIN";
  }

  /**
   * @return the posToAliasMap
   */
  public Map<Integer, Set<String>> getPosToAliasMap() {
    return posToAliasMap;
  }

  /**
   * @param posToAliasMap the posToAliasMap to set
   */
  public void setPosToAliasMap(Map<Integer, Set<String>> posToAliasMap) {
    this.posToAliasMap = posToAliasMap;
  }
  
  //Added by Brantzhang for patch HIVE-1158 Begin
  RowContainer getRowContainer(Configuration hconf, byte pos, Byte alias,
		    int containerSize) throws HiveException {
    tableDesc tblDesc = getSpillTableDesc(alias);
	SerDe serde = getSpillSerDe(alias);
		   
	if (serde == null) {
	  containerSize = 1;
	}
		  
	RowContainer rc = new RowContainer(containerSize);
	StructObjectInspector rcOI = null;
	if (tblDesc != null) {
	  // arbitrary column names used internally for serializing to spill table
	  List<String> colNames = Utilities.getColumnNames(tblDesc.getProperties());
	  // object inspector for serializing input tuples
	  rcOI = ObjectInspectorFactory.getStandardStructObjectInspector(colNames,
		      joinValuesStandardObjectInspectors.get(pos));
	}
		  
	rc.setSerDe(serde, rcOI);
    rc.setTableDesc(tblDesc);
	return rc;
  }
  //Added by Brantzhang for patch HIVE-1158 End
}
