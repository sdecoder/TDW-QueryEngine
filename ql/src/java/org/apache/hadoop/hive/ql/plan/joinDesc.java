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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.exec.Utilities;                   //Added by Brantzhang for patch HIVE-963
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;  //Added by Brantzhang for patch HIVE-963
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;  //Added by Brantzhang for patch HIVE-963
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;           //Added by Brantzhang for patch HIVE-963
import org.apache.hadoop.mapred.SequenceFileInputFormat;          //Added by Brantzhang for patch HIVE-963
import java.util.ArrayList;
//import java.util.HashMap;       //Removed by Brantzhang for patch HIVE-963
//import java.util.Iterator;      //Removed by Brantzhang for patch HIVE-963
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
//import java.util.Set;           //Removed by Brantzhang for patch HIVE-963
//import java.util.Map.Entry;     //Removed by Brantzhang for patch HIVE-963

/**
 * Join operator Descriptor implementation.
 * 
 */
@explain(displayName="Join Operator")
public class joinDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  public static final int INNER_JOIN = 0;
  public static final int LEFT_OUTER_JOIN = 1;
  public static final int RIGHT_OUTER_JOIN = 2;
  public static final int FULL_OUTER_JOIN = 3;
  public static final int UNIQUE_JOIN = 4;      //Added by Brantzhang for patch HIVE-591
  public static final int LEFT_SEMI_JOIN   = 5; //Added by Brantzhang for patch HIVE-870

  // alias to key mapping
  private Map<Byte, List<exprNodeDesc>> exprs;
  
  //used for create joinOutputObjectInspector
  protected java.util.ArrayList<java.lang.String> outputColumnNames;
  
  // key:column output name, value:tag
  transient private Map<String, Byte> reversedExprs;
  
  // No outer join involved
  protected boolean noOuterJoin;

  protected joinCond[] conds;
  
  protected Byte[] tagOrder;   //Added by Brantzhang for patch HIVE-853
  
  //protected tableDesc[] spillTableDesc; // spill tables are used if the join input is too large to fit in memory      //Added by Brantzhang for patch HIVE-963  //Removed by Brantzhang for patch HIVE-1037
  
  public joinDesc() { }
  
  public joinDesc(final Map<Byte, List<exprNodeDesc>> exprs, ArrayList<String> outputColumnNames, final boolean noOuterJoin, final joinCond[] conds) {
    this.exprs = exprs;
    this.outputColumnNames = outputColumnNames;
    this.noOuterJoin = noOuterJoin;
    this.conds = conds;
    
    //Added by Brantzhang for patch HIVE-853 Begin
    tagOrder = new Byte[exprs.size()];
    for(int i = 0; i<tagOrder.length; i++)
    {
      tagOrder[i] = (byte)i;
    }
    //Added by Brantzhang for patch HIVE-853 End
    
    //initSpillTables();   //Added by Brantzhang for patch HIVE-963  //Removed by Brantzhang for patch HIVE-1037
  }
  
  public joinDesc(final Map<Byte, List<exprNodeDesc>> exprs, ArrayList<String> outputColumnNames) {
    this(exprs, outputColumnNames, true, null);
  }

  public joinDesc(final Map<Byte, List<exprNodeDesc>> exprs, ArrayList<String> outputColumnNames, final joinCond[] conds) {
    this(exprs, outputColumnNames, false, conds);
  }
  
  public Map<Byte, List<exprNodeDesc>> getExprs() {
    return this.exprs;
  }
  
  public Map<String, Byte> getReversedExprs() {
    return reversedExprs;
  }

  public void setReversedExprs(Map<String, Byte> reversed_Exprs) {
    this.reversedExprs = reversed_Exprs;
  }
  
  @explain(displayName="condition expressions")
  public Map<Byte, String> getExprsStringMap() {
    if (getExprs() == null) {
      return null;
    }
    
    LinkedHashMap<Byte, String> ret = new LinkedHashMap<Byte, String>();
    
    for(Map.Entry<Byte, List<exprNodeDesc>> ent: getExprs().entrySet()) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      if (ent.getValue() != null) {
        for(exprNodeDesc expr: ent.getValue()) {
          if (!first) {
            sb.append(" ");
          }
          
          first = false;
          sb.append("{");
          sb.append(expr.getExprString());
          sb.append("}");
        }
      }
      ret.put(ent.getKey(), sb.toString());
    }
    
    return ret;
  }
  
  public void setExprs(final Map<Byte, List<exprNodeDesc>> exprs) {
    this.exprs = exprs;
  }
  
  @explain(displayName="outputColumnNames")
  public java.util.ArrayList<java.lang.String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(
      java.util.ArrayList<java.lang.String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  public boolean getNoOuterJoin() {
    return this.noOuterJoin;
  }

  public void setNoOuterJoin(final boolean noOuterJoin) {
    this.noOuterJoin = noOuterJoin;
  }

  @explain(displayName="condition map")
  public List<joinCond> getCondsList() {
    if (conds == null) {
      return null;
    }

    ArrayList<joinCond> l = new ArrayList<joinCond>();
    for(joinCond cond: conds) {
      l.add(cond);
    }

    return l;
  }

  public joinCond[] getConds() {
    return this.conds;
  }

  public void setConds(final joinCond[] conds) {
    this.conds = conds;
  }
  
  //Added by Brantzhang for patch HIVE-863 Begin
  /**
   * The order in which tables should be processed when joining
   * 
   * @return Array of tags
   */
  public Byte[] getTagOrder() {
    return tagOrder;
  }
  
  /**
   * The order in which tables should be processed when joining
   * 
   * @param tagOrder Array of tags
   */
  public void setTagOrder(Byte[] tagOrder) {
    this.tagOrder = tagOrder;
  }
  //Added by Brantzhang for patch HIVE-863 End
  
  //Removed by Brantzhang for patch HIVE-1037 Begin
  /*
  //Added by Brantzhang for patch HIVE-963 Begin
  public void initSpillTables() {
	spillTableDesc = new tableDesc[exprs.size()];
	for (int tag = 0; tag < exprs.size(); tag++) {
	  List<exprNodeDesc> valueCols = exprs.get((byte)tag);
	  int columnSize = valueCols.size();
	  StringBuffer colNames = new StringBuffer();
	  StringBuffer colTypes = new StringBuffer();
	  List<exprNodeDesc> newValueExpr = new ArrayList<exprNodeDesc>();
	  if ( columnSize <= 0 ) {
	    spillTableDesc[tag] = null;
	    continue;
	  }
	  for (int k = 0; k < columnSize; k++) {
	    TypeInfo type = valueCols.get(k).getTypeInfo();
	  	String newColName = tag + "_VALUE_" + k; // any name, it does not matter.
	    colNames.append(newColName);
	    colNames.append(',');
	    colTypes.append(valueCols.get(k).getTypeString());
	    colTypes.append(',');
	  }
	  // remove the last ','
	  colNames.setLength(colNames.length()-1);
	  colTypes.setLength(colTypes.length()-1);
	  spillTableDesc[tag] = 
	    new tableDesc(LazyBinarySerDe.class,
	            SequenceFileInputFormat.class,
	            HiveSequenceFileOutputFormat.class, 
	            Utilities.makeProperties(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "" + Utilities.ctrlaCode,
	                  org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS, colNames.toString(),
	                  org.apache.hadoop.hive.serde.Constants.LIST_COLUMN_TYPES, colTypes.toString()));
	}
  }
	    
  public tableDesc getSpillTableDesc(int i) {
	return spillTableDesc[i];
  }
	    
  public tableDesc[] getSpillTableDesc() {
	return spillTableDesc;
  }
	    
  public void setSpillTableDesc(tableDesc[] std) {
	spillTableDesc = std;
  }
  //Added by Brantzhang for patch HIVE-963 End
  */
  //Removed by Brantzhang for patch HIVE-1037 Begin

}
