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

package org.apache.hadoop.hive.ql.optimizer.ppr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.TablePartition;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFOPAnd;
import org.apache.hadoop.hive.ql.udf.UDFOPEqual;
import org.apache.hadoop.hive.ql.udf.UDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.UDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.UDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.UDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.UDFOPNot;
import org.apache.hadoop.hive.ql.udf.UDFOPOr;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * The transformation step that does partition pruning.该类完成分区修剪
 *
 */
public class PartitionPruner implements Transform {

  // The log
  private static final Log LOG = LogFactory.getLog("hive.ql.optimizer.ppr.PartitionPruner");
 
  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.optimizer.Transform#transform(org.apache.hadoop.hive.ql.parse.ParseContext)
   */
  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    // create a the context for walking operators
    OpWalkerCtx opWalkerCtx = new OpWalkerCtx(pctx.getOpToPartPruner());
    
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", "(TS%FIL%)|(TS%FIL%FIL%)"), 
                OpProcFactory.getFilterProc());

    // The dispatcher fires the processor corresponding to the closest matching rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(OpProcFactory.getDefaultProc(), opRules, opWalkerCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);
    
    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    pctx.setHasNonPartCols(opWalkerCtx.getHasNonPartCols());

    return pctx;
  }

  private static class PartitionPrunerContext {
    ObjectInspector partKeysInpsector;
    int numPartitionLevels;
    List<String> partKeyNames;
    Map<String, ObjectInspectorConverters.Converter> partKeyConverters;
    Map<String, Set<String>> partKeyToTotalPartitions;
    Map<String, org.apache.hadoop.hive.metastore.api.Partition> partitionMap;
  }

  // Added by guosijie
  // Date: 2010-03-30
  //   as the PartitionPruner#prune method is used over the optimizer,
  //   so we do not change its interface while changing the different
  //   implementation of the method.
  //   Here, we walk through the whole where-clause and generate the
  //   the target partitions that the where-clause hits.
  //
  //   The rules that we walks through the expression are defined as below:
  //   1) Predicates like "x > 'value'"
  //      a. if x is the partition key, return the partitions that it hits.
  //      b. if x is not the partition key, just return a empty list of partitions.
  //   2) Operations like AND, OR and Not, do the logic operation of these two partition list
  //      to generate a new partition list.
  //   3) Other unkown udf of non-partition keys are just return an empty list of partitions.
  //   4) Unknown udf of partition keys are just return all the partitions.
  //   
  private static List<exprNodeDesc> evaluatePrunedExpression(
      exprNodeDesc prunerExpr, PartitionPrunerContext ppc,
      Map<String, Set<String>> partColToTargetPartitions) throws HiveException {
    // We recursively evaluate the pruner expression.
    // Base cases:
    // when we encounter a column desc(it should be the partition key!!! it is ensured by
    // ASTPartitionPruner), we do nothing and just return the exprNodeColumnDesc to tell
    // its parents that there is a partition key contained in its children.
    // when we encounter a constant, we just return false and let it's parents handle it.
    // when we encounter a function, we collect the return Boolean's from all its children
    // to see if there are any partition keys. If yes, we evaluate the expression to get
    // the target partitions; otherwise, we do nothing and return false.
    boolean isFunction = prunerExpr instanceof exprNodeFuncDesc;
    if (!isFunction) {
      if (prunerExpr instanceof exprNodeColumnDesc) { // it's a partition key
        List<exprNodeDesc> partExprList = new ArrayList<exprNodeDesc>();
        partExprList.add(prunerExpr);
        return partExprList;
      } else {
        return null;
      }
    }
    
    // here we encounter a function
    List<exprNodeDesc> children = prunerExpr.getChildren();
    List<Map<String, Set<String>>> childrenPartitions =
      new ArrayList<Map<String, Set<String>>>();
    List<List<exprNodeDesc>> partDescsFromChildren = 
      new ArrayList<List<exprNodeDesc>>();
    List<exprNodeDesc> returnPartDescs = new ArrayList<exprNodeDesc>();
    
    // evaluate all its children
    for (exprNodeDesc child : children) {
      Map<String, Set<String>> partMap = new HashMap<String, Set<String>>();
      List<exprNodeDesc> results = evaluatePrunedExpression(child, ppc, partMap);
      partDescsFromChildren.add(results);
      if (results != null) {
        // here we need to deduplicate the same exprNodeDescs
        // as exprNodeDesc just use exprNodeDesc#isSame method
        // to do the comparison, so we can't use the existed
        // SET collection to do the deduplicate.
        addAndDeduplicate(returnPartDescs, results);
      }
      childrenPartitions.add(partMap);
    }
    
    // how many partition expressions exist in this expression tree
    int numPartExprs = returnPartDescs.size();
    // Evaluate the Operation AND, OR and NOT
    Class UDFClz = ((exprNodeFuncDesc)prunerExpr).getUDFMethod().getDeclaringClass();
    if (UDFClz.equals(UDFOPOr.class)) {
      // Or the part maps from all its children
      for (String partKeyName : ppc.partKeyNames) {
        Set<String> leftParts = childrenPartitions.get(0).get(partKeyName);
        Set<String> rightParts = childrenPartitions.get(1).get(partKeyName);
        if (leftParts != null && rightParts == null) {
          rightParts = ppc.partKeyToTotalPartitions.get(partKeyName);
        } else if (rightParts != null && leftParts == null) {
          leftParts = rightParts;
          rightParts = ppc.partKeyToTotalPartitions.get(partKeyName);
        } else if (rightParts == null && leftParts == null) {
          continue;
        }
        leftParts.addAll(rightParts);
        partColToTargetPartitions.put(partKeyName, leftParts);
      }
    } else if (UDFClz.equals(UDFOPAnd.class)) {
      // And the part maps from all its children
      for (Map<String, Set<String>> childPartMap : childrenPartitions) {
        for (Entry<String, Set<String>> entry : childPartMap.entrySet()) {
          Set<String> partitions = partColToTargetPartitions.get(entry.getKey());
          if (partitions == null) {
            partitions = entry.getValue();
            partColToTargetPartitions.put(entry.getKey(), partitions);
          } else
            partitions.retainAll(entry.getValue());
        }
      }
    } else if (UDFClz.equals(UDFOPNot.class)) {
      assert(childrenPartitions.size() < 2);
      // Not the part maps from its children
      if (childrenPartitions.size() == 1) {
        Map<String, Set<String>> partMap = childrenPartitions.get(0);
        for (Entry<String, Set<String>> entry : partMap.entrySet()) {
          Set<String> targetPartitions = new TreeSet<String>();
          Set<String> partitions = entry.getValue();
          Set<String> totalPartitions = ppc.partKeyToTotalPartitions.get(entry.getKey());
          for (String i : totalPartitions) {
            if (!partitions.contains(i)) 
              targetPartitions.add(i);
          }
          
          partColToTargetPartitions.put(entry.getKey(), targetPartitions);
        }
      }
    }
    
    // Here we encounter the comparison predicates.
    
    // if means more than 2 different part expression in the current
    // expression tree. 
    // such as A.x + A.y > 10 or A.x > A.y, we just skip it.
    // 
    // The expressions like A.x + A.x > 10, we treat them as
    // expressions that contain only one part expression.
    boolean isEqualUDF = UDFClz.equals(UDFOPEqual.class);
    if ((UDFClz.equals(UDFOPEqual.class) ||
         UDFClz.equals(UDFOPEqualOrGreaterThan.class) ||
         UDFClz.equals(UDFOPEqualOrLessThan.class) ||
         UDFClz.equals(UDFOPGreaterThan.class) ||
         UDFClz.equals(UDFOPLessThan.class)) && numPartExprs == 1) {
      assert(partDescsFromChildren.size() == 2);
      exprNodeFuncDesc funcDesc = (exprNodeFuncDesc) prunerExpr;
      
      // we need to make sure that the left child contains the 
      // part expression
      if (partDescsFromChildren.get(0) == null ||
          partDescsFromChildren.get(0).size() == 0) {
        // xchange the children
        exprNodeDesc temp = funcDesc.getChildExprs().get(0);
        funcDesc.getChildExprs().set(0, funcDesc.getChildExprs().get(1));
        funcDesc.getChildExprs().set(1, temp);
        
        String newMethodName = null;
        if (UDFClz.equals(UDFOPEqualOrGreaterThan.class)) {
          newMethodName = "<=";
        } else if (UDFClz.equals(UDFOPEqualOrLessThan.class)) {
          newMethodName = ">=";
        } else if (UDFClz.equals(UDFOPGreaterThan.class)) {
          newMethodName = "<";
        } else if (UDFClz.equals(UDFOPLessThan.class)) {
          newMethodName = ">";
        }
        
        if (newMethodName != null) {
          funcDesc.setMethodName(newMethodName);
          funcDesc.setUDFClass(FunctionRegistry.getUDFClass(newMethodName));
          ArrayList<TypeInfo> argumentTypeInfos = new ArrayList<TypeInfo>(funcDesc.getChildExprs().size());
          for(int i=0; i<children.size(); i++) {
            exprNodeDesc child = funcDesc.getChildExprs().get(i);
            argumentTypeInfos.add(child.getTypeInfo());
          }
          funcDesc.setUDFMethod(FunctionRegistry.getUDFMethod(newMethodName, argumentTypeInfos));
        }
      }
      
      exprNodeColumnDesc partColDesc = (exprNodeColumnDesc) returnPartDescs.get(0);
      String partColName = partColDesc.getColumn();
      org.apache.hadoop.hive.metastore.api.Partition part =
        ppc.partitionMap.get(partColName);
      boolean isRangePartition = part.getParType().equalsIgnoreCase("RANGE");
      boolean isHashPartition = part.getParType().equalsIgnoreCase("HASH");//Added by Brantzhang for Hash Partition
      
      // OK. Now we ensure the arguments' order. But we need to rewrite
      // the expression like '>=' '>' to '<' '<='. So we make the partition evaluation
      // more easy
      boolean exprRewritten = false;
      boolean containEqual = false;
      Set<String> tempPartitions; 
      if (isRangePartition) {
        exprNodeFuncDesc boundaryCheckerDesc = null;
        if (UDFClz.equals(UDFOPEqualOrGreaterThan.class) ||
            UDFClz.equals(UDFOPGreaterThan.class)) {
          boundaryCheckerDesc = (exprNodeFuncDesc) TypeCheckProcFactory.getDefaultExprProcessor()
            .getFuncExprNodeDesc("=", funcDesc.getChildExprs());
          
          List<exprNodeDesc> argDescs = new ArrayList<exprNodeDesc>();
          argDescs.add(funcDesc);
          funcDesc = (exprNodeFuncDesc) TypeCheckProcFactory.getDefaultExprProcessor()
            .getFuncExprNodeDesc("not", argDescs);
          exprRewritten = true;
          if (UDFClz.equals(UDFOPGreaterThan.class)) 
            containEqual = true;
        } else if (UDFClz.equals(UDFOPEqual.class)) {
          funcDesc = (exprNodeFuncDesc) TypeCheckProcFactory.getDefaultExprProcessor()
            .getFuncExprNodeDesc("<=", funcDesc.getChildExprs());
        }

        tempPartitions = evaluateRangePartition(funcDesc, boundaryCheckerDesc, ppc.partKeysInpsector,
            ppc.partKeyConverters.get(partColName), part.getParSpaces(), 
            part.getLevel(), ppc.numPartitionLevels, isEqualUDF, exprRewritten, containEqual);
      }
      //Added by Brantzhang for hash partition Begin
      else if (isHashPartition){
    	  
    	  tempPartitions = evaluateHashPartition(funcDesc, ppc.partKeysInpsector,
    	            ppc.partKeyConverters.get(partColName), part.getParSpaces(), 
    	            part.getLevel(), ppc.numPartitionLevels);
      }
      //Added by Brantzhang for hash partition End
      else {
        tempPartitions = evaluateListPartition(funcDesc, ppc.partKeysInpsector,
            ppc.partKeyConverters.get(partColName), part.getParSpaces(), 
            part.getLevel(), ppc.numPartitionLevels);
      }

      Set<String> targetPartitions = partColToTargetPartitions.get(partColDesc.getColumn());
      if (targetPartitions == null) {
        targetPartitions = tempPartitions;
        partColToTargetPartitions.put(partColDesc.getColumn(), targetPartitions);
      } else {
        targetPartitions.addAll(tempPartitions);
      }
        
    }

    // for other unknown udf class, we just fall through
    return returnPartDescs;
  }
  
  private static void addAndDeduplicate(List<exprNodeDesc> target, List<exprNodeDesc> source) {
    if (target.size() == 0) {
      target.addAll(source);
      return;
    }
    for (exprNodeDesc srcDesc : source) {
      boolean added = false;
      for (exprNodeDesc targetDesc : source) {
        if (srcDesc.isSame(targetDesc)) {
          added = true;
          break;
        }
      }
      if (added)
        target.add(srcDesc);
    }
  }
  
  private static Set<String> evaluateRangePartition(exprNodeDesc desc, exprNodeDesc boundaryCheckerDesc, 
      ObjectInspector partKeysInspector, ObjectInspectorConverters.Converter converter, 
      Map<String, List<String>> partSpace, int level, int numPartitionLevel,
      boolean isEqualFunc, boolean exprRewritten, boolean containsEqual) 
      throws HiveException {
    ArrayList<Object> partObjects = new ArrayList<Object>(numPartitionLevel);
    for (int i=0; i<numPartitionLevel; i++) {
      partObjects.add(null);
    }
    // if we are here, we assume that its left child contain the
    // partition key and its right child just a constant
    
    ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(desc);
    ObjectInspector evaluateResultOI = evaluator.initialize(partKeysInspector);
    
    Set<String> targetPartitions = new LinkedHashSet<String>();
    
    for (Entry<String, List<String>> entry : partSpace.entrySet()) {
      List<String> partValues = entry.getValue();
      
      //allison:
      if(partValues == null || partValues.size() == 0){//it is default partition
    	  assert (entry.getKey().equalsIgnoreCase("default"));
    	  targetPartitions.add(entry.getKey());
    	  break;
      }
      //end
      
      Object pv = converter.convert(partValues.get(0));
      partObjects.set(level, pv);
      Object evaluateResultO = evaluator.evaluate(partObjects);
      Boolean r = (Boolean) ((PrimitiveObjectInspector)evaluateResultOI).getPrimitiveJavaObject(evaluateResultO);
      if (r == null) { // TODO: here we just return an empty partition collection
        targetPartitions.clear();
        break;
      } else {
        if (Boolean.TRUE.equals(r)) {
          if (!isEqualFunc)
            targetPartitions.add(entry.getKey());
        } else if (Boolean.FALSE.equals(r)) {
          if (boundaryCheckerDesc != null) {
            ExprNodeEvaluator boundaryChecker = ExprNodeEvaluatorFactory.get(boundaryCheckerDesc);
            ObjectInspector boundaryCheckerOI = boundaryChecker.initialize(partKeysInspector);
            Boolean isBoundary = (Boolean) ((PrimitiveObjectInspector)boundaryCheckerOI).getPrimitiveJavaObject(boundaryChecker.evaluate(partObjects));
            if (isBoundary == null) {
              targetPartitions.clear();
              break;
            } else {
              if (Boolean.FALSE.equals(isBoundary)) {
                // do nothing
                // just break;
                break;
              }
            }
          }          
          if (!(exprRewritten && containsEqual)) {
            targetPartitions.add(entry.getKey());
          }
          break;
        }
      }
    }
    
    if (exprRewritten) { // expression has been rewritten
      Set<String> oldPartitions = targetPartitions;
      targetPartitions = new TreeSet<String>();
      targetPartitions.addAll(partSpace.keySet());
      Iterator<String> iter = targetPartitions.iterator();
      while (iter.hasNext()) {
        if (oldPartitions.contains(iter.next()))
          iter.remove();
      }
    }
    
    return targetPartitions;
  }
  
  private static Set<String> evaluateListPartition(exprNodeDesc desc,
      ObjectInspector partKeysInpsector, ObjectInspectorConverters.Converter converter,
      Map<String, List<String>> partSpace, int level, int numPartitionLevels) throws HiveException {
    ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(desc);
    ObjectInspector evaluateResultOI = evaluator.initialize(partKeysInpsector);
    
    Set<String> targetPartitions = new TreeSet<String>();
    
    ArrayList<Object> partObjects = new ArrayList<Object>(numPartitionLevels);
    for (int i=0; i<numPartitionLevels; i++) {
      partObjects.add(null);
    }
    for (Entry<String, List<String>> entry : partSpace.entrySet()) {
      List<String> partValues = entry.getValue();
      
      for (String partVal : partValues) {
        Object pv = converter.convert(partVal);
        partObjects.set(level, pv);
        Object evaluateResultO = evaluator.evaluate(partObjects);
        Boolean r = (Boolean) ((PrimitiveObjectInspector)evaluateResultOI).getPrimitiveJavaObject(evaluateResultO);
        if (r == null) {
          return new TreeSet<String>();
        } else {
          if (Boolean.TRUE.equals(r)) {
            targetPartitions.add(entry.getKey());
          }
        }
      }
    }
    
    return targetPartitions;
  }
  
  
  // End by guosijie
  
  //Added by Brantzhang for hash partition Begin
  //目前该函数只会将所有的Hash分区作为目的分区，不会做其他事
  private static Set<String> evaluateHashPartition(exprNodeDesc desc,
	      ObjectInspector partKeysInpsector, ObjectInspectorConverters.Converter converter,
	      Map<String, List<String>> partSpace, int level, int numPartitionLevels) throws HiveException {
	    
	    Set<String> targetPartitions = new TreeSet<String>();
	    for (Entry<String, List<String>> entry : partSpace.entrySet()) {
	    	targetPartitions.add(entry.getKey());
	    }
	    	    
	    return targetPartitions;
	  }
  //Added by Brantzhang for hash partition End
  
  /**
   * Get the partition list for the table that satisfies the partition pruner
   * condition.
   * 
   * @param tab    the table object for the alias
   * @param prunerExpr  the pruner expression for the alias
   * @param conf   for checking whether "strict" mode is on.
   * @param alias  for generating error message only.
   * @return the partition list for the table that satisfies the partition pruner condition.
   * @throws HiveException
   */
  public static PrunedPartitionList prune(TablePartition tab, exprNodeDesc prunerExpr,
      HiveConf conf, String alias) throws HiveException {
    LOG.trace("Started pruning partiton");
    LOG.trace("tabname = " + tab.getName());
    LOG.trace("prune Expression = " + prunerExpr);
    
    // added by guosijie
    Set<String> targetPartitionPaths = new TreeSet<String>();
    
    if (tab.isPartitioned()) {
      //LOG.info("Begin to prune the partitioned table: "+ tab.getName());
      PartitionPrunerContext ppc = new PartitionPrunerContext();
      
      // also, we need a ObjectInpsectorConverters to convert the partition
      // values from string to target objects
      ppc.partKeyConverters = new HashMap<String, ObjectInspectorConverters.Converter>();
      ppc.partKeyToTotalPartitions = new HashMap<String, Set<String>>();
      ppc.partitionMap = new HashMap<String, org.apache.hadoop.hive.metastore.api.Partition>();
      org.apache.hadoop.hive.metastore.api.Partition partition =
        tab.getTTable().getPriPartition();//一级分区
      ppc.partitionMap.put(partition.getParKey().getName(), partition);
      partition = tab.getTTable().getSubPartition();//二级分区
      if (partition != null) {
        ppc.partitionMap.put(partition.getParKey().getName(), partition);
      }
      ppc.numPartitionLevels = ppc.partitionMap.size();//分区级数
      
      // initialized a partition keys inspector
      ppc.partKeyNames = new ArrayList<String>();
      ArrayList<ObjectInspector> partObjectInspectors =
        new ArrayList<ObjectInspector>();
      for (int i=0; i<ppc.numPartitionLevels; i++) {
        ppc.partKeyNames.add(null);
        partObjectInspectors.add(null);
      }
      for (org.apache.hadoop.hive.metastore.api.Partition part : ppc.partitionMap.values()) {//逐级处理分区
        ppc.partKeyNames.set(part.getLevel(), part.getParKey().getName());
        ObjectInspector partOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            ((PrimitiveTypeInfo)TypeInfoFactory.getPrimitiveTypeInfo(part.getParKey().getType())).getPrimitiveCategory());
        partObjectInspectors.set(part.getLevel(), partOI);
        
        ObjectInspector partStringOI = 
          PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
        ppc.partKeyConverters.put(part.getParKey().getName(), ObjectInspectorConverters.getConverter(partStringOI, partOI));
        ppc.partKeyToTotalPartitions.put(part.getParKey().getName(), part.getParSpaces().keySet());
      }
      // the partition keys inspector is used to evaluate the part objects
      ppc.partKeysInpsector =
        ObjectInspectorFactory.getStandardStructObjectInspector(ppc.partKeyNames, partObjectInspectors);
      
      Map<String, Set<String>> partColToPartitionNames =
        new HashMap<String, Set<String>>();
      evaluatePrunedExpression(prunerExpr, ppc, partColToPartitionNames);//进行分区修剪
      //修剪结果输出
      String priPartColName = ppc.partKeyNames.get(0);//一级分区涉及的列名
      Set<String> priPartNames = partColToPartitionNames.get(priPartColName);//所有一级分区名
      if (priPartNames == null) 
        priPartNames = ppc.partKeyToTotalPartitions.get(priPartColName);
      String subPartColName = null;
      Set<String> subPartNames = null;
      if (ppc.partKeyNames.size() > 1) {//如果有二级分区
        subPartColName = ppc.partKeyNames.get(1);//二级分区涉及的列名
        subPartNames = partColToPartitionNames.get(subPartColName);//所有二级分区名
        if (subPartNames == null)
          subPartNames = ppc.partKeyToTotalPartitions.get(subPartColName);
      }
      
      for (String priPartName : priPartNames) {
        if (subPartNames != null) {
          for (String subPartName : subPartNames) {
            targetPartitionPaths.add(new Path(tab.getPath(), priPartName + "/" + subPartName).toString());
          }
        } else {
          targetPartitionPaths.add(new Path(tab.getPath(), priPartName).toString());
        }
      }
    } else {
      // HACK!!!
      // if the table is not partitioned we put the table path
      // in the target partition path list.
      targetPartitionPaths.add(tab.getPath().toString());
    }
    
    return new PrunedPartitionList(targetPartitionPaths);
    
    // added end
   
    /**
    LinkedHashSet<Partition> true_parts = new LinkedHashSet<Partition>();
    LinkedHashSet<Partition> unkn_parts = new LinkedHashSet<Partition>();
    LinkedHashSet<Partition> denied_parts = new LinkedHashSet<Partition>();

    try {
      StructObjectInspector rowObjectInspector = (StructObjectInspector)tab.getDeserializer().getObjectInspector();
      Object[] rowWithPart = new Object[2];

      if(tab.isPartitioned()) {
        for(String partName: Hive.get().getPartitionNames(tab.getDbName(), tab.getTbl(), (short) -1).get(0)) {
          // Set all the variables here
          LinkedHashMap<String, String> partSpec = Warehouse.makeSpecFromName(partName);
          // Create the row object
          ArrayList<String> partNames = new ArrayList<String>();
          ArrayList<String> partValues = new ArrayList<String>();
          ArrayList<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>();
          for(Map.Entry<String,String>entry : partSpec.entrySet()) {
            partNames.add(entry.getKey());
            partValues.add(entry.getValue());
            partObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector); 
          }
          StructObjectInspector partObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(partNames, partObjectInspectors);

          rowWithPart[1] = partValues;
          ArrayList<StructObjectInspector> ois = new ArrayList<StructObjectInspector>(2);
          ois.add(rowObjectInspector);
          ois.add(partObjectInspector);
          StructObjectInspector rowWithPartObjectInspector = ObjectInspectorFactory.getUnionStructObjectInspector(ois);

          // If the "strict" mode is on, we have to provide partition pruner for each table.  
          if ("strict".equalsIgnoreCase(HiveConf.getVar(conf, HiveConf.ConfVars.HIVEMAPREDMODE))) {
            if (!hasColumnExpr(prunerExpr)) {
              throw new SemanticException(ErrorMsg.NO_PARTITION_PREDICATE.getMsg( 
                  "for Alias \"" + alias + "\" Table \"" + tab.getName() + "\""));
            }
          }
          
          // evaluate the expression tree
          if (prunerExpr != null) {
            ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(prunerExpr);
            ObjectInspector evaluateResultOI = evaluator.initialize(rowWithPartObjectInspector);
            Object evaluateResultO = evaluator.evaluate(rowWithPart);
            Boolean r = (Boolean) ((PrimitiveObjectInspector)evaluateResultOI).getPrimitiveJavaObject(evaluateResultO);
            LOG.trace("prune result for partition " + partSpec + ": " + r);
            if (Boolean.FALSE.equals(r)) {
              if (denied_parts.isEmpty()) {
                Partition part = null;//
                //[TODO]more thinking Hive.get().getPartition(tab, partSpec, Boolean.FALSE);
                denied_parts.add(part);
              }
              LOG.trace("pruned partition: " + partSpec);
            } else {
              Partition part = null;
              //[TODO] more thinking; Hive.get().getPartition(tab, partSpec, Boolean.FALSE);
              if (Boolean.TRUE.equals(r)) {
                LOG.debug("retained partition: " + partSpec);
                true_parts.add(part);
              } else {             
                LOG.debug("unknown partition: " + partSpec);
                unkn_parts.add(part);
              }
            }
          } else {
            // is there is no parition pruning, all of them are needed
            //true_parts.add(Hive.get().getPartition(tab, partSpec, Boolean.FALSE));
            ;
          }
        }
      } else {
        true_parts.addAll(Hive.get().getPartitions(tab.getTbl()));//TODO:more think
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
    **/

    // Now return the set of partitions
    // return new PrunedPartitionList(true_parts, unkn_parts, denied_parts);
  }
  
  /**
   * Whether the expression contains a column node or not.
   */
  public static boolean hasColumnExpr(exprNodeDesc desc) {
    // Return false for null 
    if (desc == null) {
      return false;
    }
    // Return true for exprNodeColumnDesc
    if (desc instanceof exprNodeColumnDesc) {
      return true;
    }
    // Return true in case one of the children is column expr.
    List<exprNodeDesc> children = desc.getChildren();
    if (children != null) {
      for (int i = 0; i < children.size(); i++) {
        if (hasColumnExpr(children.get(i))) {
          return true;
        }
      }
    }
    // Return false otherwise
    return false;
  }
  
}
