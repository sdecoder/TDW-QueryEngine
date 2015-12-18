package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.PartValuesList;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.plan.partitionSinkDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * <p>
 * <i>PartitonerOperator</i> is just used to forward the rows to its
 * target partition <i>FileSinkOperator</i>.
 * </p>
 */
public class PartitionerOperator extends TerminalOperator <partitionSinkDesc> 
    implements Serializable {

  private static final long serialVersionUID = 1L;
  
  transient protected ExprNodeEvaluator[] partKeyFields;
  transient protected ObjectInspector[] partKeyObjectInspectors;
  transient protected Object[] partKeyObjects;
  
  transient protected Partitioner[] partitioners;
  transient protected int[] targetPartitions;
  transient protected int[] numPartitionsEachLevel;
  transient protected int numPartKeys;
  
  transient protected String[][] partNames;
  transient protected FileSinkOperator[][] fsOps;
  
  transient protected Configuration config;
  transient protected partitionSinkDesc partSinkDesc;
  
  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    this.config = hconf;
    
    partSinkDesc = (partitionSinkDesc)conf;
    
    assert(inputObjInspectors.length == 1);
    ObjectInspector rowInspector = inputObjInspectors[0];
    
    System.out.println("row inspector is " + rowInspector);
    
    // init all the partition key fields
    numPartKeys = partSinkDesc.getPartKeys().size();
    partKeyFields = new ExprNodeEvaluator[numPartKeys];
    partKeyObjectInspectors = new ObjectInspector[numPartKeys];
    partKeyObjects = new Object[numPartKeys];
    partNames = new String[numPartKeys][];
    partitioners = new Partitioner[numPartKeys];
    numPartitionsEachLevel = new int[numPartKeys];
    
    System.out.println("Num partition keys is " + numPartKeys + ", " + partSinkDesc.getPartTypes());
    System.out.println("File Sink descriptor : " + partSinkDesc);
    
    for (int i=0; i<numPartKeys; i++) {
      partKeyFields[i] = ExprNodeEvaluatorFactory.get(partSinkDesc.getPartKeys().get(i));
      partKeyObjectInspectors[i] = partKeyFields[i].initialize(rowInspector);
      partKeyObjects[i] = null;
      
      numPartitionsEachLevel[i] = partSinkDesc.getPartSpaces().get(i).getPartSpace().size();
      
      System.out.println("Partiton level " + i + " has " + numPartitionsEachLevel[i] + " partitons.");
      
      partNames[i] = new String[numPartitionsEachLevel[i]];
      int j=0;
      for (Entry<String, PartValuesList> entry : partSinkDesc.getPartSpaces().get(i).getPartSpace().entrySet()) {
        partNames[i][j] = entry.getKey();
        ++j;
      }
      
      //Modified by Brantzhang for Hash Partition Begin
      String partitionType = partSinkDesc.getPartTypes().get(i);
      if (partitionType.equalsIgnoreCase("LIST")) {
        partitioners[i] = getListPartitioner(i);
      } else if (partitionType.equalsIgnoreCase("RANGE")) {
        partitioners[i] = getRangePartitioner(i);
      }else if (partitionType.equalsIgnoreCase("HASH")) {
        partitioners[i] = getHashPartitioner(i);
      } else {
        throw new HiveException("Unknow partition type.");
      }
      //Modified by Brantzhang for Hash Partition End
    }
    
    // initialize the file-sink operator
    fsOps = new FileSinkOperator[numPartitionsEachLevel[0]][];
    int partitionsLevel2 = numPartKeys == 1 ? 1 : numPartitionsEachLevel[1];
    for (int i=0; i<numPartitionsEachLevel[0]; i++) {
      fsOps[i] = new FileSinkOperator[partitionsLevel2];
    }
    
    targetPartitions = new int[2];
    Arrays.fill(targetPartitions, 0);
    initializeChildren(hconf);
  }
  
  private Partitioner getRangePartitioner(int level) throws HiveException {
    // get the part key type
//    TypeInfo partKeyType = TypeInfoFactory.getPrimitiveTypeInfo(partSinkDesc.getPartTypeInfos().get(level));
//    if (partKeyType.getCategory() != Category.PRIMITIVE) {
//      throw new HiveException("Current just accept primitive type as partition key.");
//    } 
    
    ExprNodeEvaluator[] partEvaluators = 
      new ExprNodeEvaluator[partSinkDesc.getPartSpaces().get(level).getPartSpace().size()];
    
    // initialize an primitive string object inspector
//    ObjectInspector stringOI = 
//      PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
//   
//    ObjectInspector valueOI =
//      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
//          ((PrimitiveTypeInfo)partKeyType).getPrimitiveCategory());
//    
    int defaultPart = -1, partition = 0;
    Map<String, PartValuesList> partSpace = partSinkDesc.getPartSpaces().get(level).getPartSpace();
    ArrayList<exprNodeDesc> exprTree = partSinkDesc.getExprTrees().get(level).getExprs();
//    for (Map.Entry<String, PartValuesList> entry : partSpace.entrySet()) {
//      String partName = entry.getKey();
//      List<String> partValues = entry.getValue().getValues();
      
    for (String partName : partSpace.keySet()) {
      if (partName.equalsIgnoreCase("default")) {
        defaultPart = partition;
        ++partition;
        continue;
      }
      
//      ObjectInspectorConverters.Converter converter = 
//        ObjectInspectorConverters.getConverter(stringOI, valueOI);
//      Object pv = converter.convert(partValues.get(0));
//      pv = ((PrimitiveObjectInspector)valueOI).getPrimitiveJavaObject(pv);
//      
//      exprNodeDesc partValueDesc = new exprNodeConstantDesc(partKeyType, pv);
//      exprNodeDesc rowDesc = partSinkDesc.getPartKeys().get(level);
//      rowDesc.setTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo(rowDesc.getTypeInfo().getTypeName()));
//      exprNodeDesc compareDesc = 
//        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("comparison", partValueDesc, rowDesc);
//      
      exprNodeDesc partKeyDesc = exprTree.get(partition);
      if (partKeyDesc == null) {
        throw new HiveException("No partition values defined in partition " + partName);
      }
      partEvaluators[partition] = ExprNodeEvaluatorFactory.get(exprTree.get(partition));
      partEvaluators[partition].initialize(inputObjInspectors[0]);
      
      ++partition;
    }
    
    System.out.println("Total partiton is " + partition);

    return new RangePartitioner(partEvaluators, defaultPart);
  }
  
  private Partitioner getListPartitioner(int level) throws HiveException {
    // get the part key type
    TypeInfo partKeyType = TypeInfoFactory.getPrimitiveTypeInfo(partSinkDesc.getPartTypeInfos().get(level));
    if (partKeyType.getCategory() != Category.PRIMITIVE) {
      throw new HiveException("Current just accept primitive type as partition key.");
    }
    
    Map<Object, Integer> partitionValueSpaces = 
      new HashMap<Object, Integer>();
    
    // initialize an primitive string object inspector
    ObjectInspector stringOI = 
      PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
   
    ObjectInspector valueOI =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          ((PrimitiveTypeInfo)partKeyType).getPrimitiveCategory());
    
    int partition = 0, defaultPart = -1;
    Map<String, PartValuesList> partSpace = partSinkDesc.getPartSpaces().get(level).getPartSpace();
    for (Map.Entry<String, PartValuesList> entry : partSpace.entrySet()) {
      String partName = entry.getKey();
      List<String> partValues = entry.getValue().getValues();
      
      if (partName.equalsIgnoreCase("default")) 
        defaultPart = partition;
      
      for (String value : partValues) {
        ObjectInspectorConverters.Converter converter =
          ObjectInspectorConverters.getConverter(stringOI, valueOI);
        
        Object pv = converter.convert(value);
        partitionValueSpaces.put(pv, partition);
      }
      
      ++partition;
    }
    
    return new ListPartitioner(partKeyObjectInspectors[level], partKeyFields[level], partitionValueSpaces, defaultPart);
  }
  
  //Added by Brantzhang for Hash Partition Begin
  private Partitioner getHashPartitioner(int level) throws HiveException {
	    Map<String, PartValuesList> partSpace = partSinkDesc.getPartSpaces().get(level).getPartSpace();
	    	    
	    return new HashPartitioner(partKeyObjectInspectors[level], partKeyFields[level], partSpace.size());
  }
  //Added by Brantzhang for hash partition End

  @Override
  public void process(Object row, int tag) throws HiveException {
    // evaluate the row to get the target partition information
    for (int i=0; i<numPartKeys; i++) {
      targetPartitions[i] = partitioners[i].getPartition(row);
    }
    
    FileSinkOperator op = fsOps[targetPartitions[0]][targetPartitions[1]];
    if (op == null) {
      int partId = getPartitionId(targetPartitions[0], targetPartitions[1]);
      String dirName = conf.getDirName() + "/" + partNames[0][targetPartitions[0]];
      if (numPartKeys == 2) {
        dirName += "/" + partNames[1][targetPartitions[1]];
      }
      fileSinkDesc fsDesc = new fileSinkDesc(dirName, conf.getTableInfo(), false, partId);
      op = (FileSinkOperator) OperatorFactory.get(fsDesc);
      /**
      List<Operator<? extends Serializable>> parents = op.getParentOperators();
      if (parents == null) {
        parents = new ArrayList<Operator<? extends Serializable>>();
      }
      parents.add(this);
      op.setParentOperators(parents);
      **/
      // this.getChildOperators().add(op);
      
      LOG.info("Init a FileSink Operator for partition path " + dirName);
      // LOG.info("Its parents state is " + op.getParentOperators().get(0).state);
      op.initialize(config, inputObjInspectors);
      // op.initializeOp(config);
      fsOps[targetPartitions[0]][targetPartitions[1]] = op;
    }
    
    op.process(row, tag);
  }
  
  // just get a id to identify a partition
  private int getPartitionId(int level1, int level2) {
    return level1 << 16 | level2;
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    for(FileSinkOperator[] ops : fsOps) {
      for (FileSinkOperator op : ops) {
        if (op != null)
          op.closeOp(abort);
      }
    }
  }

  @Override
  public void jobClose(Configuration hconf, boolean success)
      throws HiveException {
    try {
      if (conf != null) {
        String specTablePath = conf.getDirName();
        numPartKeys = conf.getPartKeys().size();
        partNames = new String[numPartKeys][];
        for (int i=0; i<numPartKeys; i++) {
          partNames[i] = new String[conf.getPartSpaces().get(i).getPartSpace().size()];
          int j=0;
          for (Entry<String, PartValuesList> entry : conf.getPartSpaces().get(i).getPartSpace().entrySet()) {
            partNames[i][j] = entry.getKey();
            ++j;
          }
        }
        
        for (int i=0; i<partNames[0].length; i++) {
          String specPartPath = specTablePath + "/" + partNames[0][i];
          if (numPartKeys == 2) {
            for (int j=0; j<partNames[1].length; j++) {
              partitionClose(specPartPath + "/" + partNames[1][j], success, hconf);
            }
          } else {
            partitionClose(specPartPath, success, hconf);
          }
        }
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
    super.jobClose(hconf, success);
  }
  
  private void partitionClose(String specPath, boolean success, Configuration hconf) 
  throws IOException, HiveException {
    FileSystem fs = (new Path(specPath)).getFileSystem(hconf);
    Path tmpPath = Utilities.toTempPath(specPath);
    Path intermediatePath = new Path(tmpPath.getParent(), tmpPath.getName() + ".intermediate");
    Path finalPath = new Path(specPath);
    if(success) {
      if(fs.exists(tmpPath)) {
        // Step1: rename tmp output folder to intermediate path. After this
        // point, updates from speculative tasks still writing to tmpPath 
        // will not appear in finalPath.
        LOG.info("Moving tmp dir: " + tmpPath + " to: " + intermediatePath);
        Utilities.rename(fs, tmpPath, intermediatePath);
        // Step2: remove any tmp file or double-committed output files
        Utilities.removeTempOrDuplicateFiles(fs, intermediatePath);
        // Step3: move to the file destination
        LOG.info("Moving tmp dir: " + intermediatePath + " to: " + finalPath);
        Utilities.renameOrMoveFiles(fs, intermediatePath, finalPath);
      }
    } else {
      fs.delete(tmpPath, true);
    }
  }
  
  /**
   * @return the name of the operator
   */
  public String getName() {
    return new String("FS");
  }

}
