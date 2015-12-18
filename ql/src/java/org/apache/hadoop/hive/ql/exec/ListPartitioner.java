package org.apache.hadoop.hive.ql.exec;

import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public class ListPartitioner implements Partitioner {

  ObjectInspector partKeyInspector;
  ExprNodeEvaluator partKeyEvaluator;
  Map<Object, Integer> partValueSpaces;
  int defaultPart;
  
  public ListPartitioner(ObjectInspector partKeyInspector,
      ExprNodeEvaluator partKeyEvaluator,
      Map<Object, Integer> partValueSpaces, int defaultPartition) {
    this.partKeyInspector = partKeyInspector;
    this.partKeyEvaluator = partKeyEvaluator;
    this.partValueSpaces = partValueSpaces;
    this.defaultPart = defaultPartition;
  }
  
  @Override
  public int getPartition(Object row) throws HiveException {
    Object partKey = this.partKeyEvaluator.evaluate(row);
    partKey = ((PrimitiveObjectInspector)partKeyInspector).getPrimitiveWritableObject(partKey);
    // if we meet a null row in the partition field, how to handle it ?
    if (partKey == null) {
      if (defaultPart == -1)
        throw new HiveException("No default partition defined to accept a null part key.");
      return defaultPart;
    }
    
    Integer partition = partValueSpaces.get(partKey);
    if (partition == null) {
      if (defaultPart == -1)
        throw new HiveException("No default partition defined to accept value " + partKey + " (class : " + partKey.getClass() + ").");
      return defaultPart;
    }
    else
      return partition;
  }

}
