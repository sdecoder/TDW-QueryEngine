package org.apache.hadoop.hive.ql.exec;

import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

//Added by Brantzhang for Hash Partition
public class HashPartitioner implements Partitioner {

  ObjectInspector partKeyInspector;
  ExprNodeEvaluator partKeyEvaluator;
  int numHashPartitions;
  
  public HashPartitioner(ObjectInspector partKeyInspector,
      ExprNodeEvaluator partKeyEvaluator, int numHashPartitions) {
    this.partKeyInspector = partKeyInspector;
    this.partKeyEvaluator = partKeyEvaluator;
    this.numHashPartitions = numHashPartitions;
  }
  
  /**
   * which partition the row will be saved to
   */
  public int getPartition(Object row) throws HiveException {
    Object partKey = this.partKeyEvaluator.evaluate(row);
    partKey = ((PrimitiveObjectInspector)partKeyInspector).getPrimitiveWritableObject(partKey);
    // if we meet a null row in the partition field, how to handle it ?
    if (partKey == null) {//如果用于hash分区的列有空值，则统一发送到该分区
    	//throw new HiveException("No default partition defined to accept a null part key.");
    	return 0;
    }
    
    return (partKey.hashCode() & Integer.MAX_VALUE) % numHashPartitions;
  }

}