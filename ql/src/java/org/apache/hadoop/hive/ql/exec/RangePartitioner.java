package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.IntWritable;

/**
 * Return the target partition that the row falls in.
 */
public class RangePartitioner implements Partitioner {
  
  ExprNodeEvaluator[] partValueSpaces;
  int defaultPart;
  
  public RangePartitioner(ExprNodeEvaluator[] partValueSpaces, int defaultPart) {
    this.partValueSpaces = partValueSpaces;
    this.defaultPart = defaultPart;
  }
  
  public int getPartition(Object row) throws HiveException {
    int low = 0;
    int high = defaultPart < 0 ? partValueSpaces.length - 1 : partValueSpaces.length - 2;
    
    while (low <= high) {
      int mid = (low + high) >>> 1;
      
      IntWritable cmpWritable = (IntWritable)partValueSpaces[mid].evaluate(row);
      // if we meet a null row in the partition field, 
      // we put it in the default partition
      if (cmpWritable == null) {
        if (defaultPart == -1) 
          throw new HiveException("No default partition defined to accept a null part key.");
        return defaultPart;
      }
      
      int cmp = cmpWritable.get();
      if (cmp < 0) 
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else
        return mid + 1;
    }
    
    return low;
  }
  
}
