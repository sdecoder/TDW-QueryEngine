package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public interface Partitioner {

  public int getPartition(Object row) throws HiveException;
  
}
