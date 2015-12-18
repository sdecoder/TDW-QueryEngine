package org.apache.hadoop.hive.metastore.model;

import java.util.List;

/**
 * Current it just contain a list of values.
 */
public class MPartSpace {
  
  private List<String> values;
  
  public MPartSpace() { }
  
  public MPartSpace(List<String> values_) {
    this.values = values_;
  }
  
  public void setValues(List<String> values_) {
    this.values = values_;
  }
  
  public List<String> getValues() {
    return this.values;
  }
  
}
