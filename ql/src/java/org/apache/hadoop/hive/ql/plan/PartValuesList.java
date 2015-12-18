package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;

@explain(displayName="Partition values list")
public class PartValuesList implements Serializable {

  private static final long serialVersionUID = 1L;
  private ArrayList<String> values;
  
  public PartValuesList() { }
  public PartValuesList(
      final ArrayList<String> values) {
    this.values = values;
  }
  
  @explain(displayName="values list")
  public ArrayList<String> getValues() {
    return values;
  }
  
  public void setValues(final ArrayList<String> values) {
    this.values = values;
  }
  
}
