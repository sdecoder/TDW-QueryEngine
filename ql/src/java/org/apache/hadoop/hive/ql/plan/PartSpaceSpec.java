package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@explain(displayName="Partition Space Spec")
public class PartSpaceSpec implements Serializable {

  private static final long serialVersionUID = 1L;
  private Map<String, PartValuesList> partSpace;
  
  public PartSpaceSpec() { }
  public PartSpaceSpec(
      final Map<String, PartValuesList> partSpace) {
    this.partSpace = partSpace;
  }
  
  @explain(displayName="Partition space")
  public Map<String, PartValuesList> getPartSpace() {
    return this.partSpace;
  }
  
  public void setPartSpace(final Map<String, PartValuesList> partSpace) {
    this.partSpace = partSpace;
  }
  
  public static PartSpaceSpec convertToPartSpaceSpec(
      Map<String, List<String>> partSpaces) {
    Map<String, PartValuesList> newPartSpaces = 
      new LinkedHashMap<String, PartValuesList>();
    
    for (Entry<String, List<String>> entry : partSpaces.entrySet()) {
      ArrayList<String> values = new ArrayList<String>();
      values.addAll(entry.getValue());
      PartValuesList pvList = new PartValuesList(values);
      newPartSpaces.put(entry.getKey(), pvList);
    }
    return new PartSpaceSpec(newPartSpaces);
  }
  
}
