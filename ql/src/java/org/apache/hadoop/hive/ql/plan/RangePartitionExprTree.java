package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;

public class RangePartitionExprTree implements Serializable {

  private static final long serialVersionUID = 1L;
  private ArrayList<exprNodeDesc> exprs;
  
  public RangePartitionExprTree() { }
  public RangePartitionExprTree(
      final ArrayList<exprNodeDesc> exprs) {
    this.exprs = exprs;
  }
  
  public ArrayList<exprNodeDesc> getExprs() {
    return this.exprs;
  }
  
  public void setExprs(ArrayList<exprNodeDesc> exprs) {
    this.exprs = exprs;
  }
  
}
