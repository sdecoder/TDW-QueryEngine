package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;

/**
 * New partition descriptor.
 */
@explain(displayName="Partition Output Operator")
public class partitionSinkDesc extends fileSinkDesc {

  private static final long serialVersionUID = 1L;
  private ArrayList<exprNodeDesc> partKeys;
  private ArrayList<String> partTypeInfos;
  private ArrayList<String> partTypes;
  private ArrayList<PartSpaceSpec> partSpaces;
  private ArrayList<RangePartitionExprTree> exprTrees;
  
  public partitionSinkDesc() { }
  public partitionSinkDesc(
      final String dirName,
      final tableDesc table, 
      final boolean compressed,
      final int destTableId,
      final ArrayList<java.lang.String> partTypes,
      final ArrayList<String> partTypeInfos,
      final ArrayList<exprNodeDesc> partKeys,
      final ArrayList<PartSpaceSpec> partSpaces,
      final ArrayList<RangePartitionExprTree> exprTrees
      ) {
    super(dirName, table, compressed, destTableId);
    this.partTypes = partTypes;
    this.partTypeInfos = partTypeInfos;
    this.partKeys = partKeys;
    this.partSpaces = partSpaces;
    this.partTypeInfos = partTypeInfos;
    this.exprTrees = exprTrees;
  }
  
  @explain(displayName="partition types")
  public ArrayList<String> getPartTypes() {
    return this.partTypes;
  }
  
  public void setPartTypes(final ArrayList<String> partTypes) {
    this.partTypes = partTypes;
  }
  
  @explain(displayName="partition values")
  public ArrayList<PartSpaceSpec> getPartSpaces() {
    return this.partSpaces;
  }
  
  public void setPartSpaces(final ArrayList<PartSpaceSpec> partSpaces) {
    this.partSpaces = partSpaces;
  }
  
  @explain(displayName="partition field typeinfos")
  public ArrayList<String> getPartTypeInfos() {
    return this.partTypeInfos;
  }
  
  public void setPartTypeInfos(ArrayList<String> partTypeInfos) {
    this.partTypeInfos = partTypeInfos;
  }
  
  @explain(displayName="partition keys")
  public ArrayList<exprNodeDesc> getPartKeys() {
    return this.partKeys;
  }
  
  public void setPartKeys(ArrayList<exprNodeDesc> partKeys) {
    this.partKeys = partKeys;
  }
  
  public ArrayList<RangePartitionExprTree> getExprTrees() {
    return this.exprTrees;
  }
  
  public void setExprTrees(ArrayList<RangePartitionExprTree> exprTrees) {
    this.exprTrees = exprTrees;
  }

  @Override
  protected partitionSinkDesc clone() throws CloneNotSupportedException {
    return (partitionSinkDesc) super.clone();
  }
  
}
