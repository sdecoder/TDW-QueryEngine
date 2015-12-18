package org.apache.hadoop.hive.ql.plan;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.parse.QB;

public class MsckDesc {

  private String tableName;
  //private List<Map<String, String>> partitionSpec;
  
  QB.tableRef trf;
  private Path resFile;
  
  /**
   * Description of a msck command.
   * @param tableName Table to check, can be null.
   * @param partSpecs Partition specification, can be null. 
   * @param resFile Where to save the output of the command
   */
  public MsckDesc(String tableName, QB.tableRef trf, Path resFile) {
    super();
    this.tableName = tableName;
    this.trf = trf;
    this.resFile = resFile;
  }

  
  
  public QB.tableRef getTrf() {
	return trf;
}



public void setTrf(QB.tableRef trf) {
	this.trf = trf;
}



/**
   * @return the table to check
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName the table to check
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return file to save command output to
   */
  public Path getResFile() {
    return resFile;
  }

  /**
   * @param resFile file to save command output to
   */
  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }
  
}
