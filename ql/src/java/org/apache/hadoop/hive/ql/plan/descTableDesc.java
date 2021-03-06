/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;

@explain(displayName="Describe Table")
public class descTableDesc extends ddlDesc implements Serializable 
{
  private static final long serialVersionUID = 1L;
  
  String tableName;
  String partName;
  Path              resFile;
  boolean           isExt;
  /**
   * table name for the result of describe table
   */
  private final String table = "describe";
  /**
   * thrift ddl for the result of describe table
   */
  private final String schema = "col_name,data_type,comment#string:string:string";

  public String getTable() {
    return table;
  }

  public String getSchema() {
    return schema;
  }
 
  /**
   * @param isExt
   * @param partSpec
   * @param resFile
   * @param tableName
   */
  public descTableDesc(Path resFile, String tableName, String partName, boolean isExt) {
    this.isExt = isExt;
    this.partName = partName;
    this.resFile = resFile;
    this.tableName = tableName;
  }

  /**
   * @return the isExt
   */
  public boolean isExt() {
    return isExt;
  }

  /**
   * @param isExt the isExt to set
   */
  public void setExt(boolean isExt) {
    this.isExt = isExt;
  }

  /**
   * @return the tableName
   */
  @explain(displayName="table")
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName the tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getPartName() {
	return partName;
}

public void setPartName(String partName) {
	this.partName = partName;
}

/**
   * @return the resFile
   */
  public Path getResFile() {
    return resFile;
  }

  @explain(displayName="result file", normalExplain=false)
  public String getResFileString() {
    return getResFile().getName();
  }
  
  /**
   * @param resFile the resFile to set
   */
  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }
}
