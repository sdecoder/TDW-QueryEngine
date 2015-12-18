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
import java.util.Map;

import org.apache.hadoop.hive.ql.parse.PartitionDesc;

/**
 * Contains the information needed to add a partition. 
 */
public class AddPartitionDesc extends ddlDesc implements Serializable {

  String tableName;
  String dbName;
  //String location;
  Boolean isSubPartition;
  PartitionDesc partDesc;
  
  /**
   * @param dbName database to add to.
   * @param tableName table to add to.
   * @param partSpec partition specification.
   * @param location partition location, relative to table location. 
   */
  public AddPartitionDesc(String dbName, String tableName,
      PartitionDesc pd, Boolean isSub) {
    super();
    this.dbName = dbName;
    this.tableName = tableName;
    this.partDesc = pd;
    this.isSubPartition = isSub;
    //this.location = location;
  }

  public Boolean getIsSubPartition() {
	//TODO:implement it by call api.table's interface
	  return isSubPartition;
}

public void setIsSubPartition(Boolean isSubPartition) {
	//TODO:see above
	this.isSubPartition = isSubPartition;
}

/**
   * @return database name
   */
  public String getDbName() {
    return dbName;
  }

  /**
   * @param dbName database name
   */
  public void setDbName(String dbName) {
    this.dbName = dbName;
  }
  
  /**
   * @return the table we're going to add the partitions to.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName the table we're going to add the partitions to.
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

public PartitionDesc getPartDesc() {
	return partDesc;
}

public void setPartDesc(PartitionDesc partDesc) {
	this.partDesc = partDesc;
}

  /**
   * @return location of partition in relation to table
   
  public String getLocation() {
    return location;
  }

  /**
   * @param location location of partition in relation to table
   
  public void setLocation(String location) {
    this.location = location;
  }

  /**
   * @return partition specification.
   
  public Map<String, String> getPartSpec() {
    return partSpec;
  }

  /**
   * @param partSpec partition specification
   
  public void setPartSpec(Map<String, String> partSpec) {
    this.partSpec = partSpec;
  }
  */

}