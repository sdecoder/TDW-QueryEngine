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

package org.apache.hadoop.hive.metastore.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MPartition {

  // Modified By : guosijie
  // Modified Date : 2010-02-05
  //   change to the new data structure to support LIST/RANGE partition
  
//  private String partitionName; // partitionname ==>  (key=value/)*(key=value)
//  private MTable table; 
//  private List<String> values;
//  private int createTime;
//  private int lastAccessTime;
//  private MStorageDescriptor sd;
//  private Map<String, String> parameters;
  
  // Modification start
  
  private String dbName;
  private String tableName;
  private int level;
  private String parType;
  private MFieldSchema parKey;
  private List<String> partNames;
  private List<MPartSpace> parSpaces;
  
  // Modification end
  
  public MPartition() {}
  
  /**
   * @param partitionName
   * @param table
   * @param values
   * @param createTime
   * @param lastAccessTime
   * @param sd
   * @param parameters
   */
  public MPartition(String dbName, String tableName, 
      int level, String parType,
      MFieldSchema parKey, Map<String, List<String>> parValues) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.level = level;
    this.parType = parType;
    this.parKey = parKey;
    this.partNames = new ArrayList<String>();
    this.parSpaces = new ArrayList<MPartSpace>();
    convertToPartSpaces(parValues, partNames, parSpaces);
  }
  
  /**
   * @return level
   */
  public int getLevel() {
    return level;
  }
  
  /**
   * Set the partition level
   * @param level
   */
  public void setLevel(int level) {
    this.level = level;
  }
  
  /**
   * @return the partition type
   */
  public String getParType() {
    return this.parType;
  }
  
  /**
   * Set the partition type
   * @param parType
   */
  public void setParType(String parType) {
    this.parType = parType;
  }
  
  /**
   * @return the partition key
   */
  public MFieldSchema getParKey() {
    return parKey;
  }
  
  /**
   * Set the partition key
   * @param parKey
   */
  public void setParKey(MFieldSchema parKey) {
    this.parKey = parKey;
  }
  
  /**
   * @return the partition spaces
   */
  public Map<String, List<String>> getParSpaces() {
    return convertToPartValues(this.partNames, this.parSpaces);
  }
  
  public Map<String, MPartSpace> getPartSpaceMap() {
    Map<String, MPartSpace> map = new LinkedHashMap<String, MPartSpace>();
    for (int i=0; i<partNames.size(); i++) {
      map.put(partNames.get(i), parSpaces.get(i));
    }
    return map;
  }
  
  /**
   * Set the partition spaces
   * @param parSpaces
   */
  public void setParSpaces(Map<String, List<String>> parValues) {
    if (this.parSpaces != null) 
      parSpaces.clear();
    else
      parSpaces = new ArrayList<MPartSpace>();
    if (this.partNames != null)
      partNames.clear();
    else
      partNames = new ArrayList<String>();
    convertToPartSpaces(parValues, partNames, parSpaces);
  }

//  /**
//   * @return the lastAccessTime
//   */
//  public int getLastAccessTime() {
//    return lastAccessTime;
//  }
//
//  /**
//   * @param lastAccessTime the lastAccessTime to set
//   */
//  public void setLastAccessTime(int lastAccessTime) {
//    this.lastAccessTime = lastAccessTime;
//  }
//
//  /**
//   * @return the values
//   */
//  public List<String> getValues() {
//    return values;
//  }
//
//  /**
//   * @param values the values to set
//   */
//  public void setValues(List<String> values) {
//    this.values = values;
//  }

  /**
   * @return the database name
   */
  public String getDBName() {
    return dbName;
  }
  
  /**
   * @param dbName the database name
   */
  public void setDBName(String dbName) {
    this.dbName = dbName;
  }
  
  /**
   * @return the table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param table the table to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

//  /**
//   * @return the parameters
//   */
//  public Map<String, String> getParameters() {
//    return parameters;
//  }
//
//  /**
//   * @param parameters the parameters to set
//   */
//  public void setParameters(Map<String, String> parameters) {
//    this.parameters = parameters;
//  }

//  /**
//   * @return the createTime
//   */
//  public int getCreateTime() {
//    return createTime;
//  }
//
//  /**
//   * @param createTime the createTime to set
//   */
//  public void setCreateTime(int createTime) {
//    this.createTime = createTime;
//  }
  
  static void convertToPartSpaces(Map<String, List<String>> partValues_, List<String> partNames_, List<MPartSpace> partSpaces_) {
    Map<String, MPartSpace> partSpace = new LinkedHashMap<String, MPartSpace>();
    for (Map.Entry<String, List<String>> entry : partValues_.entrySet()) {
      // System.out.println("PUT " + entry.getKey() + " : " + entry.getValue());
      partNames_.add(entry.getKey());
      partSpaces_.add(new MPartSpace(entry.getValue()));
    }
  }
  
  static Map<String, List<String>> convertToPartValues(List<String> partNames_, List<MPartSpace> partSpaces_) {
    Map<String, List<String>> partValues = new LinkedHashMap<String, List<String>>();
    for (int i=0; i<partNames_.size(); i++) {
      partValues.put(partNames_.get(i), partSpaces_.get(i).getValues());
    }
    return partValues;
  }

}
