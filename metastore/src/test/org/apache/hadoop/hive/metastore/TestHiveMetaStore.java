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

package org.apache.hadoop.hive.metastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.util.StringUtils;

public class TestHiveMetaStore extends TestCase {
  private HiveMetaStoreClient client;
  private HiveConf hiveConf;

  protected void setUp() throws Exception {
    super.setUp();
    hiveConf = new HiveConf(this.getClass());
    try {
      client = new HiveMetaStoreClient(hiveConf);
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  protected void tearDown() throws Exception {
    try {
      super.tearDown();
      client.close();
    } catch (Throwable e) {
      System.err.println("Unable to close metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }
  
  public void testPartition() throws Exception {
    try {
    String dbName = "compdb";
    String tblName = "comptbl";
    String typeName = "Person";
  
    client.dropTable(dbName, tblName);
    client.dropDatabase(dbName);
    boolean ret = client.createDatabase(dbName, "strange_loc");
    assertTrue("Unable to create the databse " + dbName, ret);
  
    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<FieldSchema>(2));
    typ1.getFields().add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
    typ1.getFields().add(new FieldSchema("income", Constants.INT_TYPE_NAME, ""));
    ret = client.createType(typ1);
    assertTrue("Unable to create type " + typeName, ret);
  
    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    StorageDescriptor sd = new StorageDescriptor(); 
    tbl.setSd(sd);
    sd.setCols(typ1.getFields());
    sd.setCompressed(false);
    sd.setNumBuckets(1);
    sd.setParameters(new HashMap<String, String>());
    sd.getParameters().put("test_param_1", "Use this for comments etc");
    sd.setBucketCols(new ArrayList<String>(2));
    sd.getBucketCols().add("name");
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(Constants.SERIALIZATION_FORMAT, "1");
    sd.setSortCols(new ArrayList<Order>());
  
    // primary partition 
    Map<String, List<String>> priParSpaces = new HashMap<String, List<String>>();
    List<String> rangeValues = new ArrayList<String>(); rangeValues.add("2006");
    priParSpaces.put("2006", rangeValues); 
    rangeValues = new ArrayList<String>(); rangeValues.add("2007");
    priParSpaces.put("2007", rangeValues);
    
    Partition priPartition = new Partition(
        dbName, tblName, 0, "RANGE", 
        new FieldSchema("ds", org.apache.hadoop.hive.serde.Constants.DATE_TYPE_NAME, ""),
        priParSpaces
    );
    
    tbl.setPriPartition(priPartition);

    // secondary paritiion
    Map<String, List<String>> subParSpaces = new HashMap<String, List<String>>();
    List<String> listValues = new ArrayList<String>();
    for (int i=0; i<24; i+=2) {
      listValues.add(String.valueOf(i));
    }
    subParSpaces.put("0", listValues); 
    listValues = new ArrayList<String>();
    for (int i=1; i<24; i+=2) {
      listValues.add(String.valueOf(i));
    }
    subParSpaces.put("1", listValues);
    
    Partition subPartition = new Partition(
        dbName, tblName, 1, "LIST", 
        new FieldSchema("hr", org.apache.hadoop.hive.serde.Constants.INT_TYPE_NAME, ""),
        subParSpaces
    );
    
    tbl.setSubPartition(subPartition);
  
    client.createTable(tbl);
  
    Partition priPart = client.getPriPartition(dbName, tblName);
    Partition subPart = client.getSubPartition(dbName, tblName);
    validatePartition(priPartition, priPart);
    validatePartition(subPartition, subPart);
    
    subPartition.setLevel(0);
    client.setPriPartition(dbName, tblName, subPartition);
    priPartition.setLevel(1);
    client.setSubPartition(dbName, tblName, priPartition);
    
    priPart = client.getPriPartition(dbName, tblName);
    subPart = client.getSubPartition(dbName, tblName);
    validatePartition(subPartition, priPart);
    validatePartition(priPartition, subPart);
    
    priPartition.setLevel(0);
    subPartition.setLevel(1);
    List<Partition> parts = new ArrayList<Partition>();
    parts.add(priPartition); parts.add(subPartition);
    client.setParititions(dbName, tblName, parts);
    
    priPart = client.getPriPartition(dbName, tblName);
    subPart = client.getSubPartition(dbName, tblName);
    validatePartition(priPartition, priPart);
    validatePartition(subPartition, subPart);
  
    client.dropTable(dbName, tblName);
  
    ret = client.dropType(typeName);
    assertTrue("Unable to drop type " + typeName, ret);
    
    ret = client.dropDatabase(dbName);
    assertTrue("Unable to drop the databse " + dbName, ret);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testPartition() failed.");
      throw e;
    }    
  }

//  /**
//   * tests create table and partition and tries to drop the table without droppping the partition
//   * @throws Exception 
//   */
//  public void testPartition() throws Exception {
//    try {
//    String dbName = "compdb";
//    String tblName = "comptbl";
//    String typeName = "Person";
//    List<String> vals = new ArrayList<String>(2);
//    vals.add("2008-07-01");
//    vals.add("14");
//  
//    client.dropTable(dbName, tblName);
//    client.dropDatabase(dbName);
//    boolean ret = client.createDatabase(dbName, "strange_loc");
//    assertTrue("Unable to create the databse " + dbName, ret);
//  
//    client.dropType(typeName);
//    Type typ1 = new Type();
//    typ1.setName(typeName);
//    typ1.setFields(new ArrayList<FieldSchema>(2));
//    typ1.getFields().add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
//    typ1.getFields().add(new FieldSchema("income", Constants.INT_TYPE_NAME, ""));
//    ret = client.createType(typ1);
//    assertTrue("Unable to create type " + typeName, ret);
//  
//    Table tbl = new Table();
//    tbl.setDbName(dbName);
//    tbl.setTableName(tblName);
//    StorageDescriptor sd = new StorageDescriptor(); 
//    tbl.setSd(sd);
//    sd.setCols(typ1.getFields());
//    sd.setCompressed(false);
//    sd.setNumBuckets(1);
//    sd.setParameters(new HashMap<String, String>());
//    sd.getParameters().put("test_param_1", "Use this for comments etc");
//    sd.setBucketCols(new ArrayList<String>(2));
//    sd.getBucketCols().add("name");
//    sd.setSerdeInfo(new SerDeInfo());
//    sd.getSerdeInfo().setName(tbl.getTableName());
//    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
//    sd.getSerdeInfo().getParameters().put(Constants.SERIALIZATION_FORMAT, "1");
//    sd.setSortCols(new ArrayList<Order>());
//  
//    tbl.setPartitionKeys(new ArrayList<FieldSchema>(2));
//    tbl.getPartitionKeys().add(new FieldSchema("ds", Constants.STRING_TYPE_NAME, ""));
//    tbl.getPartitionKeys().add(new FieldSchema("hr", Constants.INT_TYPE_NAME, ""));
//  
//    client.createTable(tbl);
//  
//    Partition part = new Partition();
//    part.setDbName(dbName);
//    part.setTableName(tblName);
//    part.setValues(vals);
//    part.setParameters(new HashMap<String, String>());
//    part.setSd(tbl.getSd());
//    part.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo());
//    part.getSd().setLocation(tbl.getSd().getLocation() + "/part1");
//  
//    Partition retp = client.add_partition(part);
//    assertNotNull("Unable to create partition " + part, retp);
//  
//    Partition part2 = client.getPartition(dbName, tblName, part.getValues());
//    assertTrue("Partitions are not same",part.equals(part2));
//  
//    FileSystem fs = FileSystem.get(this.hiveConf);
//    Path partPath = new Path(part2.getSd().getLocation());
//    
//    assertTrue(fs.exists(partPath));
//    ret = client.dropPartition(dbName, tblName, part.getValues(), true);
//    assertTrue(ret);
//    assertFalse(fs.exists(partPath));
//  
//    // add the partition again so that drop table with a partition can be tested
//    retp = client.add_partition(part);
//    assertNotNull("Unable to create partition " + part, ret);
//  
//    client.dropTable(dbName, tblName);
//  
//    ret = client.dropType(typeName);
//    assertTrue("Unable to drop type " + typeName, ret);
//
//    //recreate table as external, drop partition and it should
//    //still exist
//    tbl.setParameters(new HashMap<String, String>());
//    tbl.getParameters().put("EXTERNAL", "TRUE");
//    client.createTable(tbl);
//    retp = client.add_partition(part);
//    assertTrue(fs.exists(partPath));
//    client.dropPartition(dbName, tblName, part.getValues(), true);
//    assertTrue(fs.exists(partPath));
//    
//    ret = client.dropDatabase(dbName);
//    assertTrue("Unable to create the databse " + dbName, ret);
//    } catch (Exception e) {
//      System.err.println(StringUtils.stringifyException(e));
//      System.err.println("testPartition() failed.");
//      throw e;
//    }
//  }
//  
//  public void testAlterPartition() throws Throwable {
//    
//    try {
//      String dbName = "compdb";
//      String tblName = "comptbl";
//      List<String> vals = new ArrayList<String>(2);
//      vals.add("2008-07-01");
//      vals.add("14");
//    
//      client.dropTable(dbName, tblName);
//      client.dropDatabase(dbName);
//      boolean ret = client.createDatabase(dbName, "strange_loc");
//      assertTrue("Unable to create the databse " + dbName, ret);
//    
//      ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
//      cols.add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
//      cols.add(new FieldSchema("income", Constants.INT_TYPE_NAME, ""));
//    
//      Table tbl = new Table();
//      tbl.setDbName(dbName);
//      tbl.setTableName(tblName);
//      StorageDescriptor sd = new StorageDescriptor(); 
//      tbl.setSd(sd);
//      sd.setCols(cols);
//      sd.setCompressed(false);
//      sd.setNumBuckets(1);
//      sd.setParameters(new HashMap<String, String>());
//      sd.getParameters().put("test_param_1", "Use this for comments etc");
//      sd.setBucketCols(new ArrayList<String>(2));
//      sd.getBucketCols().add("name");
//      sd.setSerdeInfo(new SerDeInfo());
//      sd.getSerdeInfo().setName(tbl.getTableName());
//      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
//      sd.getSerdeInfo().getParameters().put(Constants.SERIALIZATION_FORMAT, "1");
//      sd.setSortCols(new ArrayList<Order>());
//    
//      tbl.setPartitionKeys(new ArrayList<FieldSchema>(2));
//      tbl.getPartitionKeys().add(new FieldSchema("ds", Constants.STRING_TYPE_NAME, ""));
//      tbl.getPartitionKeys().add(new FieldSchema("hr", Constants.INT_TYPE_NAME, ""));
//    
//      client.createTable(tbl);
//    
//      Partition part = new Partition();
//      part.setDbName(dbName);
//      part.setTableName(tblName);
//      part.setValues(vals);
//      part.setParameters(new HashMap<String, String>());
//      part.setSd(tbl.getSd());
//      part.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo());
//      part.getSd().setLocation(tbl.getSd().getLocation() + "/part1");
//    
//      client.add_partition(part);
//    
//      Partition part2 = client.getPartition(dbName, tblName, part.getValues());
//      
//      part2.getParameters().put("retention", "10");
//      part2.getSd().setNumBuckets(12);
//      part2.getSd().getSerdeInfo().getParameters().put("abc", "1");
//      client.alter_partition(dbName, tblName, part2);
//    
//      Partition part3 = client.getPartition(dbName, tblName, part.getValues());
//      assertEquals("couldn't alter partition", part3.getParameters().get("retention"), "10");
//      assertEquals("couldn't alter partition", part3.getSd().getSerdeInfo().getParameters().get("abc"), "1");
//      assertEquals("couldn't alter partition", part3.getSd().getNumBuckets(), 12);
//      
//      client.dropTable(dbName, tblName);
//      
//      ret = client.dropDatabase(dbName);
//      assertTrue("Unable to create the databse " + dbName, ret);
//      } catch (Exception e) {
//        System.err.println(StringUtils.stringifyException(e));
//        System.err.println("testPartition() failed.");
//        throw e;
//      }
//  }

  public void testDatabase() throws Throwable {
    try {
    // clear up any existing databases
    client.dropDatabase("test1");
    client.dropDatabase("test2");
    
    boolean ret = client.createDatabase("test1", "strange_loc");
    assertTrue("Unable to create the databse", ret);

    Database db = client.getDatabase("test1");
    
    assertEquals("name of returned db is different from that of inserted db", "test1", db.getName());
    assertEquals("location of the returned db is different from that of inserted db", "strange_loc", db.getDescription());
    
    boolean ret2 = client.createDatabase("test2", "another_strange_loc");
    assertTrue("Unable to create the databse", ret2);

    Database db2 = client.getDatabase("test2");
    
    assertEquals("name of returned db is different from that of inserted db", "test2", db2.getName());
    assertEquals("location of the returned db is different from that of inserted db", "another_strange_loc", db2.getDescription());
    
    List<String> dbs = client.getDatabases();
    
    assertTrue("first database is not test1", dbs.contains("test1"));
    assertTrue("second database is not test2", dbs.contains("test2"));
    
    ret = client.dropDatabase("test1");
    assertTrue("couldn't delete first database", ret);
    ret = client.dropDatabase("test2");
    assertTrue("couldn't delete second database", ret);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testDatabase() failed.");
      throw e;
    }
  }
  
  public void testSimpleTypeApi() throws Exception {
    try {
    client.dropType(Constants.INT_TYPE_NAME);
    
    Type typ1 = new Type();
    typ1.setName(Constants.INT_TYPE_NAME);
    boolean ret = client.createType(typ1);
    assertTrue("Unable to create type", ret);
    
    Type typ1_2 = client.getType(Constants.INT_TYPE_NAME);
    assertNotNull(typ1_2);
    assertEquals(typ1.getName(), typ1_2.getName());
    
    ret = client.dropType(Constants.INT_TYPE_NAME);
    assertTrue("unable to drop type integer", ret);
    
    Type typ1_3 = null;
    typ1_3 = client.getType(Constants.INT_TYPE_NAME);
    assertNull("unable to drop type integer",typ1_3);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testSimpleTypeApi() failed.");
      throw e;
    }
}
  
  // TODO:pc need to enhance this with complex fields and getType_all function
  public void testComplexTypeApi() throws Exception {
    try {
    client.dropType("Person");
    
    Type typ1 = new Type();
    typ1.setName("Person");
    typ1.setFields(new ArrayList<FieldSchema>(2));
    typ1.getFields().add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
    typ1.getFields().add(new FieldSchema("income", Constants.INT_TYPE_NAME, ""));
    boolean ret = client.createType(typ1);
    assertTrue("Unable to create type", ret);
    
    Type typ1_2 = client.getType("Person");
    assertNotNull("type Person not found", typ1_2);
    assertEquals(typ1.getName(), typ1_2.getName());
    assertEquals(typ1.getFields().size(), typ1_2.getFields().size());
    assertEquals(typ1.getFields().get(0), typ1_2.getFields().get(0));
    assertEquals(typ1.getFields().get(1), typ1_2.getFields().get(1));
    
    client.dropType("Family");
    
    Type fam = new Type();
    fam.setName("Family");
    fam.setFields(new ArrayList<FieldSchema>(2));
    fam.getFields().add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
    fam.getFields().add(new FieldSchema("members", MetaStoreUtils.getListType(typ1.getName()), ""));
    
    ret = client.createType(fam);
    assertTrue("Unable to create type " + fam.getName(), ret);
    
    Type fam2 = client.getType("Family");
    assertNotNull("type Person not found", fam2);
    assertEquals(fam.getName(), fam2.getName());
    assertEquals(fam.getFields().size(), fam2.getFields().size());
    assertEquals(fam.getFields().get(0), fam2.getFields().get(0));
    assertEquals(fam.getFields().get(1), fam2.getFields().get(1));
    
    ret = client.dropType("Family");
    assertTrue("unable to drop type Family", ret);
    
    ret = client.dropType("Person");
    assertTrue("unable to drop type Person", ret);
    
    Type typ1_3 = null;
    typ1_3 = client.getType("Person");
    assertNull("unable to drop type Person",typ1_3);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testComplexTypeApi() failed.");
      throw e;
    }
  }
  
  public void testSimpleTable() throws Exception {
    try {
    String dbName = "simpdb";
    String tblName = "simptbl";
    String tblName2 = "simptbl2";
    String typeName = "Person";
  
    client.dropTable(dbName, tblName);
    client.dropDatabase(dbName);
    boolean ret = client.createDatabase(dbName, "strange_loc");
    assertTrue("Unable to create the databse " + dbName, ret);
    
    client.dropType(typeName);
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<FieldSchema>(2));
    typ1.getFields().add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
    typ1.getFields().add(new FieldSchema("income", Constants.INT_TYPE_NAME, ""));
    ret = client.createType(typ1);
    assertTrue("Unable to create type " + typeName, ret);
    
    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    StorageDescriptor sd = new StorageDescriptor();
    tbl.setSd(sd);
    sd.setCols(typ1.getFields());
    sd.setCompressed(false);
    sd.setNumBuckets(1);
    sd.setParameters(new HashMap<String, String>());
    sd.getParameters().put("test_param_1", "Use this for comments etc");
    sd.setBucketCols(new ArrayList<String>(2));
    sd.getBucketCols().add("name");
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "1");
    sd.getSerdeInfo().setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
    // tbl.setPartitionKeys(new ArrayList<FieldSchema>());
    
    client.createTable(tbl);
    
    Table tbl2 = client.getTable(dbName, tblName);
    assertNotNull(tbl2);
    assertEquals(tbl2.getDbName(), dbName);
    assertEquals(tbl2.getTableName(), tblName);
    assertEquals(tbl2.getSd().getCols().size(), typ1.getFields().size());
    assertEquals(tbl2.getSd().isCompressed(), false);
    assertEquals(tbl2.getSd().getNumBuckets(), 1);
    assertEquals(tbl2.getSd().getLocation(), tbl.getSd().getLocation());
    assertNotNull(tbl2.getSd().getSerdeInfo());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "1");
    
    tbl2.setTableName(tblName2);
    tbl2.setParameters(new HashMap<String, String>());
    tbl2.getParameters().put("EXTERNAL", "TRUE");
    tbl2.getSd().setLocation(tbl.getSd().getLocation() +"-2");
    
    List<FieldSchema> fieldSchemas = client.getFields(dbName, tblName);
    assertNotNull(fieldSchemas);
    assertEquals(fieldSchemas.size(), tbl.getSd().getCols().size());
    for (FieldSchema fs : tbl.getSd().getCols()) {
      assertTrue(fieldSchemas.contains(fs));
    }
    
    List<FieldSchema> fieldSchemasFull = client.getSchema(dbName, tblName);
    assertNotNull(fieldSchemasFull);
    // assertEquals(fieldSchemasFull.size(), tbl.getSd().getCols().size()+tbl.getPartitionKeys().size());
    assertEquals(fieldSchemasFull.size(), tbl.getSd().getCols().size());
    for (FieldSchema fs : tbl.getSd().getCols()) {
      assertTrue(fieldSchemasFull.contains(fs));
    }
    /**
    for (FieldSchema fs : tbl.getPartitionKeys()) {
      assertTrue(fieldSchemasFull.contains(fs));
    }
    **/
    
    client.createTable(tbl2);
  
    Table tbl3 = client.getTable(dbName, tblName2);
    assertNotNull(tbl3);
    assertEquals(tbl3.getDbName(), dbName);
    assertEquals(tbl3.getTableName(), tblName2);
    assertEquals(tbl3.getSd().getCols().size(), typ1.getFields().size());
    assertEquals(tbl3.getSd().isCompressed(), false);
    assertEquals(tbl3.getSd().getNumBuckets(), 1);
    assertEquals(tbl3.getSd().getLocation(), tbl2.getSd().getLocation());
    assertEquals(tbl3.getParameters(), tbl2.getParameters());
    
    fieldSchemas = client.getFields(dbName, tblName2);
    assertNotNull(fieldSchemas);
    assertEquals(fieldSchemas.size(), tbl2.getSd().getCols().size());
    for (FieldSchema fs : tbl2.getSd().getCols()) {
      assertTrue(fieldSchemas.contains(fs));
    }
    
    fieldSchemasFull = client.getSchema(dbName, tblName2);
    assertNotNull(fieldSchemasFull);
    // assertEquals(fieldSchemasFull.size(), tbl2.getSd().getCols().size()+tbl2.getPartitionKeys().size());
    assertEquals(fieldSchemasFull.size(), tbl2.getSd().getCols().size());
    for (FieldSchema fs : tbl2.getSd().getCols()) {
      assertTrue(fieldSchemasFull.contains(fs));
    }
    /**
    for (FieldSchema fs : tbl2.getPartitionKeys()) {
      assertTrue(fieldSchemasFull.contains(fs));
    }
    **/
  
    assertEquals("Use this for comments etc", tbl2.getSd().getParameters().get("test_param_1"));
    assertEquals("name", tbl2.getSd().getBucketCols().get(0));
    // assertTrue("Partition key list is not empty",  (tbl2.getPartitionKeys() == null) || (tbl2.getPartitionKeys().size() == 0));
    
    FileSystem fs = FileSystem.get(hiveConf);
    client.dropTable(dbName, tblName);
    assertFalse(fs.exists(new Path(tbl.getSd().getLocation())));
    
    client.dropTable(dbName, tblName2);
    assertTrue(fs.exists(new Path(tbl2.getSd().getLocation())));
  
    ret = client.dropType(typeName);
    assertTrue("Unable to drop type " + typeName, ret);
    ret = client.dropDatabase(dbName);
    assertTrue("Unable to drop databse " + dbName, ret);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testSimpleTable() failed.");
      throw e;
    }
  }

  public void testAlterTable() throws Exception {
    try {
      String dbName = "alterdb";
      String invTblName = "alter-tbl";
      String tblName = "altertbl";

      client.dropTable(dbName, tblName);
      client.dropDatabase(dbName);
      boolean ret = client.createDatabase(dbName, "strange_loc");
      assertTrue("Unable to create the databse " + dbName, ret);

      ArrayList<FieldSchema> invCols = new ArrayList<FieldSchema>(2);
      invCols.add(new FieldSchema("n-ame", Constants.STRING_TYPE_NAME, ""));
      invCols.add(new FieldSchema("in.come", Constants.INT_TYPE_NAME, ""));

      Table tbl = new Table();
      tbl.setDbName(dbName);
      tbl.setTableName(invTblName);
      StorageDescriptor sd = new StorageDescriptor();
      tbl.setSd(sd);
      sd.setCols(invCols);
      sd.setCompressed(false);
      sd.setNumBuckets(1);
      sd.setParameters(new HashMap<String, String>());
      sd.getParameters().put("test_param_1", "Use this for comments etc");
      sd.setBucketCols(new ArrayList<String>(2));
      sd.getBucketCols().add("name");
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(tbl.getTableName());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().getParameters().put(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "1");
      boolean failed = false;
      try {
        client.createTable(tbl);
      } catch (InvalidObjectException ex) {
        failed = true;
      }
      if(!failed) {
        assertTrue("Able to create table with invalid name: " + invTblName, false);
      }
      ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
      cols.add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("income", Constants.INT_TYPE_NAME, ""));

      // create a valid table
      tbl.setTableName(tblName);
      tbl.getSd().setCols(cols);
      client.createTable(tbl);
      
      // now try to invalid alter table
      Table tbl2 = client.getTable(dbName, tblName);
      failed = false;
      try {
        tbl2.setTableName(invTblName);
        tbl2.getSd().setCols(invCols);
        client.alter_table(dbName, tblName, tbl2);
      } catch (InvalidOperationException ex) {
        failed = true;
      }
      if(!failed) {
        assertTrue("Able to rename table with invalid name: " + invTblName, false);
      }
      // try a valid alter table
      tbl2.setTableName(tblName+"_renamed");
      tbl2.getSd().setCols(cols);
      tbl2.getSd().setNumBuckets(32);
      client.alter_table(dbName, tblName, tbl2);
      Table tbl3 = client.getTable(dbName, tbl2.getTableName());
      assertEquals("Alter table didn't succeed. Num buckets is different ",
          tbl2.getSd().getNumBuckets(), tbl3.getSd().getNumBuckets());
      // check that data has moved
      FileSystem fs = FileSystem.get(hiveConf);
      assertFalse("old table location still exists", fs.exists(new Path(tbl.getSd().getLocation())));
      assertTrue("data did not move to new location", fs.exists(new Path(tbl3.getSd().getLocation())));
      assertEquals("alter table didn't move data correct location", tbl3.getSd().getLocation(),
          tbl2.getSd().getLocation());
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testSimpleTable() failed.");
      throw e;
    }
  }
  
  public void testComplexTable() throws Exception {
    complexTableTest(false, false);
    complexTableTest(true, false);
    complexTableTest(true, true);
  }
  
  private void complexTableTest(boolean testPriPart, boolean testSubPart) throws Exception {
  
    String dbName = "compdb";
    String tblName = "comptbl";
    String typeName = "Person";
  
    try {
      client.dropTable(dbName, tblName);
      client.dropDatabase(dbName);
      boolean ret = client.createDatabase(dbName, "strange_loc");
      assertTrue("Unable to create the databse " + dbName, ret);
  
      client.dropType(typeName);
      Type typ1 = new Type();
      typ1.setName(typeName);
      typ1.setFields(new ArrayList<FieldSchema>(2));
      typ1.getFields().add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
      typ1.getFields().add(new FieldSchema("income", Constants.INT_TYPE_NAME, ""));
      ret = client.createType(typ1);
      assertTrue("Unable to create type " + typeName, ret);
  
      Table tbl = new Table();
      tbl.setDbName(dbName);
      tbl.setTableName(tblName);
      StorageDescriptor sd = new StorageDescriptor();
      tbl.setSd(sd);
      sd.setCols(typ1.getFields());
      sd.setCompressed(false);
      sd.setNumBuckets(1);
      sd.setParameters(new HashMap<String, String>());
      sd.getParameters().put("test_param_1", "Use this for comments etc");
      sd.setBucketCols(new ArrayList<String>(2));
      sd.getBucketCols().add("name");
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(tbl.getTableName());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().getParameters().put(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "9");
      sd.getSerdeInfo().setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());

      if (testPriPart) {
	      Map<String, List<String>> priParSpaces = new HashMap<String, List<String>>();
	      List<String> rangeValues = new ArrayList<String>(); rangeValues.add("2006");
	      priParSpaces.put("2006", rangeValues); 
	      rangeValues = new ArrayList<String>(); rangeValues.add("2007");
	      priParSpaces.put("2007", rangeValues);
	      
	      Partition priPartition = new Partition(
	          dbName, tblName, 0, "RANGE", 
	          new FieldSchema("ds", org.apache.hadoop.hive.serde.Constants.DATE_TYPE_NAME, ""),
	          priParSpaces
	      );
	      
	      tbl.setPriPartition(priPartition);
      }
      
      if (testSubPart) {
	      Map<String, List<String>> subParSpaces = new HashMap<String, List<String>>();
	      List<String> listValues = new ArrayList<String>();
	      for (int i=0; i<24; i+=2) {
	        listValues.add(String.valueOf(i));
	      }
	      subParSpaces.put("0", listValues); 
	      listValues = new ArrayList<String>();
	      for (int i=1; i<24; i+=2) {
	        listValues.add(String.valueOf(i));
	      }
	      subParSpaces.put("1", listValues);
	      
	      Partition subPartition = new Partition(
	          dbName, tblName, 1, "LIST", 
	          new FieldSchema("hr", org.apache.hadoop.hive.serde.Constants.INT_TYPE_NAME, ""),
	          subParSpaces
	      );
	      
	      tbl.setSubPartition(subPartition);
      }
	      
//      tbl.setPartitionKeys(new ArrayList<FieldSchema>(2));
//      tbl.getPartitionKeys().add(new FieldSchema("ds", org.apache.hadoop.hive.serde.Constants.DATE_TYPE_NAME, ""));
//      tbl.getPartitionKeys().add(new FieldSchema("hr", org.apache.hadoop.hive.serde.Constants.INT_TYPE_NAME, ""));
  
      client.createTable(tbl);
  
      Table tbl2 = client.getTable(dbName, tblName);
      assertEquals(tbl2.getDbName(), dbName);
      assertEquals(tbl2.getTableName(), tblName);
      assertEquals(tbl2.getSd().getCols().size(), typ1.getFields().size());
      assertFalse(tbl2.getSd().isCompressed());
      assertEquals(tbl2.getSd().getNumBuckets(), 1);
  
      assertEquals("Use this for comments etc", tbl2.getSd().getParameters().get("test_param_1"));
      assertEquals("name", tbl2.getSd().getBucketCols().get(0));
      
      Partition priPart2 = tbl2.getPriPartition();
      Partition subPart2 = tbl2.getSubPartition();
      if (testPriPart) {
        assertNotNull(priPart2);
        validatePartition(tbl.getPriPartition(), priPart2);
      } else {
        assertNull(priPart2);
      }
      if (testSubPart) {
        assertNotNull(subPart2);
        validatePartition(tbl.getSubPartition(), subPart2);
      } else {
        assertNull(subPart2);
      }
      
//      assertNotNull(tbl2.getPartitionKeys());
//      assertEquals(2, tbl2.getPartitionKeys().size());
//      assertEquals(Constants.DATE_TYPE_NAME, tbl2.getPartitionKeys().get(0).getType());
//      assertEquals(Constants.INT_TYPE_NAME, tbl2.getPartitionKeys().get(1).getType());
//      assertEquals("ds", tbl2.getPartitionKeys().get(0).getName());
//      assertEquals("hr", tbl2.getPartitionKeys().get(1).getName());
      
      List<FieldSchema> fieldSchemas = client.getFields(dbName, tblName);
      assertNotNull(fieldSchemas);
      assertEquals(fieldSchemas.size(), tbl.getSd().getCols().size());
      for (FieldSchema fs : tbl.getSd().getCols()) {
        assertTrue(fieldSchemas.contains(fs));
      }
      
      List<FieldSchema> fieldSchemasFull = client.getSchema(dbName, tblName);
      assertNotNull(fieldSchemasFull);
      assertEquals(fieldSchemasFull.size(), tbl.getSd().getCols().size());
      for (FieldSchema fs : tbl.getSd().getCols()) {
        assertTrue(fieldSchemasFull.contains(fs));
      }
//      for (FieldSchema fs : tbl.getPartitionKeys()) {
//        assertTrue(fieldSchemasFull.contains(fs));
//      }
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testComplexTable() failed.");
      throw e;
    } finally {
      client.dropTable(dbName, tblName);
      boolean ret = client.dropType(typeName);
      assertTrue("Unable to drop type " + typeName, ret);
      ret = client.dropDatabase(dbName);
      assertTrue("Unable to create the databse " + dbName, ret);
    }
  }
  
  private void validatePartition(Partition expectedPart, Partition actualPart) {
    if (expectedPart == null && actualPart == null) {
      return;
    } 
    
    assertTrue((expectedPart != null) & (actualPart != null));
    
    assertEquals(expectedPart.getDbName(), actualPart.getDbName());
    assertEquals(expectedPart.getTableName(), actualPart.getTableName());
    assertEquals(expectedPart.getLevel(), actualPart.getLevel());
    assertEquals(expectedPart.getParKey(), actualPart.getParKey());
    assertEquals(expectedPart.getParSpacesSize(), actualPart.getParSpacesSize());
    
    // validate the partition spaces
    Set<String> expectedParKeys = expectedPart.getParSpaces().keySet();
    assertTrue(expectedParKeys.containsAll(actualPart.getParSpaces().keySet()));
    for (String expectedParKey : expectedParKeys) {
      List<String> expectedParValues = expectedPart.getParSpaces().get(expectedParKey);
      List<String> actualParValues = actualPart.getParSpaces().get(expectedParKey);
      assertEquals(expectedParValues.size(), actualParValues.size());
      for (int i=0; i<expectedParValues.size(); i++) {
        assertEquals(expectedParValues.get(i), actualParValues.get(i));
      }
    }
  }
}