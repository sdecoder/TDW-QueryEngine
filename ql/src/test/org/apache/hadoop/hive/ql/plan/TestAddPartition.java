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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.PartitionDesc;
import org.apache.hadoop.hive.ql.parse.PartitionType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;

public class TestAddPartition extends TestCase {

  private static final String PART1_NAME = "part1";
  private static final String PART2_NAME = "part2";

  public TestAddPartition() throws Exception {
    super();
  }

  public void testAddPartition() throws Exception {
    Configuration conf = new Configuration();
    HiveConf hiveConf = new HiveConf(conf, TestAddPartition.class);
    HiveMetaStoreClient client = null;
    
    SessionState.start(hiveConf);

    try {
      client = new HiveMetaStoreClient(hiveConf);

      String dbName = "testdb";
      String tableName = "tablename";

      Table tbl = new Table();
      tbl.setTableName(tableName);
      tbl.setDbName(dbName);
      tbl.setParameters(new HashMap<String, String>());

      StorageDescriptor sd = new StorageDescriptor();
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(tbl.getTableName());
      sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());

      List<FieldSchema> fss = new ArrayList<FieldSchema>();
      fss.add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
      FieldSchema part1 = new FieldSchema(PART1_NAME, Constants.STRING_TYPE_NAME, "");
      FieldSchema part2 = new FieldSchema(PART2_NAME, Constants.STRING_TYPE_NAME, "");
      
      sd.setCols(fss);
      
      fss.add(part1);
      fss.add(part2);
      
      tbl.setSd(sd);
      
      
      LinkedHashMap< String,List< String > > ps1 = new LinkedHashMap< String,List< String > >();
      
      LinkedHashMap< String,List< String > > ps2 = new LinkedHashMap< String,List< String > >();
      
      ArrayList<String> pardef_less10 = new ArrayList<String>();
      pardef_less10.add("10");
     
     
      
      ps1.put("p1_less10", pardef_less10);
      
     
      
      ArrayList<String> boy = new ArrayList<String>();
      
      boy.add("boy");
      boy.add("man");
      boy.add("m");
      
      
      ArrayList<String> girl = new ArrayList<String>();
      
      girl.add("girl");
      girl.add("woman");
      girl.add("f");
      
      ps2.put("sp1_boy", boy);
      ps2.put("sp2_girl", girl);
      
      PartitionDesc SubPartdesc = new PartitionDesc(PartitionType.LIST_PARTITION,part2,ps2,null);
      SubPartdesc.setDbName(dbName);
      SubPartdesc.setTableName(tableName);
      

      PartitionDesc PriPartdesc = new PartitionDesc(PartitionType.RANGE_PARTITION,part1,ps1,SubPartdesc);
      PriPartdesc.setDbName(dbName);
      PriPartdesc.setTableName(tableName);
      
      //we use ql_table set the partitions and then get the api_table
      
      org.apache.hadoop.hive.ql.metadata.Table ql_table = new org.apache.hadoop.hive.ql.metadata.Table(tableName);
      ql_table.setTTable(tbl);
      ql_table.setPartitions(PriPartdesc);
      
      tbl = ql_table.getTTable();
       
//      tbl.setPartitionKeys(new ArrayList<FieldSchema>());
//      tbl.getPartitionKeys().add(
//          new FieldSchema(PART1_NAME, Constants.STRING_TYPE_NAME, ""));
//      tbl.getPartitionKeys().add(
//          new FieldSchema(PART2_NAME, Constants.STRING_TYPE_NAME, ""));

      client.dropTable(dbName, tableName);
      client.dropDatabase(dbName);

      client.createDatabase(dbName, "newloc");
      client.createTable(tbl);

      tbl = client.getTable(dbName, tableName);
//add a pri partition 
      ArrayList<String> pardef_less20 = new ArrayList<String>();
      pardef_less20.add("20");
      
      LinkedHashMap< String,List< String > > ps = new LinkedHashMap< String,List< String > >();
      
      ps.put("p2_less20", pardef_less20);
      
      
      PartitionDesc pd = new PartitionDesc(dbName,tableName,PartitionType.RANGE_PARTITION,tbl.getPriPartition().getParKey(),ps,null);
     
      
//      List<String> partValues = new ArrayList<String>();
//      partValues.add("value1");
//      partValues.add("value2");
//
//      Map<String, String> part1 = new HashMap<String, String>();
//      part1.put(PART1_NAME, "value1");
//      part1.put(PART2_NAME, "value2");
//      
//      List<Map<String, String>> partitions = new ArrayList<Map<String, String>>();
//      partitions.add(part1);
//      
//      // no partitions yet
//      List<Partition> parts = client.listPartitions(dbName, tableName,
//          (short) -1);
//      assertTrue(parts.isEmpty());
//
//      String partitionLocation = PART1_NAME + Path.SEPARATOR + PART2_NAME;
//      // add the partitions
      
     
      //for (Map<String,String> map : partitions) {
        AddPartitionDesc addPartition = new AddPartitionDesc(dbName, 
            tableName, pd, false);
        Task<DDLWork> task = TaskFactory.get(new DDLWork(addPartition), hiveConf);
        task.initialize(hiveConf);
        assertEquals(0, task.execute());
      //}

      // should have one
      //parts = client.listPartitions(dbName, tableName, (short) -1);
        
      tbl = client.getTable(dbName, tableName);
      assertEquals(2,tbl.getPriPartition().getParSpaces().size());
      
      assertEquals("20", tbl.getPriPartition().getParSpaces().get("p2_less20").get(0));
      

      //client.dropPartition(dbName, tableName,"p2_less20");

      // add without location specified
      
      
      
      //add a sub partition
      ArrayList<String> unknown = new ArrayList<String>();
      
      unknown.add("unknown");
      unknown.add("null");
      
  LinkedHashMap< String,List< String > > subps = new LinkedHashMap< String,List< String > >();
      
      subps.put("unknown", unknown);
      
      PartitionDesc subpd = new PartitionDesc(dbName,tableName,PartitionType.LIST_PARTITION,tbl.getSubPartition().getParKey(),subps,null);
      
      
      


      AddPartitionDesc addPartition2 = new AddPartitionDesc(dbName, tableName,subpd,true);
      task = TaskFactory.get(new DDLWork(addPartition2), hiveConf);
      task.initialize(hiveConf);
      assertEquals(0, task.execute());
      
      tbl = client.getTable(dbName, tableName);
      //parts = client.listPartitions(dbName, tableName, (short) -1);
      assertEquals(3,tbl.getSubPartition().getParSpaces().size());
      
      assertEquals("null", tbl.getSubPartition().getParSpaces().get("unknown").get(1));
      
      
      //add multi pri partitions
      
      ArrayList<String> pardef_less30 = new ArrayList<String>();
      pardef_less30.add("30");
      
      ArrayList<String> pardef_less40 = new ArrayList<String>();
      pardef_less40.add("40");
      
      LinkedHashMap< String,List< String > > ps_multi_pri = new LinkedHashMap< String,List< String > >();
      
      ps_multi_pri.put("p3_less30", pardef_less30);
      ps_multi_pri.put("p4_less40", pardef_less40);
      
      
      PartitionDesc pd_multi_pri = new PartitionDesc(dbName,tableName,PartitionType.RANGE_PARTITION,tbl.getPriPartition().getParKey(),ps_multi_pri,null);
      
      AddPartitionDesc addPartition_multi_pri = new AddPartitionDesc(dbName, tableName,pd_multi_pri,false);
      task = TaskFactory.get(new DDLWork(addPartition_multi_pri), hiveConf);
      task.initialize(hiveConf);
      assertEquals(0, task.execute());
      
      tbl = client.getTable(dbName, tableName);
      assertEquals("30",tbl.getPriPartition().getParSpaces().get("p3_less30").get(0));
      assertEquals("40",tbl.getPriPartition().getParSpaces().get("p4_less40").get(0));
      
      
      //add multi sub partitions
      ArrayList<String> unknown1 = new ArrayList<String>();
      unknown1.add("a");
      unknown1.add("b");
      
      
      ArrayList<String> unknown2 = new ArrayList<String>();
      unknown2.add("c");
      unknown2.add("d");
      
      LinkedHashMap< String,List< String > > ps_multi_sub = new LinkedHashMap< String,List< String > >();
      
      ps_multi_sub.put("unknown1", unknown1);
      ps_multi_sub.put("unknown2", unknown2);
      
      
      PartitionDesc pd_multi_sub = new PartitionDesc(dbName,tableName,PartitionType.LIST_PARTITION,tbl.getSubPartition().getParKey(),ps_multi_sub,null);
      
      AddPartitionDesc addPartition_multi_sub = new AddPartitionDesc(dbName, tableName,pd_multi_sub,true);
      task = TaskFactory.get(new DDLWork(addPartition_multi_sub), hiveConf);
      task.initialize(hiveConf);
      assertEquals(0, task.execute());
      
      tbl = client.getTable(dbName, tableName);
      assertEquals("b",tbl.getSubPartition().getParSpaces().get("unknown1").get(1));
      assertEquals("c",tbl.getSubPartition().getParSpaces().get("unknown2").get(0));
      
      //TODO:add default partition

      // see that this fails properly
      addPartition2 = new AddPartitionDesc(dbName, "doesnotexist",subpd, true);
      task = TaskFactory.get(new DDLWork(addPartition2), hiveConf);
      task.initialize(hiveConf);
      assertEquals(1, task.execute());
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

}