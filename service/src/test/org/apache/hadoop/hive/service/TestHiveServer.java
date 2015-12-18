package org.apache.hadoop.hive.service;

import java.util.*;

import org.apache.hadoop.fs.Path;
import junit.framework.TestCase;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;

public class TestHiveServer extends TestCase {

  private HiveInterface client;
  private HiveServer server;
  private HSSessionManager sessionManager;
  private final static String host = "localhost";
  private final static int port = 10000;
  private Path dataFilePath;

  private static String tableName = "testhivedrivertable";
  private HiveConf conf;
  private boolean standAloneServer = false;
  private TTransport transport;

  public TestHiveServer(String name) {
    super(name);
    conf = new HiveConf(TestHiveServer.class);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    conf.set("hadoop.job.ugi", "tdwadmin,tdwadmin");
    dataFilePath = new Path(dataFileDir, "kv1.txt");
    // See data/conf/hive-site.xml
    String paramStr = System.getProperty("test.service.standalone.server");
    if (paramStr != null && paramStr.equals("true"))
      standAloneServer = true;
  }

  protected void setUp() throws Exception {
    super.setUp();
    if (standAloneServer) {
      try {

        transport = new TSocket(host, port);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new HiveClient(protocol);
        transport.open();
      }
      catch (Throwable e) {
        e.printStackTrace();
      }
    }
    else {
      sessionManager = new HSSessionManager();
      Thread t = new Thread(sessionManager);
      t.start();
      client = new HiveServer.HiveServerHandler(null, sessionManager);
    }
  }

  protected void tearDown() throws Exception {
    super.tearDown();
    if (standAloneServer) {
      transport.close();
    }
  }

  public void testExecute() throws Exception {
    try {
      client.audit("root", "tdwroot", "default_db");
      client.createSession("test_session");
    } catch (Throwable t) {
      t.printStackTrace();
    }
    
    try {
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }

    try {
      client.execute("create table " + tableName + " (num int)");
      client.execute("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);
      client.execute("select count(1) as cnt from " + tableName);
      String row = client.fetchOne();
      assertEquals(row, "500");
      
      Schema hiveSchema = client.getSchema();
      List<FieldSchema> listFields = hiveSchema.getFieldSchemas();
      assertEquals(listFields.size(), 1);
      assertEquals(listFields.get(0).getName(), "cnt");
      assertEquals(listFields.get(0).getType(), "bigint");
      
      Schema thriftSchema = client.getThriftSchema();
      List<FieldSchema> listThriftFields = thriftSchema.getFieldSchemas();
      assertEquals(listThriftFields.size(), 1);
      assertEquals(listThriftFields.get(0).getName(), "cnt");
      assertEquals(listThriftFields.get(0).getType(), "i64");
      
      client.execute("drop table " + tableName);
    }
    catch (Throwable t) {
      t.printStackTrace();
    } finally {
      client.dropSession("test_session", "default");
    }
  }

  public void testExecuteNocheck() throws Exception {
    try {
      client.audit("root", "tdwroot", "default_db");
      client.createSession("test_session");
    } catch (Throwable t) {
      t.printStackTrace();
    }
    
    try {
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }

    client.execute("create table " + tableName + " (num int)");
    client.execute("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);
    client.execute("select count(1) from " + tableName);
    String row = client.fetchOne();
    assertEquals(row, "500");
    client.execute("drop table " + tableName);
    client.dropSession("test_session", "default");
    if (transport != null)
      transport.close();
  }

  public void testNonHiveCommand() throws Exception {
    try {
      client.audit("root", "tdwroot", "default_db");
      client.createSession("test_session");
    } catch (Throwable t) {
      t.printStackTrace();
    }
    
    try {
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }

    client.execute("create table " + tableName + " (num int)");
    client.execute("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);
    
    // Command not part of HiveQL -  verify no results
    client.execute("SET hive.mapred.mode = nonstrict");
    
    Schema schema = client.getSchema();
    assertEquals(schema.getFieldSchemasSize(), 0);
    assertEquals(schema.getPropertiesSize(), 0);
    
    Schema thriftschema = client.getThriftSchema();
    assertEquals(thriftschema.getFieldSchemasSize(), 0);
    assertEquals(thriftschema.getPropertiesSize(), 0);
    
    assertEquals(client.fetchOne(), "");
    assertEquals(client.fetchN(10).size(), 0);
    assertEquals(client.fetchAll().size(), 0);
    
    // Execute Hive query and fetch
    client.execute("select * from " + tableName + " limit 10");
    String row = client.fetchOne();
    
    // Re-execute command not part of HiveQL - verify still no results
    client.execute("SET hive.mapred.mode = nonstrict");
    
    schema = client.getSchema();
    assertEquals(schema.getFieldSchemasSize(), 0);
    assertEquals(schema.getPropertiesSize(), 0);
    
    thriftschema = client.getThriftSchema();
    assertEquals(thriftschema.getFieldSchemasSize(), 0);
    assertEquals(thriftschema.getPropertiesSize(), 0);
    
    assertEquals(client.fetchOne(), "");
    assertEquals(client.fetchN(10).size(), 0);
    assertEquals(client.fetchAll().size(), 0);

    // Cleanup
    client.execute("drop table " + tableName);
    client.dropSession("test_session", "default");
  }
  
  /**
   * Test metastore call
   */
  public void testMetastore() throws Exception {
    try {
      client.audit("root", "tdwroot", "default_db");
      client.createSession("test_session");
    } catch (Throwable t) {
      t.printStackTrace();
    }
    
    try {
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }

    client.execute("create table " + tableName + " (num int)");
    List<String> tabs = client.get_tables("default_db", tableName);
    assertEquals(tabs.get(0), tableName);
    client.execute("drop table " + tableName);
    client.dropSession("test_session", "default");
  }

  public void testSessions() throws Exception {
    try {
      client.audit("root", "tdwroot", "default_db");
      client.createSession("test_session");
    } catch (Throwable t) {
      t.printStackTrace();
    }
    
    try {
      client.detachSession("test_session", "default");
      client.showSessions();
      client.requireSession("test_session", "default");
    } finally {
      client.dropSession("test_session", "default");
    }
  }
  
  /**
   * Test cluster status retrieval
   */
  public void testGetClusterStatus() throws Exception {
    try {
      client.audit("root", "tdwroot", "default_db");
      client.createSession("test_session");
    } catch (Throwable t) {
      t.printStackTrace();
    }
    
    HiveClusterStatus clusterStatus = client.getClusterStatus();
    assertNotNull(clusterStatus);
    assertTrue(clusterStatus.getTaskTrackers() >= 0);
    assertTrue(clusterStatus.getMapTasks() >= 0);
    assertTrue(clusterStatus.getReduceTasks() >= 0);
    assertTrue(clusterStatus.getMaxMapTasks() >= 0);
    assertTrue(clusterStatus.getMaxReduceTasks() >= 0);
    assertTrue(clusterStatus.getState() == JobTrackerState.INITIALIZING ||
               clusterStatus.getState() == JobTrackerState.RUNNING);
    client.dropSession("test_session", "default");
  }
  
  /** 
   *
   */
  public void testFetch() throws Exception {
    try {
      client.audit("root", "tdwroot", "default_db");
      client.createSession("test_session");
    } catch (Throwable t) {
      t.printStackTrace();
    }
    
    // create and populate a table with 500 rows.
    try {
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }
    client.execute("create table " + tableName + " (key int, value string)");
    client.execute("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);

    try {
    // fetchAll test
    client.execute("select key, value from " + tableName);
    assertEquals(client.fetchAll().size(), 500);
    assertEquals(client.fetchAll().size(), 0);

    // fetchOne test
    client.execute("select key, value from " + tableName);
    for (int i = 0; i < 500; i++) {
      String str = client.fetchOne();
      if (str.equals("")) {
        assertTrue(false);
      }
    }
    assertEquals(client.fetchOne(), "");

    // fetchN test
    client.execute("select key, value from " + tableName);
    assertEquals(client.fetchN(499).size(), 499);
    assertEquals(client.fetchN(499).size(), 1);
    assertEquals(client.fetchN(499).size(), 0);
    }
    catch (Throwable e) {
      e.printStackTrace();
    }
    client.dropSession("test_session", "default");
  }

  public void testDynamicSerde() throws Exception {
    try {
      client.audit("root", "tdwroot", "default_db");
      client.createSession("test_session");
    } catch (Throwable t) {
      t.printStackTrace();
    }
    
    try {
      client.execute("drop table " + tableName);
    } catch (Exception ex) {
    }

    client.execute("create table " + tableName + " (key int, value string)");
    client.execute("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);
    //client.execute("select key, count(1) from " + tableName + " where key > 10 group by key");
    String sql = "select key, value from " + tableName + " where key > 10";
    client.execute(sql);

    // Instantiate DynamicSerDe
    DynamicSerDe ds = new DynamicSerDe();
    Properties dsp = new Properties();
    dsp.setProperty(Constants.SERIALIZATION_FORMAT, org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol.class.getName());
    dsp.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME, "result");
    String serDDL = new String("struct result { ");
    List<FieldSchema> schema = client.getThriftSchema().getFieldSchemas();
    for (int pos = 0; pos < schema.size(); pos++) {
      if (pos != 0) 
          serDDL = serDDL.concat(",");
      serDDL = serDDL.concat(schema.get(pos).getType());
      serDDL = serDDL.concat(" ");
      serDDL = serDDL.concat(schema.get(pos).getName());
    }
    serDDL = serDDL.concat("}");

    dsp.setProperty(Constants.SERIALIZATION_DDL, serDDL);
    dsp.setProperty(Constants.SERIALIZATION_LIB, ds.getClass().toString());
    dsp.setProperty(Constants.FIELD_DELIM, "9");
    ds.initialize(new Configuration(), dsp);

    String row = client.fetchOne();
    Object o = ds.deserialize(new BytesWritable(row.getBytes()));

    assertEquals(o.getClass().toString(), "class java.util.ArrayList");
    List<?> lst = (List<?>)o;
    assertEquals(lst.get(0), 238);

    // TODO: serde doesn't like underscore  -- struct result { string _c0}
    sql = "select count(1) as c from " + tableName;
    client.execute(sql);
    row = client.fetchOne();

    serDDL = new String("struct result { ");
    schema = client.getThriftSchema().getFieldSchemas();
    for (int pos = 0; pos < schema.size(); pos++) {
      if (pos != 0) 
          serDDL = serDDL.concat(",");
      serDDL = serDDL.concat(schema.get(pos).getType());
      serDDL = serDDL.concat(" ");
      serDDL = serDDL.concat(schema.get(pos).getName());
    }
    serDDL = serDDL.concat("}");

    dsp.setProperty(Constants.SERIALIZATION_DDL, serDDL);
    // Need a new DynamicSerDe instance - re-initialization is not supported.
    ds = new DynamicSerDe();
    ds.initialize(new Configuration(), dsp);
    o = ds.deserialize(new BytesWritable(row.getBytes()));
    
    client.dropSession("test_session", "default");
  }
  
  public void testCompile() throws Exception {
    try {
      client.audit("root", "tdwroot", "default_db");
      client.createSession("test_session");
    } catch (Throwable t) {
      t.printStackTrace();
    }
    
    try {
      client.compile("drop table " + tableName);
    } catch (Exception ex) {
    }

    try {
      client.compile("create table " + tableName + " (num int)");
      client.compile("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);
      client.compile("select count(1) as cnt from " + tableName);
      client.compile("drop table " + tableName);
    }
    catch (Throwable t) {
      t.printStackTrace();
    } finally {
      client.dropSession("test_session", "default");
    }
  }
  public void testCompileFail() throws Exception {
    String res;
    try {
      client.audit("root", "tdwroot", "default_db");
      client.createSession("test_session");
    } catch (Throwable t) {
      t.printStackTrace();
    }
    
    try {
      client.compile("drop table " + tableName);
    } catch (Exception ex) {
    }

    try {
      res = client.compile("create table " + tableName + " (num int)");
      assertEquals(res, "success");
      res = client.compile("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);
      assertEquals(res, "success");
      res = client.compile("select count(*) as cnt from " + tableName);
      assertEquals(false, res.equals("succcess"));
      res = client.compile("drop table " + tableName);
      assertEquals(res, "success");
    } catch (HiveServerException e) {
      // it is ok
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      client.dropSession("test_session", "default");
    }
  }
}
