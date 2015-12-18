package org.apache.hadoop.hive.serde2.protobuf;

import org.apache.hadoop.hive.conf.HiveConf;

import com.google.protobuf.*;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import org.apache.hadoop.hive.serde2.protobuf.objectinspector.pbStructObjectInspector;
import org.apache.hadoop.io.BytesWritable;

public class TestProtobufSerDe extends TestCase {
	HiveConf conf;
	private Properties createProperties() {
	    Properties tbl = new Properties();
	    
	    // Set the configuration parameters
	    tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
	    tbl.setProperty("columns",
	        "name,id,email,phone");
	    tbl.setProperty("columns.types",
	        "string:int:string:array<string>");
	    tbl.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
	    tbl.setProperty(Constants.PB_OUTER_CLASS_NAME, "pb");
	    tbl.setProperty(Constants.PB_MSG_NAME, "Person");
	    return tbl;
	  }
	
	public void testProtobufSerDe() throws Throwable{
		try {
	
	      // Create the SerDe
	      ProtobufSerDe serDe = new ProtobufSerDe();
	      Configuration conf = new Configuration();
	      Properties tbl = createProperties();
	      serDe.initialize(conf, tbl);

	      String name = new String("allisonzhao");
	      int id = 100;
	      String email = new String("jovz@vip.qq.com");
	      String phone1 = new String("110");
	      String phone2 = new String("119");
	      String[] phones = new String[]{"120","911"};//TODO check add reapeted addall() later

	      
	      conf = new HiveConf(ProtobufSerDe.class);
	      
	      Class<?> msgC = Class.forName("tdw.pb$Person");
	      
	      Class<?> builderC = Class.forName("tdw.pb$Person$Builder");
	      Method newBuilder = msgC.getMethod("newBuilder", (Class<?>[])null);
	      
	      Object builder = newBuilder.invoke(null, (Object[])null);
	      
	      Method setId = builderC.getMethod("setId", new Class<?>[]{int.class});
	      Method setName = builderC.getMethod("setName", new Class<?>[]{String.class});
	      Method setEmail = builderC.getMethod("setEmail", new Class<?>[]{String.class});
	      Method addPhone = builderC.getMethod("addPhone", new Class<?>[]{String.class});
	      
	      
	      
	      Method build = builderC.getMethod("build", (Class<?>[])null);
	      
	      Method writeTo = msgC.getMethod("writeTo", new Class<?>[]{com.google.protobuf.CodedOutputStream.class});
	      
	      Method getName = msgC.getMethod("getName", (Class<?>[])null);
	      setId.invoke(builder, new Object[]{id});
	      setName.invoke(builder, new Object[]{name});
	      setEmail.invoke(builder, new Object[]{email});
	      addPhone.invoke(builder, new Object[]{phone1});
	      addPhone.invoke(builder, new Object[]{phone2});
	      
	      
	      
	      Object p = build.invoke(builder, (Object[])null);//build the person msg
	      
	      
	      assertEquals("get name : ","allisonzhao", getName.invoke(p, (Object[])null));
	      
	      
	      ByteArrayOutputStream baos = new ByteArrayOutputStream();
	      
	      com.google.protobuf.CodedOutputStream cos = com.google.protobuf.CodedOutputStream.newInstance(baos);
	      writeTo.invoke(p, cos);
	      cos.flush();
	      
	      byte[] bytes = baos.toByteArray();
	      
	      
	      BytesWritable tmp = new BytesWritable(bytes);
	      
	      pbStructObjectInspector.dataStruct data = new pbStructObjectInspector.dataStruct(tmp);
	      
	      assert (((byte[])bytes).length != 0);
	      
	      System.out.println("tytes length : " + bytes.length);
	      
	      
	      
	      StructObjectInspector row = serDe.getObjectInspector();
	      
	      ((pbStructObjectInspector)row).setMsgClass(msgC);
	      
	      assertFalse(((pbStructObjectInspector)row).isDeserialized(data.getData()));
	      
	     //for serialize bytes get field,lazy mode
	      Object e = row.getStructFieldData(data, row.getStructFieldRef("email"));
	      
	      assertTrue(((pbStructObjectInspector)row).isDeserialized(data.getData()));
	      
	      assertEquals("jovz@vip.qq.com", ((String)e));
	      
	      
	      
	      //for msg object get field
	      
	      Object e2 = row.getStructFieldData(data, row.getStructFieldRef("name"));
	      assertEquals("allisonzhao", (String)e2);
	      
	      
	      //for repeated field
	      Object o3 = row.getStructFieldData(data, row.getStructFieldRef("phone"));
	      ObjectInspector phone_oi = row.getStructFieldRef("phone").getFieldObjectInspector();
	      
	      assertTrue(phone_oi instanceof StandardListObjectInspector);
	      
	      assertEquals(2, ((ListObjectInspector)phone_oi).getListLength(o3));
	      
	      assertEquals("119",(String)((ListObjectInspector)phone_oi).getListElement(o3, 1));
	     
	      
	      //test for pb serialize
	      BytesWritable bw = (BytesWritable)serDe.serialize(data,row);
	      
	      assertEquals(bytes.length, bw.getSize());
	     
	      for(int i = 0;i < bytes.length;++i){
	    	  assertEquals(bytes[i], bw.get()[i]);
	      }
	      
	    } catch (Throwable e) {
	      e.printStackTrace();
	      throw e;
	    }
	}
}
