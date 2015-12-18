package org.apache.hadoop.hive.serde2.protobuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.protobuf.objectinspector.pbObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.protobuf.objectinspector.pbStructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

public class ProtobufSerDe implements SerDe{

	private pbStructObjectInspector rowOI= null;
	/**
	 * we do the really deserialize in the pbStructObjectInspector
	 */
	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		
		pbStructObjectInspector.dataStruct data = new pbStructObjectInspector.dataStruct(blob);
		
		return data;
	}

	@Override
	public StructObjectInspector getObjectInspector() throws SerDeException {
		return rowOI;
	}

	@Override
	public void initialize(Configuration conf, Properties tbl)
			throws SerDeException {
		 // We can get the table definition from tbl.
	    
	    // Read the configuration parameters
	    String columnNameProperty = tbl.getProperty(Constants.LIST_COLUMNS);
	    String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
	    String pb_msg_name = tbl.getProperty(Constants.PB_MSG_NAME);
	    String pb_outer_name = tbl.getProperty(Constants.PB_OUTER_CLASS_NAME);
	    
	    
	    // Parse the configuration parameters
	    List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
	    List<TypeInfo> columnTypes = TypeInfoUtils
	        .getTypeInfosFromTypeString(columnTypeProperty);
	    assert columnNames.size() == columnTypes.size();
	    
	    ArrayList<ObjectInspector> typeList = new ArrayList<ObjectInspector>(columnTypes.size());
	    
	    for(int i = 0; i < columnTypes.size();++i){
	    	typeList.add(pbObjectInspectorFactory.getObjectInspectorFromTypeInfo(columnTypes.get(i)));
	    }
	    
	    Class<?> msg = null;
	    try {
	    		
	    	   //URL[] urls = { new URL("file:/data/allison/tdw_v2.0_pb/qe/data/pb_test_jar/pb_test.jar") };
	           //ClassLoader loader = new URLClassLoader(urls);
	           //msg = loader.loadClass("tdw.pb$"+"Person");//TODO:note,the class name is exactly same as pb generated code
	           msg= Class.forName("tdw." + pb_outer_name + "$" + pb_msg_name);//loader.loadClass("tdw.pb$" + pb_msg_name);
	           System.out.println("load class : "  + msg.getName() + " OK");
	           //ClassLoader loader = Thread.currentThread().getContextClassLoader()
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
//		} catch (MalformedURLException e) {
//			e.printStackTrace();
//		}
	    rowOI = pbObjectInspectorFactory.getPbStructObjectInspector(columnNames, typeList, msg);
	}

	@Override
	public String toString() {
		return rowOI.getTypeName();
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return BytesWritable.class;
	}

	
	BytesWritable serializeCache = new BytesWritable();
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	com.google.protobuf.CodedOutputStream cos = com.google.protobuf.CodedOutputStream.newInstance(baos);
	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector)
			throws SerDeException {
		/**
		 * TODO : optimizer as this: 
		 * we should check if the src table is a protobuf table too,if so ,we should not deserailze,and write it directly!
		 * this happens when: from pb1 insert table pb2 select *;
		 */
		//System.out.println("will serialize the obj : " + obj.getClass().getName() + "in OI : " + objInspector.getClass().getName());
		
	    if (objInspector.getCategory() != Category.STRUCT) {
	      throw new SerDeException(getClass().toString() 
	          + " can only serialize struct types, but we got: " 
	          + objInspector.getTypeName());
	    }
	    
	    // Prepare the field ObjectInspectors
	    StructObjectInspector soi = (StructObjectInspector)objInspector;
	    
	    //dest fields
	    List<? extends StructField> destfields = getObjectInspector().getAllStructFieldRefs();//the data insert to the table should be the same as dest table     
	    
	    //src fields
	    List<? extends StructField> srcfields = soi.getAllStructFieldRefs();
	    
	    //System.out.println("get data list from obj and will serialize it to pb format");	    
	    
	    List<Object> list = soi.getStructFieldsDataAsList(obj);
	        
	    assert list.size() == destfields.size();
	    assert list.size() == srcfields.size();
	      
	    /*
	     * we must use the pb descriptor to find each field' exactly pb type,so we can use the right protobuf output api 
	     *
	     *TODO: we should do as less as possible because this function will called every row,so we can move the 
	     *      java reflect code to a static block for this class
	     */

		try {
			cos.flush();
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		
	  	    
//	    if(soi.equals(getObjectInspector()) && obj instanceof pbStructObjectInspector.dataStruct){//pb to pb insert optimizer
//	    	System.out.println("pb table to pb table select * insert optimizer!");
//	    	serializeCache.set((BytesWritable)((pbStructObjectInspector.dataStruct)obj).getData());
//	    	return serializeCache;
//	    }    
	    
	    baos.reset();
	    
		//System.out.println("type Desc to String : " + ((pbStructObjectInspector)(getObjectInspector())).getPbTypeDesc().getFullName());
	    
	    Descriptors.FieldDescriptor fd = null;
	    ObjectInspector srcOi = null;
	    
	    ListObjectInspector listOi = null;
	    ObjectInspector elemOi = null; 
	    Text t = null;
	    try {
	    	
	    	for(int i = 0;i < srcfields.size(); ++i){
	    		//for null field,we skip it
	    		if(list.get(i) == null){
	    			//System.out.println("this field is null: do not write to pb file" + destfields.get(i).getFieldName());
	    			continue;
	    		}
	    		else
	    		{
	    			;//System.out.println("will write field : " + destfields.get(i).getFieldName());
	    		}
	    		
	    		fd = ((pbStructObjectInspector)(getObjectInspector())).getPbTypeDesc().findFieldByName(destfields.get(i).getFieldName());
	    		
	    		if(fd == null)
	    			throw new SerDeException("do not find fild describe: "  + destfields.get(i).getFieldName() + " in pb msg describe");

	    		srcOi = srcfields.get(i).getFieldObjectInspector();

	    		//System.out.println("write pb field ...." + fd.getFullName());

	    		//System.out.println("src type: " + srcfields.get(i).getFieldObjectInspector().getTypeName() + " to dest pb type : " + fd.getType().name());


	    		switch(fd.getType()){
	    		case DOUBLE:
	    			if(!fd.isRepeated()){

	    				cos.writeDouble(fd.getNumber(),((Double)((DoubleObjectInspector)srcOi).get(list.get(i))).doubleValue());

	    			}else
	    			{
	    				//assert list.get(i) instanceof List<?>;
	    				listOi = (ListObjectInspector)srcOi;
	    				elemOi = listOi.getListElementObjectInspector();
	    				for (Object element: listOi.getList(list.get(i))){
	    					cos.writeDouble(fd.getNumber(),((Double)((DoubleObjectInspector)elemOi).get(element)).doubleValue());
	    				}
	    			}
	    			break;
	    		case FLOAT:
	    			if(!fd.isRepeated()){
	    				cos.writeFloat(fd.getNumber(),((Float)((FloatObjectInspector)srcOi).get(list.get(i))).floatValue());
	    			}else
	    			{
	    				listOi = (ListObjectInspector)srcOi;
	    				elemOi = listOi.getListElementObjectInspector();
	    				for (Object element: listOi.getList(list.get(i))){
	    					cos.writeFloat(fd.getNumber(),((Float)((FloatObjectInspector)elemOi).get(element)).floatValue());
	    				}
	    			}
	    			break;
	    		case INT64:
	    			if(!fd.isRepeated()){
	    				cos.writeInt64(fd.getNumber(),((Long)((LongObjectInspector)srcOi).get(list.get(i))).longValue());
	    			}else
	    			{
	    				listOi = (ListObjectInspector)srcOi;
	    				elemOi = listOi.getListElementObjectInspector();
	    				for (Object element: listOi.getList(list.get(i))){
	    					cos.writeInt64(fd.getNumber(),((Long)((LongObjectInspector)elemOi).get(element)).longValue());
	    				}
	    			}
	    			break;
	    		case UINT64:
	    			if(!fd.isRepeated()){
	    				cos.writeUInt64(fd.getNumber(),((Long)((LongObjectInspector)srcOi).get(list.get(i))).longValue());
	    			}else
	    			{
	    				listOi = (ListObjectInspector)srcOi;
	    				elemOi = listOi.getListElementObjectInspector();
	    				for (Object element: listOi.getList(list.get(i))){
	    					cos.writeUInt64(fd.getNumber(),((Long)((LongObjectInspector)elemOi).get(element)).longValue());
	    				}
	    			}
	    			break;
	    		case INT32:
	    			if(!fd.isRepeated()){

	    				cos.writeInt32(fd.getNumber(),((Integer)((IntObjectInspector)srcOi).get(list.get(i))).intValue());

	    			}else
	    			{
	    				listOi = (ListObjectInspector)srcOi;
	    				elemOi = listOi.getListElementObjectInspector();
	    				for (Object element: listOi.getList(list.get(i))){
	    					cos.writeInt32(fd.getNumber(),((Integer)((IntObjectInspector)elemOi).get(element)).intValue());
	    				}
	    			}
	    			break;
	    		case FIXED64:
	    			if(!fd.isRepeated()){
	    				cos.writeFixed64(fd.getNumber(),((Long)((LongObjectInspector)srcOi).get(list.get(i))).longValue());
	    			}else
	    			{
	    				listOi = (ListObjectInspector)srcOi;
	    				elemOi = listOi.getListElementObjectInspector();
	    				for (Object element: listOi.getList(list.get(i))){
	    					cos.writeFixed64(fd.getNumber(),((Long)((LongObjectInspector)elemOi).get(element)).longValue());
	    				}
	    			}
	    			break;
	    		case FIXED32:
	    			if(!fd.isRepeated()){
	    				cos.writeFixed32(fd.getNumber(),((Integer)((IntObjectInspector)srcOi).get(list.get(i))).intValue());
	    			}else
	    			{
	    				listOi = (ListObjectInspector)srcOi;
	    				elemOi = listOi.getListElementObjectInspector();
	    				for (Object element: listOi.getList(list.get(i))){
	    					cos.writeFixed32(fd.getNumber(),((Integer)((IntObjectInspector)elemOi).get(element)).intValue());
	    				}
	    			}
	    			break;
	    		case BOOL:
	    			if(!fd.isRepeated()){
	    				cos.writeBool(fd.getNumber(),((Boolean)((BooleanObjectInspector)srcOi).get(list.get(i))).booleanValue());
	    			}else
	    			{
	    				listOi = (ListObjectInspector)srcOi;
	    				elemOi = listOi.getListElementObjectInspector();
	    				for (Object element: listOi.getList(list.get(i))){
	    					cos.writeBool(fd.getNumber(),((Boolean)((BooleanObjectInspector)elemOi).get(element)).booleanValue());
	    				}
	    			}
	    			break;
	    		case STRING:{
	    			
	    			if(!fd.isRepeated()){
	    				t = ((StringObjectInspector)srcOi).getPrimitiveWritableObject(list.get(i));
	    				cos.writeString(fd.getNumber(),t.toString());
	    			}else
	    			{
	    				listOi = (ListObjectInspector)srcOi;
	    				elemOi = listOi.getListElementObjectInspector();
	    				for (Object element: listOi.getList(list.get(i))){
	    					t = ((StringObjectInspector)elemOi).getPrimitiveWritableObject(element);
	    					cos.writeString(fd.getNumber(),t.toString());
	    				}
	    			}
	    			break;
	    		}
	    		case GROUP:
	    			//nest type ,not support now
	    			break;
	    		case MESSAGE:
	    			//nest type ,not support now
	    			break;
	    		case BYTES:
	    			//bytes type, not support now
	    			break;
	    		case UINT32:
	    			if(!fd.isRepeated()){
	    				cos.writeUInt32(fd.getNumber(),((Integer)((IntObjectInspector)srcOi).get(list.get(i))).intValue());
	    			}else
	    			{
	    				listOi = (ListObjectInspector)srcOi;
	    				elemOi = listOi.getListElementObjectInspector();
	    				for (Object element: listOi.getList(list.get(i))){
	    					cos.writeUInt32(fd.getNumber(),((Integer)((IntObjectInspector)elemOi).get(element)).intValue());
	    				}
	    			}
	    			break;
	    		case ENUM:
	    			//not support now
	    			break;
	    		case SFIXED32:
	    			if(!fd.isRepeated()){
	    				cos.writeSFixed32(fd.getNumber(),((Integer)((IntObjectInspector)srcOi).get(list.get(i))).intValue());
	    			}else
	    			{
	    				listOi = (ListObjectInspector)srcOi;
	    				elemOi = listOi.getListElementObjectInspector();
	    				for (Object element: listOi.getList(list.get(i))){
	    					cos.writeSFixed32(fd.getNumber(),((Integer)((IntObjectInspector)elemOi).get(element)).intValue());
	    				}
	    			}
	    			break;
	    		case SFIXED64:
	    			if(!fd.isRepeated()){
	    				cos.writeSFixed64(fd.getNumber(),((Long)((LongObjectInspector)srcOi).get(list.get(i))).longValue());
	    			}else
	    			{
	    				listOi = (ListObjectInspector)srcOi;
	    				elemOi = listOi.getListElementObjectInspector();
	    				for (Object element: listOi.getList(list.get(i))){
	    					cos.writeSFixed64(fd.getNumber(),((Long)((LongObjectInspector)elemOi).get(element)).longValue());
	    				}
	    			}
	    			break;
	    		case SINT32:
	    			if(!fd.isRepeated()){
	    				cos.writeSInt32(fd.getNumber(),((Integer)((IntObjectInspector)srcOi).get(list.get(i))).intValue());
	    			}else
	    			{
	    				listOi = (ListObjectInspector)srcOi;
	    				elemOi = listOi.getListElementObjectInspector();
	    				for (Object element: listOi.getList(list.get(i))){
	    					cos.writeSInt32(fd.getNumber(),((Integer)((IntObjectInspector)elemOi).get(element)).intValue());
	    				}
	    			}
	    			break;
	    		case SINT64:
	    			if(!fd.isRepeated()){
	    				cos.writeSInt64(fd.getNumber(),((Long)((LongObjectInspector)srcOi).get(list.get(i))).longValue());
	    			}else
	    			{
	    				listOi = (ListObjectInspector)srcOi;
	    				elemOi = listOi.getListElementObjectInspector();
	    				for (Object element: listOi.getList(list.get(i))){
	    					cos.writeSInt64(fd.getNumber(),((Long)((LongObjectInspector)elemOi).get(element)).longValue());
	    				}
	    			}
	    			break;	 
	    		default:
	    			System.out.println("[ProtobufSerDe.serialize()] serde type error!");

	    		}

	    	}
	    	//we do not write unknow fields because TDW always use the new API

	    	cos.flush();
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	    baos.toByteArray();
	    serializeCache.set(baos.toByteArray(), 0,
	    		baos.toByteArray().length);

	    //System.out.println("baos.array.length : " + baos.toByteArray().length);
	    //System.out.println("bytewritable.array.length : " + serializeCache.get().length);
	    //System.out.println("bytewritable.getsize : " + serializeCache.getSize());
	    return serializeCache;


	}

}
