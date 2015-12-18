package org.apache.hadoop.hive.serde2.protobuf.objectinspector;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.derby.iapi.util.ByteArray;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.io.BytesWritable;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class pbStructObjectInspector extends StructObjectInspector{

	public static class dataStruct{
		Object data = null;
		
		public dataStruct(/*boolean isDeserialized*/ Object data) {
			super();
			this.data = data;
		}

		public Object getData() {
			return data;
		}
		public void setData(Object data) {
			this.data = data;
		}
		
	}
	
	protected static class pbField implements StructField{

		private String name;
		private ObjectInspector oi;
		
		protected pbField(String name, ObjectInspector oi){
			this.name = name;
			this.oi = oi;
		}
		@Override
		public String getFieldName() {		
			return name;
		}

		@Override
		public ObjectInspector getFieldObjectInspector() {
			return oi;
		}

		@Override
		public String toString() {
			return (name + ": " + oi.toString());
		}
		
	}
	private List<pbField> fields;
	private Class<?> msgClass;
	
	private Descriptors.Descriptor pbTypeDesc = null;
	
	private Method parseFromMethod = null;
	
	Class<?>[] argsClass = null;
	
	byte [] buf = null;
	
	public Class<?> getMsgClass() {
		return msgClass;
	}
	public void setMsgClass(Class<?> msgClass) {
		this.msgClass = msgClass;
	}
	public pbStructObjectInspector(List<String> structFieldNames, 
		      List<ObjectInspector> structFieldObjectInspectors, Class<?> msgClass) {
		fields = new ArrayList<pbField> (structFieldNames.size());
		
		for(int i = 0; i < structFieldNames.size(); ++i){
			fields.add(new pbField(structFieldNames.get(i), structFieldObjectInspectors.get(i)));
		}
		this.msgClass = msgClass;
		
		
		 Method m = null;
			try {
				m = msgClass.getMethod("getDescriptor", (Class<?>[])null);
			} catch (SecurityException e1) {
				e1.printStackTrace();
			} catch (NoSuchMethodException e1) {
				e1.printStackTrace();
			}
		   
			try {
				pbTypeDesc = (Descriptors.Descriptor)m.invoke(null, (Object[])null);
			} catch (IllegalArgumentException e1) {
				e1.printStackTrace();
			} catch (IllegalAccessException e1) {
				e1.printStackTrace();
			} catch (InvocationTargetException e1) {
				e1.printStackTrace();
			}
			
			argsClass = new Class[1];
			argsClass[0] = byte[].class;
			
			try {
				parseFromMethod= msgClass.getMethod("parseFrom", argsClass);
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			}
			
	}
	
	
	
	public Descriptors.Descriptor getPbTypeDesc() {
		return pbTypeDesc;
	}
	@Override
	public List<? extends StructField> getAllStructFieldRefs() {
		return fields;
	}

	@Override
	public Object getStructFieldData(Object msg, StructField fieldRef) {
		
		Object data = ((dataStruct)msg).getData();
		
		if(data == null){//the row is null,so the field is null ,too;
			//System.err.println("the msg is null!" );
			return null;
		}
		
		if(isDeserialized(data)){
			//System.out.println("get pb field directly!");
			Map<FieldDescriptor, Object> pb_fields = ((GeneratedMessage)data).getAllFields();
			Object o = null;
			
			o = pb_fields.get(pbTypeDesc.findFieldByName(fieldRef.getFieldName()));
			
			if(o == null)
				;//System.out.println("this field : " + fieldRef.getFieldName() + " is null!" );
			
			return o;
		}
		else
		{	
			Object pbObj = null;
			try {
				byte[] rawbytes = ((BytesWritable)data).get();
				int sz = ((BytesWritable)data).getSize();
				
				if(sz <= 0){//the row size is small than 0,so return null;
					return null;
				}
				buf = new byte[sz];			
				System.arraycopy(rawbytes, 0, buf, 0,sz);
				
//				System.out.println("parseFromMethod Declare calss : " + parseFromMethod.getDeclaringClass().getCanonicalName());
//				
//				for(int i = 0; i < sz ;++i){				
//					System.out.println(buf[i]);
//				}
//				
//				System.out.println("msgClass : " + msgClass.getCanonicalName());
				
				pbObj = parseFromMethod.invoke(null, buf);
			//	System.out.println("deserialize pb msg~~~~ in getSructFieldData()");
				buf=null;
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
		 catch (SecurityException e1) {
			e1.printStackTrace();
		 }
			((dataStruct)msg).setData(pbObj);
			//System.out.println("pbObj class name : " + pbObj.getClass().getName());
			Map<FieldDescriptor, Object> pb_fields = ((GeneratedMessage)pbObj).getAllFields();
			Object o = null;
			o = pb_fields.get(pbTypeDesc.findFieldByName(fieldRef.getFieldName()));
			
			if(o == null)
				System.out.println("error ! can't find field : " + fieldRef.getFieldName());		
			
			return o;
		}
	}

	@Override
	public StructField getStructFieldRef(String fieldName) {
		return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields);
	}

	@Override
	public List<Object> getStructFieldsDataAsList(Object msg) {
		Object data = ((dataStruct)msg).getData();
		ArrayList<Object> rt = null;
		
		//System.out.println("pbOI,getStructFieldsDataAsList().....");
		
		if(isDeserialized(data)){
			rt = new ArrayList<Object>(fields.size());
			for(int i = 0;i < fields.size(); ++i){
				if(fields.get(i).getFieldObjectInspector().getCategory() == Category.PRIMITIVE){
					rt.add(getPrimitiveFieldFromPBObject(data, fields.get(i).getFieldName()));
				}
				else if(getStructFieldRef(fields.get(i).getFieldName()).getFieldObjectInspector().getCategory() == Category.LIST){
					rt.add(getRepeatedFieldFromPBObject(data,fields.get(i).getFieldName()));
				}
				else{
					System.out.println("[pbStructObjectInspcetor.getStructFieldsDataAsList() : do not support struct now!]");
					; 
					//do not support nest struct now!
				}
				
			}
			return rt;
		}
		else
		{
			//System.out.println("pbOI,getStructFieldsDataAsList().....do really deserialize");
			byte[] rawbytes = ((BytesWritable)data).get();
			int sz = ((BytesWritable)data).getSize();
			
			if(sz <= 0)
				return null;	
			
			buf = new byte[sz];
			System.arraycopy(rawbytes, 0, buf, 0,sz);
			
			try {
				
				//System.out.println("parseFromMethod Declare calss : " + parseFromMethod.getDeclaringClass().getCanonicalName());
				
//				for(int i = 0; i < sz ;++i){				
//					System.out.println(buf[i]);
//				}
//				
//				System.out.println("msgClass : " + msgClass.getCanonicalName());
//				
//				
				
				data = parseFromMethod.invoke(null, buf);
				
				//System.out.println("deserialize pb msg~~~~ done");
				
				buf = null;
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
		 catch (SecurityException e1) {
			e1.printStackTrace();
		} 
			
			
			
			rt = new ArrayList<Object>(fields.size());
			
			for(int i = 0;i < fields.size(); ++i){
				if(getStructFieldRef(fields.get(i).getFieldName()).getFieldObjectInspector().getCategory() == Category.PRIMITIVE){
					rt.add(getPrimitiveFieldFromPBObject(data, fields.get(i).getFieldName()));
				}
				else if(getStructFieldRef(fields.get(i).getFieldName()).getFieldObjectInspector().getCategory() == Category.LIST){
					rt.add(getRepeatedFieldFromPBObject(data,fields.get(i).getFieldName()));
				}
				else{
					System.out.println("[pbStructObjectInspcetor.getStructFieldsDataAsList() : do not support struct now!]");
					; 
					//do not support nest struct now!
				}
				
			}
			((dataStruct)msg).setData(data);
			
			return rt;
			
		}
	}

	@Override
	public String toString() {
		String rt = null;
		for(int i = 0; i < fields.size(); ++i){
			rt += fields.get(i).toString();
		}
		return rt;
	}

	@Override
	public Category getCategory() {
		return Category.STRUCT;	
	}

	@Override
	public String getTypeName() {
		
		return ObjectInspectorUtils.getStandardStructTypeName(this);
	}
	
	public boolean isDeserialized(Object data){
		return msgClass.isInstance(data);
	}
	
	//for list and struct,we change pb object to java object,so we can use StandardObjectInspector
	//there may be nest object

	private Object getPrimitiveFieldFromPBObject(Object pbmsg,String fieldName){
		
		Object o = null;
		Map<FieldDescriptor, Object> pb_fields = ((GeneratedMessage)pbmsg).getAllFields();
		
		o = pb_fields.get(pbTypeDesc.findFieldByName(fieldName));
		
		//assert (o != null);
			return o;
			
	}

private Object getRepeatedFieldFromPBObject(Object pbmsg,String fieldName){
		
		FieldDescriptor fdi = pbTypeDesc.findFieldByName(fieldName);
		
		ArrayList<Object> o = new ArrayList<Object>();
		for(int i = 0; i < ((GeneratedMessage)pbmsg).getRepeatedFieldCount(fdi);++i){
			o.add(((GeneratedMessage)pbmsg).getRepeatedField(fdi, i));
		}
			return o;	
	}

}
