package org.apache.hadoop.hive.serde2.protobuf.objectinspector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import com.google.protobuf.GeneratedMessage;

public class pbObjectInspectorFactory {
	
	 static HashMap<ArrayList<Object>, pbStructObjectInspector> cachedPbStructObjectInspector =
		    new HashMap<ArrayList<Object>, pbStructObjectInspector>(); 
		    
		  public static pbStructObjectInspector getPbStructObjectInspector(List<String> structFieldNames, 
		      List<ObjectInspector> structFieldObjectInspectors, Class<?> msgClass) {
		    ArrayList<Object> signature = new ArrayList<Object>();
		    signature.add(structFieldNames);
		    signature.add(structFieldObjectInspectors);
		    signature.add(msgClass);
		
		    pbStructObjectInspector result = cachedPbStructObjectInspector.get(signature);
		    if (result == null) {
		      result = new pbStructObjectInspector(structFieldNames, structFieldObjectInspectors, msgClass);
		      cachedPbStructObjectInspector.put(signature, result);
		    }
		    return result;
		  }

		  static HashMap<ArrayList<Object>, pbListObjectInspector> cachedPbListObjectInspector =
		    new HashMap<ArrayList<Object>, pbListObjectInspector>(); 
		  public static pbListObjectInspector getPbListObjectInspector( 
		      ObjectInspector listElementObjectInspector) {
		    ArrayList<Object> signature = new ArrayList<Object>();
		    signature.add(Category.LIST);
		    signature.add(listElementObjectInspector);
		   
		    pbListObjectInspector result = cachedPbListObjectInspector.get(signature);
		    if (result == null) {
		      result = new pbListObjectInspector(listElementObjectInspector);
		      cachedPbListObjectInspector.put(signature, result);
		    }
		    return result;
		  }

		  public static ObjectInspector getObjectInspectorFromTypeInfo(TypeInfo ti){

			  try{
			  switch(ti.getCategory()){
			  case STRUCT:
				  //ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors)
				  //TODO:for support nested type
				  throw new Exception("do not support nested type : " + ti.getTypeName());
				  //break;
			  case LIST:
				  // ListTypeInfo lti = (ListTypeInfo)ti; 
				  return ObjectInspectorFactory.getStandardListObjectInspector(getObjectInspectorFromTypeInfo(((ListTypeInfo)ti).getListElementTypeInfo()));
				  //break;
			  case PRIMITIVE:
				  return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(((PrimitiveTypeInfo)ti).getPrimitiveCategory());
				  // break;
			  default:
				  throw new Exception("SerDe do not support type : " + ti.getTypeName());
			  }
			  }catch(Exception e){
				  e.getStackTrace();
			  }
			return null;

		  }


}
