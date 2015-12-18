package org.apache.hadoop.hive.serde2.protobuf.objectinspector;

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;


public class pbListObjectInspector implements ListObjectInspector{

	ObjectInspector elementOI;
	protected pbListObjectInspector(ObjectInspector elementOi){
		this.elementOI = elementOi;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return ("List :" + elementOI.toString());
	}

	@Override
	public List<?> getList(Object data) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getListElement(Object data, int index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectInspector getListElementObjectInspector() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getListLength(Object data) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Category getCategory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTypeName() {
		// TODO Auto-generated method stub
		return null;
	}

}
