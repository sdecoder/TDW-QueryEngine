package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.Externalizable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator.MapJoinObjectCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//Added by Brantzhang for Sorted Merge Join
/**
 * Used to hold key value pair in memory or persistent file.
 */
public class SortedKeyValueList<K,V> {
	
	  protected Log LOG = LogFactory.getLog(this.getClass().getName());
 
	  // default threshold for using main memory based ArrayList
	  private static final int THRESHOLD = 2500000;
	  
	  private int threshold;             // threshold to put data into persistent table instead
	  private ArrayList<Object> mKeyList;// main memory Key List
	  private ArrayList<V> mValueList;   // main memory Value List
	  private Object currentPersistentKey; // current key read from persistent file
	  private V currentPersistentValue;  // current value read from persistent file
	  private Object lastKey;
	  private File pFile;                // temp file holding the persistent data
	  private int mIndex;                // index for main memory key/value list	
	  private int pIndex;                // index for persistent key/value list
	  private int mSize;                 // number of key/value pairs in memory
	  private int pSize;                 // number of key/value pairs in persistent file
	  private ObjectInputStream oInput;  // object input stream
	  private ObjectOutputStream oOutput;  // object output stream
	  	
	  /**
	   * Constructor.
	   * @param threshold User specified threshold to store new values into persistent storage.
	   */
	  public SortedKeyValueList(int threshold) {
	    this.threshold = threshold;
	    this.mKeyList = new ArrayList<Object>(threshold);
	    this.mValueList = new ArrayList<V>(threshold);
	    this.currentPersistentKey = null;
	    this.currentPersistentValue = null;
	    this.lastKey = null;
	    this.pFile = null;
	    this.mIndex = 0;
	    this.pIndex = 0;
	    this.mSize = 0;
	    this.pSize = 0;
	    this.oInput = null;
	    this.oOutput = null;
	  }
	  
	  /**
	   * Constructor.
	   */
	  public SortedKeyValueList() {
	    this(THRESHOLD);
	  }
	 
	  /**
	   * comparing two keys
	   * @param k1
	   * @param k2
	   * @return comparing result, if k1>k2 return >0; if k1=k2 return 0, if k1<k2 return <0;
	   */
	  private int compareKeys (Object k1, Object k2) {
		return WritableComparator.get(((WritableComparable)k1).getClass()).compare((WritableComparable)k1, (WritableComparable)k2);
	  }
	  
	  /**
	   * Get the value based on the key. We try to get it from the main memory key/value list first.
	   * If it is not there, we will look up the persistent key/value list. 
	   * @param key
	   * @return Value corresponding to the key. If the key is not found, return null.
	   * @throws HiveException 
	   */
	  public V get(K key) throws HiveException  {
	    
		int compareResult;
		Object key0 = ((ArrayList<Object>)key).get(0);
		
		if(key0 == null){
			return null;
		}
		  
		if(lastKey != null){
		  compareResult = compareKeys(lastKey, key0);
	      if(compareResult > 0){
	        //The key comes from another file of the big table
	    	reset();
	      }
		}
	    		
	    //search in memory firstly
	    if(mIndex < mSize){
	      
	      //search in memory firstly
	      do{
	    	compareResult = compareKeys(mKeyList.get(mIndex), key0);
	    	if(compareResult == 0){//equal
	    		lastKey = key0;
	    		return mValueList.get(mIndex);
	    	}else if(compareResult > 0){//bigger
	    		lastKey = key0;
	    		return null;
	    	}else{
	    		mIndex++;
	    	}
	      }while(mIndex < mSize);
	      
	      //Begin to read the key/value pairs saved in persistent file
	      //Open the object input stream
	      if(oInput == null){
	    	  try {
	    		if(pFile == null){
	    			lastKey = key0;
	    			return null;
	    		}
	    			
				oInput = new ObjectInputStream( new FileInputStream(pFile));
			} catch (FileNotFoundException e) {
				LOG.warn(e.toString());
			    throw new HiveException(e);
			} catch (IOException e) {
				LOG.warn(e.toString());
			    throw new HiveException(e);
			}
	      }
	      
	      //Search in persistent file
	      if(pIndex < pSize){
	    	  
	    	//the first time reading persistent key/value pairs
	    	if(currentPersistentKey == null){
	    	  getNewPersistentKeyValue();  
	    	}
	    	
	    	do{
	    	  compareResult = compareKeys(currentPersistentKey, key0);
	    	  
		      if(compareResult == 0){
		    	lastKey = key0;
		    	return currentPersistentValue;
		      }else if(compareResult > 0){
		    	lastKey = key0;
		    	return null;
		      }else{
		    	getNewPersistentKeyValue();  
		      }
		    }while(pIndex<pSize);
	    	
	    	lastKey = key0;
	    	return null;
	      }else{
	    	  lastKey = key0;
	    	  return null;
	      }
	    }else{
	      
	      //Search in persistent file
		  if(pIndex < pSize){
			  
			//the first time reading persistent key/value pairs
		    if(currentPersistentKey == null){
		      getNewPersistentKeyValue();  
		    }
		    	
		    do{
		      compareResult = compareKeys(currentPersistentKey,key0);
		      
			  if(compareResult == 0){
				lastKey = key0;
			 	return currentPersistentValue;
			  }else if(compareResult > 0){
				lastKey = key0;
			   	return null;
			  }else{
				getNewPersistentKeyValue();  
			  }
			  
		    }while(pIndex<pSize);
		    
		    lastKey = key0;
		    return null;
		  }
		}
	    
	    lastKey = key0;
	    return null;
	  }
	  
	  /**
	   * reset the status
	   * @throws HiveException
	   */
	  private void reset() throws HiveException{
		  
		    //reset the persistent file
		    this.currentPersistentKey = null;
		    this.currentPersistentValue = null;
		    
		    if(oInput!=null){
		      try {
				oInput.close();
				oInput = new ObjectInputStream( new FileInputStream(pFile));
			  } catch (IOException e) {
				LOG.warn(e.toString());
			    throw new HiveException(e);
			  }
			  this.pIndex = 0;
		    }
		    
		    //reset the memory key/value list
		    this.mIndex = 0;
	  }
	  
	  /**
	   * get a persistent key/value pair
	   * @throws HiveException
	   */
	  private void getNewPersistentKeyValue() throws HiveException{
		  
		try {
			MapJoinObjectKey mjoKey = (MapJoinObjectKey)oInput.readObject();
			this.currentPersistentKey = mjoKey.getObj().get(0);
			//this.currentPersistentKey = ((KeySerializable)(oInput.readObject())).getKey();
			this.currentPersistentValue = (V)oInput.readObject();
			pIndex++;
		} catch (IOException e) {
			LOG.warn(e.toString());
		    throw new HiveException(e);
		} catch (ClassNotFoundException e) {
			LOG.warn(e.toString());
		    throw new HiveException(e);
		}
		
	  }
	  
	  /**
	   * Put the key value pair in the persistent file . It will first try to 
	   * put it into the main memory list. If the size exceeds the
	   * threshold, it will put it into the persistent file.
	   * @param key
	   * @param value
	   * @throws HiveException
	   */
	  public void put(K key, V value)  throws HiveException {
	    
		Object key0 = ((ArrayList<Object>)key).get(0);
		
		//put the key/value pair in memory
		if(mSize < threshold){
	      mSize++;
	      mKeyList.add(key0);
	      mValueList.add(value);
	      lastKey = key0;
	    }else{
	      //put the key/value pair in persistent file;
	      if(oOutput == null){
	    	  createPersistentFile();
	      }
	      
	      try {
	    	if(currentPersistentValue != null)
	    		oOutput.writeObject(currentPersistentValue);
	    	
	    	//oOutput.writeObject(new KeySerializable(key0));
	    	oOutput.writeObject(new MapJoinObjectKey(MapJoinOperator.metadataKeyTag, (ArrayList<Object>)key));
	    	oOutput.flush();
	    	currentPersistentValue = value;
			lastKey = key0;
			pSize++;
		  } catch (IOException e) {
			LOG.warn(e.toString());
		    throw new HiveException(e);
		  }
	    }
	  }
	  
	  /**
	   * does it equal the key put lastest?
	   * @param key
	   * @return
	   */
	  public boolean equalLastPutKey(K key){
		  
		  if(lastKey == null)
			  return false;
		  
		  if(compareKeys(((ArrayList<Object>)key).get(0), lastKey) == 0)
			  return true;
		  else
			  return false;
	  }
	  
	  /**
	   * save it to the lastest value;
	   * @param value
	   */
	  public void saveToLastPutValue(Object value) throws HiveException{
		  
		  //last key/value pair saved in memory
		  if(pFile==null){
			  try {
				((MapJoinObjectValue)(mValueList.get(mSize-1))).getObj().add((ArrayList<Object>)value);
			} catch (HiveException e) {
				LOG.warn(e.toString());
			    throw new HiveException(e);
			}
		  }else{//last key/value pair saved in memory
			  ((MapJoinObjectValue)currentPersistentValue).getObj().add((ArrayList<Object>)value);  
		  }
	  }
	  
	  /**
	   * create persistent file to store key/value pairs
	   * @throws HiveException
	   */
	  private void createPersistentFile() throws HiveException{
		  try{
			  
			if(pFile != null){
				pFile.delete();
			}
			
			//create a temporary file to store the key/value pairs peresistently
			pFile = File.createTempFile("SortedKeyValueList", ".tmp", new File("/tmp"));
			LOG.info("SortedKeyValueList created temp file " + pFile.getAbsolutePath());
			
			// Delete the temp file if the JVM terminate normally through Hadoop job kill command.
		    // Caveat: it won't be deleted if JVM is killed by 'kill -9'.
		    pFile.deleteOnExit(); 
		    
		    oOutput = new ObjectOutputStream(new FileOutputStream(pFile));
		  }catch (Exception e) {
		      LOG.warn(e.toString());
		      throw new HiveException(e);
		  } 
	  }
	  
	  /**
	   * Clean up the two key value lists. All elements in the main memory key/value list will be removed, and
	   * the persistent file will be destroyed (temporary file will be deleted).
	   */
	  public void clear() throws HiveException {
		  
		mKeyList.clear();
		mValueList.clear();
		mSize = 0;
		mIndex = 0;
		close();
	  }
	  
	  
	  /**
	   * Get the main memory key/value list capacity. 
	   * @return the maximum number of items can be put into main memory key/value list.
	   */
	  public int cacheSize() {
		  
	    return threshold;
	  }
	  
	  /**
	   * close the persistent file saving key/value lists
	   * @throws HiveException
	   */
	  public void closePFile() throws HiveException{
		  
		  if(pFile!=null){
			try {
			  if(currentPersistentValue != null){
				oOutput.writeObject(currentPersistentValue);  
				oOutput.flush();
			  }
				  
			  currentPersistentValue = null;
			  
			  oOutput.close();
			  oOutput = null;
			  
			  lastKey = null;
		    } catch (IOException e) {
			  LOG.warn(e.toString());
		      throw new HiveException(e);
		    }  
		  }
	  }
	  
	  /**
	   * Close the persistent file and clean it up.
	   * @throws HiveException
	   */
	  public void close() throws HiveException {
	    
	    if ( pFile != null ) {
	    	
	      try {
	        if ( oOutput != null )
	          oOutput.close();
	        
	        if( oInput != null)
	          oInput.close();
	      }  catch (Exception e) {
	        throw new HiveException(e);
	      }
	      
	      // delete the temporary file
	      pFile.delete();
	      
	      //clean up status
	      oOutput = null;
	      oInput  = null;
	      pFile = null;
	      pSize = 0;
	      pIndex = -1;
	      currentPersistentKey = null;
	      lastKey = null;
	      currentPersistentValue = null;
	    }
	  }
}
