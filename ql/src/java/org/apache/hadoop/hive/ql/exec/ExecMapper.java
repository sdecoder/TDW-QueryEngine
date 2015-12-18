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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.mapredLocalWork;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.mapredLocalWork.HashMapJoinContext;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ExecMapper extends MapReduceBase implements Mapper {

  private MapOperator mo;
  private Map<String, FetchOperator> fetchOperators;
  private OutputCollector oc;
  private JobConf jc;
  private boolean abort = false;
  private Reporter rp;
  public static final Log l4j = LogFactory.getLog("ExecMapper");
  private static boolean done;

  // used to log memory usage periodically
  private MemoryMXBean memoryMXBean;
  private long numRows = 0;
  private long nextCntr = 1;
  
  //Added by Brantzhang for Hash map join Begin
  private String lastInputFile = null;
  private mapredLocalWork localWork = null;
  private LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> aliasHashPathMapping;
  //Added by Brantzhang for Hash map join End
  
  public void configure(JobConf job) {
    // Allocate the bean at the beginning - 
    memoryMXBean = ManagementFactory.getMemoryMXBean();
    l4j.info("maximum memory = " + memoryMXBean.getHeapMemoryUsage().getMax());
      
    try {
      l4j.info("conf classpath = " 
          + Arrays.asList(((URLClassLoader)job.getClassLoader()).getURLs()));
      l4j.info("thread classpath = " 
          + Arrays.asList(((URLClassLoader)Thread.currentThread().getContextClassLoader()).getURLs()));
    } catch (Exception e) {
      l4j.info("cannot get classpath: " + e.getMessage());
    }
    try {
      jc = job;
      // create map and fetch operators
      mapredWork mrwork = Utilities.getMapRedWork(job);
      mo = new MapOperator();
      mo.setConf(mrwork);
      // initialize map operator
      mo.setChildren(job);
      l4j.info(mo.dump(0));
      mo.initialize(jc, null);

      // initialize map local work
      //Modidifed by Brantzhang for hash map join begin
      //mapredLocalWork localWork = mrwork.getMapLocalWork(); 
      localWork = mrwork.getMapLocalWork();
      //Modidifed by Brantzhang for hash map join end
      if (localWork == null) {
        return;
      }
      fetchOperators = new HashMap<String, FetchOperator>();
      // create map local operators
      for (Map.Entry<String, fetchWork> entry : localWork.getAliasToFetchWork().entrySet()) {
        fetchOperators.put(entry.getKey(), new FetchOperator(entry.getValue(), job));
        l4j.info("fetchoperator for " + entry.getKey() + " created");
      }
      // initialize map local operators
      for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
        Operator<? extends Serializable> forwardOp = localWork.getAliasToWork().get(entry.getKey()); 
        // All the operators need to be initialized before process
        forwardOp.initialize(jc, new ObjectInspector[]{entry.getValue().getOutputObjectInspector()});
        l4j.info("fetchoperator for " + entry.getKey() + " initialized");
      }
      // defer processing of map local operators to first row if in case there is no input (??)
    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        // will this be true here?
        // Don't create a new object if we are already out of memory 
        throw (OutOfMemoryError) e; 
      } else {
        throw new RuntimeException ("Map operator initialization failed", e);
      }
    }

  }

  public void map(Object key, Object value,
                  OutputCollector output,
                  Reporter reporter) throws IOException {
    if(oc == null) {
      oc = output;
      rp = reporter;
      mo.setOutputCollector(oc);
      mo.setReporter(rp);
      //Removed by Brantzhang for hash map join begin
      /*
      // process map local operators
      if (fetchOperators != null) {
        try {
          mapredLocalWork localWork = mo.getConf().getMapLocalWork();
          int fetchOpNum = 0;
          for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {//逐个处理fetch operator，每个算子读取一个数据源
            int fetchOpRows = 0;
            String alias = entry.getKey();
            FetchOperator fetchOp = entry.getValue();
            Operator<? extends Serializable> forwardOp = localWork.getAliasToWork().get(alias); 

            while (true) {//一次读完该算子要读的所有的行
              InspectableObject row = fetchOp.getNextRow();
              if (row == null) {
                break;
              }
              fetchOpRows++;
              forwardOp.process(row.o, 0);
            }
            
            if (l4j.isInfoEnabled()) {
              l4j.info("fetch " + fetchOpNum++ + " processed " + fetchOpRows + " used mem: " + memoryMXBean.getHeapMemoryUsage().getUsed());
            }
          }
        } catch (Throwable e) {
          abort = true;
          if (e instanceof OutOfMemoryError) {
            // Don't create a new object if we are already out of memory 
            throw (OutOfMemoryError) e; 
          } else {
            throw new RuntimeException ("Map local work failed", e);
          }
        }
      }*/
    //Removed by Brantzhang for hash map join end
      
    //Added by Brantzhang for hash map join begin
    //之所以这么处理是由于单个Map任务可能会读入多个文件，假如这些文件没有变化，则
    if (localWork != null
    	&& (this.lastInputFile == null /*|| //如果这是本次读的第一个文件
    	(localWork.getInputFileChangeSensitive() && inputFileChanged())*/)) {//或者，该文件与上次读的文件不是同一个文件
      if(this.localWork.getHashMapjoinContext()==null){
    	  processOldMapLocalWork();  //Not hash map join  
      }else{
    	this.lastInputFile = HiveConf.getVar(jc, HiveConf.ConfVars.HADOOPMAPFILENAME);//获取本次正在读的文件的名字
        processMapLocalWork();    //hash map join
      }
      
    }
    //Added by Brantzhang for hash map join end
    }
    
    

    try {
      if (mo.getDone())
        done = true;
      else {
        // Since there is no concept of a group, we don't invoke startGroup/endGroup for a mapper
        mo.process((Writable)value);
        if (l4j.isInfoEnabled()) {
          numRows++;
          if (numRows == nextCntr) {
            long used_memory = memoryMXBean.getHeapMemoryUsage().getUsed();
            l4j.info("ExecMapper: processing " + numRows + " rows: used memory = " + used_memory);
            nextCntr = getNextCntr(numRows);
          }
        }
      }
    } catch (Throwable e) {
      abort = true;
      e.printStackTrace();
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory 
        throw (OutOfMemoryError) e; 
      } else {
        throw new RuntimeException (e.getMessage(), e);
      }
    }
  }
  
  //Added by Brantzhang for hash map join begin
   /**
     * For CombineFileInputFormat, the mapper's input file will be changed on the
     * fly. If the map local work has any mapping depending on the current
     * mapper's input file, the work need to clear context and re-initialization
     * after the input file changed. This is first introduced to process hash
     * map join. But it will not be used for now.
     * 
     * @return
     */
  private boolean inputFileChanged() {
    String currentInputFile = HiveConf.getVar(jc, HiveConf.ConfVars.HADOOPMAPFILENAME);
    if (this.lastInputFile == null
        || !this.lastInputFile.equals(currentInputFile)) {
      return true;
    }
    return false;
  }
  
  //处理fetchOperator
  private void processMapLocalWork() {
    // 处理map本地算子，实际上就是处理fetchOperator
	l4j.info("Begin to process map side computing!");
    if (fetchOperators != null) {
      //this.jc.setBoolean("hive.mapside.computing", true);
      try {
        int fetchOpNum = 0;
        for (Map.Entry<String, FetchOperator> entry : fetchOperators
            .entrySet()) {
          int fetchOpRows = 0;
          String alias = entry.getKey();
          FetchOperator fetchOp = entry.getValue();
          l4j.info("The table processed in the fetch operator: " + alias);
          
          fetchOp.clearFetchContext();//清理上下文
          setUpFetchOpContext(fetchOp, alias);//重新设定上下文
           
          Operator<? extends Serializable> forwardOp = localWork
              .getAliasToWork().get(alias);//将被转发到哪个算子
  
          while (true) {
            InspectableObject row = fetchOp.getNextRow();//循环读
            if (row == null) {
              forwardOp.close(false);
              break;
            }
            fetchOpRows++;
            forwardOp.process(row.o, 0);
            // check if any operator had a fatal error or early exit during
            // execution
            if (forwardOp.getDone()) {
              done = true;
              break;
            }
          }
  
          if (l4j.isInfoEnabled()) {
            l4j.info("fetch " + fetchOpNum++ + " processed " + fetchOpRows
                      + " used mem: "
                      + memoryMXBean.getHeapMemoryUsage().getUsed());
          }
        }
      } catch (Throwable e) {
        abort = true;
        if (e instanceof OutOfMemoryError) {
          throw (OutOfMemoryError) e;
        } else {
          throw new RuntimeException("Map local work failed", e);
        }
      }
    }
  }
  
  //for the none-hash map join
  private void processOldMapLocalWork(){
	// process map local operators
      if (fetchOperators != null) {
        try {
          mapredLocalWork localWork = mo.getConf().getMapLocalWork();
          int fetchOpNum = 0;
          for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
            int fetchOpRows = 0;
            String alias = entry.getKey();
            FetchOperator fetchOp = entry.getValue();
            Operator<? extends Serializable> forwardOp = localWork.getAliasToWork().get(alias); 

            while (true) {
              InspectableObject row = fetchOp.getNextRow();
              if (row == null) {
                break;
              }
              fetchOpRows++;
              forwardOp.process(row.o, 0);
            }
            
            if (l4j.isInfoEnabled()) {
              l4j.info("fetch " + fetchOpNum++ + " processed " + fetchOpRows + " used mem: " + memoryMXBean.getHeapMemoryUsage().getUsed());
            }
          }
        } catch (Throwable e) {
          abort = true;
          if (e instanceof OutOfMemoryError) {
            // Don't create a new object if we are already out of memory 
            throw (OutOfMemoryError) e; 
          } else {
            throw new RuntimeException ("Map local work failed", e);
          }
        }
      }
  }
  
  //该函数通过文件名判断出该文件所属的Hash分区，为此，不允许数据表中存在两级Hash分区
  private String tellHashParFromFileName(String fileName){
	  String hashPar = null;
	  if(!fileName.contains("Hash_"))
		  return null;
	  int begin = fileName.indexOf("Hash_");
	  hashPar = fileName.substring(begin, begin+9);
	  return hashPar;
  }
  
  //根据表名和正在处理的Hash分区名来获取需要读取的小表对应的文件
  List<Path> getAliasHashFiles(String hashPar, String refTableAlias, String alias){
	List<String> pathStr = aliasHashPathMapping.get(alias).get(hashPar);//应该读取的该所有对应的hash分区的文件
	List<Path> paths = new ArrayList<Path>();
	if(pathStr!=null) {
	  for (String p : pathStr) {
	    l4j.info("Loading file " + p + " for " + alias + ". (" + hashPar + ")");
	    paths.add(new Path(p));
	  }
	}
	return paths;
  }
    
  private void setUpFetchOpContext(FetchOperator fetchOp, String alias)
      throws Exception {
    //String currentInputFile = HiveConf.getVar(jc, HiveConf.ConfVars.HADOOPMAPFILENAME);//获取当前要处理的大表文件
    HashMapJoinContext hashMapJoinCxt = this.localWork.getHashMapjoinContext();
    if(hashMapJoinCxt != null){
    	l4j.info("The HashMapJoinContext is not null!");
    }
    this.aliasHashPathMapping = hashMapJoinCxt.getAliasHashParMapping();
    if(this.aliasHashPathMapping == null){
    	l4j.info("aliasHashPathMapping is null!");
    }
    //获知该文件所属的Hash分区名
    String par = tellHashParFromFileName(this.lastInputFile);
    l4j.info("The last input file: " + this.lastInputFile);
    l4j.info("The par being processed: " + par);
    if(par == null)
    	throw new Exception("The input file " + this.lastInputFile + " is not in a hash partition!");
    List<Path> aliasFiles =  getAliasHashFiles(par,               //Hash分区名
			hashMapJoinCxt.getMapJoinBigTableAlias(), //大表名
			alias);                                   //小表名
        
    l4j.info("The file processed in the fetch operator: " + aliasFiles.toArray().toString());
    Iterator<Path> iter = aliasFiles.iterator();//该map任务要处理的对应于小表alias的指定hash分区的所有目录对应的迭代器
    fetchOp.setupContext(iter);
  }    
  //Added by Brantzhang for hash map join end

  private long getNextCntr(long cntr) {
    // A very simple counter to keep track of number of rows processed by the reducer. It dumps
    // every 1 million times, and quickly before that
    if (cntr >= 1000000)
      return cntr + 1000000;
    
    return 10 * cntr;
  }

  public void close() {
    // No row was processed
    if(oc == null) {
      l4j.trace("Close called. no row processed by map.");
    }

    // detecting failed executions by exceptions thrown by the operator tree
    // ideally hadoop should let us know whether map execution failed or not
    try {
      mo.close(abort);
      if (fetchOperators != null) {
        mapredLocalWork localWork = mo.getConf().getMapLocalWork();
        for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
          Operator<? extends Serializable> forwardOp = localWork.getAliasToWork().get(entry.getKey()); 
          forwardOp.close(abort);
        }
      }
      
      if (l4j.isInfoEnabled()) {
        long used_memory = memoryMXBean.getHeapMemoryUsage().getUsed();
        l4j.info("ExecMapper: processed " + numRows + " rows: used memory = " + used_memory);
      }
      
      reportStats rps = new reportStats (rp);
      mo.preorderMap(rps);
      oc = null;
      mo.setOutputCollector(oc);
      return;
    } catch (Exception e) {
      if(!abort) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException ("Error while closing operators", e);
      }
    }
  }

  public static boolean getDone() {
    return done;
  }

  public static class reportStats implements Operator.OperatorFunc {
    Reporter rp;
    public reportStats (Reporter rp) {
      this.rp = rp;
    }
    public void func(Operator op) {
      Map<Enum, Long> opStats = op.getStats();
      for(Map.Entry<Enum, Long> e: opStats.entrySet()) {
          if(this.rp != null) {
              rp.incrCounter(e.getKey(), e.getValue());
          }
      }
    }
  }
}
