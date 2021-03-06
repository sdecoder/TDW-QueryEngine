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

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.net.URI;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.tdw_sys_fields_statistics;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.histogramDesc;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.plan.sampleDesc;
import org.apache.hadoop.hive.ql.plan.staticsInfoCollectWork;
import org.apache.hadoop.hive.ql.plan.statsDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.io.*;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.varia.NullAppender;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class ExecDriver extends Task<mapredWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  transient protected JobConf job;

  /**
   * Constructor when invoked from QL
   */
  public ExecDriver() {
    super();
  }

  public static String getResourceFiles(Configuration conf, SessionState.ResourceType t) {
    // fill in local files to be added to the task environment
    SessionState ss = SessionState.get();
    Set<String> files = (ss == null) ? null : ss.list_resource(t, null);
    if (files != null) {
      ArrayList<String> realFiles = new ArrayList<String>(files.size());
      for (String one : files) {
        try {
          realFiles.add(Utilities.realFile(one, conf));
        } catch (IOException e) {
          throw new RuntimeException("Cannot validate file " + one
              + "due to exception: " + e.getMessage(), e);
        }
      }
      return StringUtils.join(realFiles, ",");
    } else {
      return "";
    }
  }

  private void initializeFiles(String prop, String files) {
    if (files != null && files.length() > 0) {
      job.set(prop, files);
      ShimLoader.getHadoopShims().setTmpFiles(prop, files);
    }
  }

  /**
   * Initialization when invoked from QL
   */
  // ++++++++++++++++++++++++++++++++++++++++++++++++++++++
  // Author : wangyouwei
  Hive _hive;
  // 	++++++++++++++++++++++++++++++++++++++++++++++++++++++
  public void initialize(HiveConf conf) {
    super.initialize(conf);
	// ++++++++++++++++++++++++++++++++++++++++++++++++++++++
	// Author : wangyouwei
	// TODO
	try {
	    _hive = Hive.get(conf);
	} catch (HiveException e) {
	    e.printStackTrace();
	}

	// ++++++++++++++++++++++++++++++++++++++++++++++++++++++
	
    job = new JobConf(conf, ExecDriver.class);
    // NOTE: initialize is only called if it is in non-local mode.
    // In case it's in non-local mode, we need to move the SessionState files
    // and jars to jobConf.
    // In case it's in local mode, MapRedTask will set the jobConf.
    //
    // "tmpfiles" and "tmpjars" are set by the method ExecDriver.execute(),
    // which will be called by both local and NON-local mode.
    String addedFiles = getResourceFiles(job, SessionState.ResourceType.FILE);
    if (StringUtils.isNotBlank(addedFiles)) {
      HiveConf.setVar(job, ConfVars.HIVEADDEDFILES, addedFiles);
    }
    String addedJars = getResourceFiles(job, SessionState.ResourceType.JAR);
    if (StringUtils.isNotBlank(addedJars)) {
      HiveConf.setVar(job, ConfVars.HIVEADDEDJARS, addedJars);
    }
  }

  /**
   * Constructor/Initialization for invocation as independent utility
   */
  public ExecDriver(mapredWork plan, JobConf job, boolean isSilent)
      throws HiveException {
    setWork(plan);
    this.job = job;
    LOG = LogFactory.getLog(this.getClass().getName());
    console = new LogHelper(LOG, isSilent);
  }

  /**
   * A list of the currently running jobs spawned in this Hive instance that is
   * used to kill all running jobs in the event of an unexpected shutdown -
   * i.e., the JVM shuts down while there are still jobs running.
   */
  public static HashMap<String, String> runningJobKillURIs = new HashMap<String, String>();

  /**
   * In Hive, when the user control-c's the command line, any running jobs
   * spawned from that command line are best-effort killed.
   *
   * This static constructor registers a shutdown thread to iterate over all the
   * running job kill URLs and do a get on them.
   *
   */
  static {
    if (new org.apache.hadoop.conf.Configuration().getBoolean(
        "webinterface.private.actions", false)) {
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          for (Iterator<String> elems = runningJobKillURIs.values().iterator(); elems
              .hasNext();) {
            String uri = elems.next();
            try {
              System.err.println("killing job with: " + uri);
              java.net.HttpURLConnection conn = (java.net.HttpURLConnection) 
                new java.net.URL(uri).openConnection();
              conn.setRequestMethod("POST");
              int retCode = conn.getResponseCode();
              if (retCode != 200) {
                System.err.println("Got an error trying to kill job with URI: "
                    + uri + " = " + retCode);
              }
            } catch (Exception e) {
              System.err.println("trying to kill job, caught: " + e);
              // do nothing
            }
          }
        }
      });
    }
  }

  /**
   * from StreamJob.java
   */
  public void jobInfo(RunningJob rj) {
    if (job.get("mapred.job.tracker", "local").equals("local")) {
      console.printInfo("Job running in-process (local Hadoop)");
    } else {
      String hp = job.get("mapred.job.tracker");
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setTaskProperty(
            SessionState.get().getQueryId(), getId(),
            Keys.TASK_HADOOP_ID, rj.getJobID());
      }
      console.printInfo("Starting Job = " + rj.getJobID() + ", Tracking URL = "
          + rj.getTrackingURL());
      console.printInfo("Kill Command = "
          + HiveConf.getVar(job, HiveConf.ConfVars.HADOOPBIN)
          + " job  -Dmapred.job.tracker=" + hp + " -kill " + rj.getJobID());
    }
  }

  /**
   * from StreamJob.java
   */
  public RunningJob jobProgress(JobClient jc, RunningJob rj) throws IOException {
    String lastReport = "";
    SimpleDateFormat dateFormat
        = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
    long reportTime = System.currentTimeMillis();
    long maxReportInterval = 60 * 1000; // One minute
    while (!rj.isComplete()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      rj = jc.getJob(rj.getJobID());
      String report = " map = " + Math.round(rj.mapProgress() * 100) + "%,  reduce ="
          + Math.round(rj.reduceProgress() * 100) + "%";

      if (!report.equals(lastReport)
          || System.currentTimeMillis() >= reportTime + maxReportInterval) {

        String output = dateFormat.format(Calendar.getInstance().getTime()) + report;
        SessionState ss = SessionState.get();
        if (ss != null) {
          ss.getHiveHistory().setTaskCounters(
              SessionState.get().getQueryId(), getId(), rj);
          ss.getHiveHistory().setTaskProperty(
              SessionState.get().getQueryId(), getId(),
              Keys.TASK_HADOOP_PROGRESS, output);
          ss.getHiveHistory().progressTask(
              SessionState.get().getQueryId(), this);
        }
        console.printInfo(output);
        lastReport = report;
        reportTime = System.currentTimeMillis();
      }
    }
    return rj;
  }

  /**
   * Estimate the number of reducers needed for this job, based on job input,
   * and configuration parameters.
   * @return the number of reducers.
   */
  public int estimateNumberOfReducers(HiveConf hive, JobConf job, mapredWork work) throws IOException {
    if (hive == null) {
      hive = new HiveConf();
    }
    long bytesPerReducer = hive.getLongVar(HiveConf.ConfVars.BYTESPERREDUCER);
    int maxReducers = hive.getIntVar(HiveConf.ConfVars.MAXREDUCERS);
    long totalInputFileSize = getTotalInputFileSize(job, work);

    LOG.info("BytesPerReducer=" + bytesPerReducer + " maxReducers=" + maxReducers
        + " totalInputFileSize=" + totalInputFileSize);

    int reducers = (int)((totalInputFileSize + bytesPerReducer - 1) / bytesPerReducer);
    reducers = Math.max(1, reducers);
    reducers = Math.min(maxReducers, reducers);
    return reducers;
  }

  /**
   * Set the number of reducers for the mapred work.
   */
  protected void setNumberOfReducers() throws IOException {
    // this is a temporary hack to fix things that are not fixed in the compiler
    Integer numReducersFromWork = work.getNumReduceTasks();

    if(work.getReducer() == null) {
      console.printInfo("Number of reduce tasks is set to 0 since there's no reduce operator");
      work.setNumReduceTasks(Integer.valueOf(0));
    } else {
      if (numReducersFromWork >= 0) {
        console.printInfo("Number of reduce tasks determined at compile time: " + work.getNumReduceTasks());
      } else if (job.getNumReduceTasks() > 0) {
        int reducers = job.getNumReduceTasks();
        work.setNumReduceTasks(reducers);
        console.printInfo("Number of reduce tasks not specified. Defaulting to jobconf value of: " + reducers);
      } else {
        int reducers = estimateNumberOfReducers(conf, job, work);
        work.setNumReduceTasks(reducers);
        console.printInfo("Number of reduce tasks not specified. Estimated from input data size: " + reducers);

      }
      console.printInfo("In order to change the average load for a reducer (in bytes):");
      console.printInfo("  set " + HiveConf.ConfVars.BYTESPERREDUCER.varname + "=<number>");
      console.printInfo("In order to limit the maximum number of reducers:");
      console.printInfo("  set " + HiveConf.ConfVars.MAXREDUCERS.varname + "=<number>");
      console.printInfo("In order to set a constant number of reducers:");
      console.printInfo("  set " + HiveConf.ConfVars.HADOOPNUMREDUCERS + "=<number>");
    }
  }

  /**
   * Calculate the total size of input files.
   * @param job the hadoop job conf.
   * @return the total size in bytes.
   * @throws IOException
   */
  public long getTotalInputFileSize(JobConf job, mapredWork work) throws IOException {
    long r = 0;
    // For each input path, calculate the total size.
    for (String path: work.getPathToAliases().keySet()) {
      try {
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(job);
        ContentSummary cs = fs.getContentSummary(p);
        r += cs.getLength();
      } catch (IOException e) {
        LOG.info("Cannot get size of " + path + ". Safely ignored.");
      }
    }
    return r;
  }

  /**
   * Execute a query plan using Hadoop
   */
  public int execute() {
		// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++z
		// TODO:
		// Author: Wangyouwei
	    String _cbrSwtich=null;
	    if(SessionState.get()==null)
	    	_cbrSwtich=null;
	    else
		    _cbrSwtich = SessionState.get().getConf().get(ToolBox.CBR_SWITCH_ATTR);
		if (_cbrSwtich != null /* joeyli add begin */
			&& _cbrSwtich.trim().equals("true") /* joeyli add end */) {
		    System.out.println("Computation balance is enabled, initiate processing");
		    LinkedHashMap<String, Operator<? extends Serializable>> _aliasWork = work.getAliasToWork();
		    int axSize = _aliasWork.keySet().size();
		    if (axSize == 1) {
			//System.out.println("[Debug]:ExecDriver.execute().(axSize == 1) Ready to hack query plan tree");
			String _switch_str_ = SessionState.get().getConf().get(ToolBox.STATISTICS_COLUMNS_ATTR);
			int _sampleWindow_ = SessionState.get().getConf().getInt(SampleOperator.STATISTICS_SAMPLING_WINDOW_ATTR, 100000);

			ArrayList<Integer> _switch_ = new ArrayList<Integer>();
			StringBuilder _sbFieldNamesFromSchema_ = new StringBuilder();
			StringBuilder _sbFieldTypesFromSchema_ = new StringBuilder();
			StringBuilder _sb_ = new StringBuilder();
			ArrayList<String> _allFieldNames_ = new ArrayList<String>();

			Iterator<String> _is = _aliasWork.keySet().iterator();
			while (_is.hasNext()) {
			    _sb_.append(_is.next());
			}
			String _key = _sb_.toString();
			//System.out.println("[Trace] Table name is " + _key);
			String _cbr_flushFileURI_ = _key + ToolBox.dotDelimiter
				+ java.util.UUID.randomUUID().toString();
			job.set(ComputationBalancerReducer.CBR_FLUSHFILEURI_ATTR, _cbr_flushFileURI_);

			//System.out.println("[Trace] Output command is "
			//	+ "hadoop fs -cat /tmp/hive-wangyouwei/" + _cbr_flushFileURI_ + "/*");
			Operator<? extends Serializable> _tblScanOp_ = _aliasWork.get(_key);
			
			try {
			    URI _dataStoreURI_ = _hive.getTable(SessionState.get().getDbName(), _key).getDataLocation();
			    if (_dataStoreURI_ == null) {
				//System.out.println("[Trace] The datastore uri is "
				//	+ null);
			    } else {
				//System.out.println("[Trace] The datastore uri is "
				//	+ _dataStoreURI_.toString());
				job.set(org.apache.hadoop.hive.ql.exec.ComputationBalancerReducer.CBR_TABLEDATALOCATION_ATTR, _dataStoreURI_.toString());
			    }
			} catch (HiveException e) {
			    e.printStackTrace();
			}

			try {
			    // "statistics.columns" = "col1,col2,col3,...,coln"
			    List<FieldSchema> _lfs_ = _hive.getTable(SessionState.get().getDbName(), _key).getCols();

			    if (_switch_str_ == null || _switch_str_.equals("")) {
				int _fldCounter = _lfs_.size();
				//System.out.println("[Debug] Field counter for this table is "
				//	+ _fldCounter);
				for (int i = 0; i < _fldCounter; i++) {
				    _switch_.add(Integer.valueOf(i));
				}
			    } else {

				StringTokenizer _st_ = new StringTokenizer(_switch_str_, ",");
				while (_st_.hasMoreTokens()) {
				    String _colx = _st_.nextToken();
				    String _colx_sub_ = _colx.substring("col".length());
				    _switch_.add(Integer.valueOf(_colx_sub_));

				}
			    }
			    // Name and type

			    for (FieldSchema fieldSchema : _lfs_) {
				_sbFieldNamesFromSchema_.append(fieldSchema.getName()
					+ ToolBox.commaDelimiter);
				_sbFieldTypesFromSchema_.append(fieldSchema.getType()
					+ ToolBox.commaDelimiter);
			    }

			    //System.out.println("[Trace] Showing all switchs:");
			    for (Integer _i : _switch_) {
				//System.out.print(_i + ",");
			    }
			   // System.out.println("");
			   // System.out.println("[Trace] Showing all fieldNames by schema: ");
			   // System.out.println("\t " + _sbFieldNamesFromSchema_);
			   // System.out.println("[Trace] Showing all fieldTypes by schema: ");
			   // System.out.println("\t " + _sbFieldTypesFromSchema_);

			} catch (Exception e) {
			    e.printStackTrace();
			}

			//System.out.println("[TRACE] CBR is required, initializing operation...");
			job.set(ToolBox.TABLE_HEADER_NAMES_ATTR, _sbFieldNamesFromSchema_.toString());
			job.set(ToolBox.TABLE_HEADER_TYPES_ATTR, _sbFieldTypesFromSchema_.toString());
			statsDesc _statsdesc = new statsDesc();
			_statsdesc.setTableName(_key);
			job.set(org.apache.hadoop.hive.ql.exec.ComputationBalancerReducer.CBR_TABLENAME_ATTR, _statsdesc.getTableName());

			try {

			    Vector<StructField> _vs = _hive.getTable(SessionState.get().getDbName(), _key).getFields();
			    //System.out.println("[Trace] Getting all fields names:");
			    for (StructField _sf : _vs) {
				_allFieldNames_.add(_sf.getFieldName());
				//System.out.println("[FieldName] " + _sf.getFieldName());
			    }

			    _statsdesc.setSelStar(false);
			    _statsdesc.setFieldNames(_allFieldNames_);
			    _statsdesc.setFieldStatsSwitch(_switch_);

			} catch (Exception e) {
			    e.printStackTrace();
			}

			{
			    boolean _return = false;
			    for (Integer _i : _switch_) {
				if (_i >= _allFieldNames_.size()) {
				    System.out.println("[ERROR]: col" + _i
					    + " specified is out of bounce");
				    _return = true;
				}
			    }
			    if (_return) {
				return -1; // failed
			    }
			}

			// / we create our own exprnodedesc here!
			ArrayList<exprNodeDesc> _colList_ = new ArrayList<exprNodeDesc>();
			try {
			    
			    Table _tbl = _hive.getTable(SessionState.get().getDbName(), _key);		    
			    List<FieldSchema> _lfs_ =_tbl.getCols();
			    for (Integer _i_ : _switch_) {
				FieldSchema _fs  = _lfs_.get(_i_);
				exprNodeColumnDesc _endesc_ = new exprNodeColumnDesc(
					TypeInfoFactory.getPrimitiveTypeInfo(_fs.getType()) , 
					_fs.getName(),
					_key, 
					_tbl.isPartitionKey(_fs.getName()));
				_colList_.add(_endesc_);
			    }
			    

			    // for (FieldSchema _f : _lfs_) {
			    //
			    // boolean _continue_ = true;
			    // for (Integer _ : _switch_) {
			    // if
			    // (_f.getName().equalsIgnoreCase(_allFieldNames_.get(_))) {
			    // _continue_ = false;
			    // }
			    // }
			    // if (_continue_) {
			    // continue;
			    // }
			    // exprNodeColumnDesc _endesc_ = new exprNodeColumnDesc(
			    // TypeInfoFactory.getPrimitiveTypeInfo(_f.getType()) ,
			    // _f.getName(),
			    // _key,
			    // _tbl.isPartitionKey(_f.getName()));
			    // _colList_.add(_endesc_);
			    // //System.out.println("exprNodeColumnDesc is " +
			    // _endesc_.getExprString());
			    // }		    
			   
			    
			} catch (Exception e) {
			    e.printStackTrace();
			}

			// pipeline: stats => sample => histogram
			StatsCollectionOperator _statsOp = (StatsCollectionOperator) (OperatorFactory.getAndMakeChild(_statsdesc, _tblScanOp_));

			sampleDesc _sampleDesc = new sampleDesc();
			_sampleDesc.setTableName(_key);
			 _statsdesc.setColList(_colList_);
			_sampleDesc.setTableFieldNames(_allFieldNames_);
			_sampleDesc.setSwitch(_switch_);
			_sampleDesc.setNumSampleRecords(_sampleWindow_);
			SampleOperator _sampleOp = (SampleOperator) (OperatorFactory.getAndMakeChild(_sampleDesc, _statsOp));

			histogramDesc _histogramDesc = new histogramDesc();
			_histogramDesc.setTableName(_key);
			_histogramDesc.setColList(_colList_);
			_histogramDesc.setFieldNames(_allFieldNames_);
			_histogramDesc.setSwitch(_switch_);
			HistogramOperator _histogramOp = (HistogramOperator) (OperatorFactory.getAndMakeChild(_histogramDesc, _sampleOp));

			//try {
			//    FileSystem fs = FileSystem.get(job);
			//    Path planPath = new Path("/tmp/debug/explain.txt");
			//    FSDataOutputStream out = fs.create(planPath);
			//    Utilities.serializeMapRedWork(work, out);
			//} catch (Exception e) {
			//    e.printStackTrace();
			//}

		    }
		}

		// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    try {
      setNumberOfReducers();
    } catch(IOException e) {
      String statusMesg = "IOException while accessing HDFS to estimate the number of reducers: "
        + e.getMessage();
      console.printError(statusMesg, "\n"
                         + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return 1;
    }

    String invalidReason = work.isInvalid();
    if (invalidReason != null) {
      throw new RuntimeException("Plan invalid, Reason: " + invalidReason);
    }


    String hiveScratchDir = HiveConf.getVar(job, HiveConf.ConfVars.SCRATCHDIR);
    String jobScratchDirStr = hiveScratchDir + File.separator + Utilities.randGen.nextInt();
    Path   jobScratchDir = new Path(jobScratchDirStr);
    String emptyScratchDirStr = null;
    Path   emptyScratchDir    = null;

    int numTries = 3;
    while (numTries > 0) {
      emptyScratchDirStr = hiveScratchDir + File.separator + Utilities.randGen.nextInt();
      emptyScratchDir = new Path(emptyScratchDirStr);

      try {
        FileSystem fs = emptyScratchDir.getFileSystem(job);
        fs.mkdirs(emptyScratchDir);
        break;
      } catch (Exception e) {
        if (numTries > 0)
          numTries--;
        else
          throw new RuntimeException("Failed to make dir " + emptyScratchDir.toString() + " : " + e.getMessage());
      }
    }

    FileOutputFormat.setOutputPath(job, jobScratchDir);
    job.setMapperClass(ExecMapper.class);

    job.setMapOutputKeyClass(HiveKey.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(work.getNumReduceTasks().intValue());
    job.setReducerClass(ExecReducer.class);

    job.setInputFormat(org.apache.hadoop.hive.ql.io.HiveInputFormat.class);

    // No-Op - we don't really write anything here ..
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Transfer HIVEAUXJARS and HIVEADDEDJARS to "tmpjars" so hadoop understands it
    String auxJars = HiveConf.getVar(job, HiveConf.ConfVars.HIVEAUXJARS);
    String addedJars = HiveConf.getVar(job, HiveConf.ConfVars.HIVEADDEDJARS);
    if (StringUtils.isNotBlank(auxJars) || StringUtils.isNotBlank(addedJars)) {
      String allJars =
        StringUtils.isNotBlank(auxJars)
        ? (StringUtils.isNotBlank(addedJars) ? addedJars + "," + auxJars : auxJars)
        : addedJars;
      LOG.info("adding libjars: " + allJars);
      initializeFiles("tmpjars", allJars);
    }

    // Transfer HIVEADDEDFILES to "tmpfiles" so hadoop understands it
    String addedFiles = HiveConf.getVar(job, HiveConf.ConfVars.HIVEADDEDFILES);
    if (StringUtils.isNotBlank(addedFiles)) {
      initializeFiles("tmpfiles", addedFiles);
    }

    int returnVal = 0;
    RunningJob rj = null, orig_rj = null;
    boolean success = false;

    try {
      addInputPaths(job, work, emptyScratchDirStr);
      Utilities.setMapRedWork(job, work);

      // remove the pwd from conf file so that job tracker doesn't show this logs
      String pwd = job.get(HiveConf.ConfVars.METASTOREPWD.varname);
      if (pwd != null)
        job.set(HiveConf.ConfVars.METASTOREPWD.varname, "HIVE");
      JobClient jc = new JobClient(job);

      // make this client wait if job trcker is not behaving well.
      Throttle.checkJobTracker(job, LOG);

      orig_rj = rj = jc.submitJob(job);
      // replace it back
      if (pwd != null)
        job.set(HiveConf.ConfVars.METASTOREPWD.varname, pwd);

      // add to list of running jobs so in case of abnormal shutdown can kill
      // it.
      runningJobKillURIs.put(rj.getJobID(), rj.getTrackingURL()
          + "&action=kill");

      jobInfo(rj);
      rj = jobProgress(jc, rj);

      if(rj == null) {
        // in the corner case where the running job has disappeared from JT memory
        // remember that we did actually submit the job.
        rj = orig_rj;
        success = false;
      } else {
        success = rj.isSuccessful();
      }
   // joeyli added for statics information collection begin
      if (success && _cbrSwtich != null /*joeyli add begin */ &&  _cbrSwtich.trim().equals("true") /*joeyli add end*/) {
          try{
        	  SessionState.get().getConf().set(ToolBox.CBR_SWITCH_ATTR,"");
        	  String cbrDirPath=job.get(ComputationBalancerReducer.CBR_FLUSHFILEURI_ATTR);
        	  if(cbrDirPath!=null && !cbrDirPath.equals("") )
        	  {
              staticsInfoCollectWork work=new staticsInfoCollectWork("/tmp/hive-wangyouwei/"+cbrDirPath,false);
              staticsInfoCollectTask task=new staticsInfoCollectTask();
              task.initialize(SessionState.get().getConf()) ;
              task.setWork(work);
              task.execute();
              
              // ��ӡ������Ϣ
              /*
         	    List<tdw_sys_fields_statistics> fields=Hive.get().get_fields_statistics_multi("*",-1);
         	    for (tdw_sys_fields_statistics field:fields )
         	    {
         	    	Hive.skewstat st=Hive.get().getSkew(field.getStat_table_name(),field.getStat_field_name());
         	        if(st==Hive.skewstat.skew)
         	        {
         	        	console.printInfo(field.getStat_table_name()+"."+field.getStat_field_name()+"="+"skew");
         	        }
         	        else if (st==Hive.skewstat.noskew)
         	        {
         	        	console.printInfo(field.getStat_table_name()+"."+field.getStat_field_name()+"="+"noskew ");
         	        }        	
         	        else
         	        {
         	        	console.printInfo(field.getStat_table_name()+"."+field.getStat_field_name()+"="+"unkonw");
         	        }
         	        
         	        Hive.mapjoinstat canMapJoin=Hive.get().canTableMapJoin(field.getStat_table_name());
         	        console.printInfo(field.getStat_table_name()+" canMapJoin="+canMapJoin);
         	    }*/
        	  }
              

          }catch(Exception e){
          	console.printError("Exception:" + e.toString());
          }    	  
      }

   // joeyli added for statics information collection end
   
      String statusMesg = "Ended Job = " + rj.getJobID();
      if (!success) {
        statusMesg += " with errors";
        returnVal = 2;
        console.printError(statusMesg);
      } else {
        console.printInfo(statusMesg);
      }
    } catch (Exception e) {
      String mesg = " with exception '" + Utilities.getNameMessage(e) + "'";
      if (rj != null) {
        mesg = "Ended Job = " + rj.getJobID() + mesg;
      } else {
        mesg = "Job Submission failed" + mesg;
      }
      // Has to use full name to make sure it does not conflict with
      // org.apache.commons.lang.StringUtils
      console.printError(mesg, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));

      success = false;
      returnVal = 1;
    } finally {
      Utilities.clearMapRedWork(job);
      try {
        FileSystem fs = jobScratchDir.getFileSystem(job);
        fs.delete(jobScratchDir, true);
        fs.delete(emptyScratchDir, true);
        if (returnVal != 0 && rj != null) {
          rj.killJob();
        }
        runningJobKillURIs.remove(rj.getJobID());
      } catch (Exception e) {
      }
    }

    try {
      if (rj != null) {
        if(work.getAliasToWork() != null) {
          for(Operator<? extends Serializable> op:
                work.getAliasToWork().values()) {
            op.jobClose(job, success);
          }
        }
        if(work.getReducer() != null) {
          work.getReducer().jobClose(job, success);
        }
      }
    } catch (Exception e) {
      // jobClose needs to execute successfully otherwise fail task
      if(success) {
        success = false;
        returnVal = 3;
        String mesg = "Job Commit failed with exception '" + Utilities.getNameMessage(e) + "'";
        console.printError(mesg, "\n"
                           + org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    }

    return (returnVal);
  }

  private static void printUsage() {
    System.out
        .println("ExecDriver -plan <plan-file> [-jobconf k1=v1 [-jobconf k2=v2] ...] "
            + "[-files <file1>[,<file2>] ...]");
    System.exit(1);
  }

  public static void main(String[] args) throws IOException, HiveException {

    String planFileName = null;
    ArrayList<String> jobConfArgs = new ArrayList<String>();
    boolean isSilent = false;
    String files = null;

    try {
      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("-plan")) {
          planFileName = args[++i];
          System.out.println("plan = " + planFileName);
        } else if (args[i].equals("-jobconf")) {
          jobConfArgs.add(args[++i]);
        } else if (args[i].equals("-silent")) {
          isSilent = true;
        } else if (args[i].equals("-files")) {
          files = args[++i];
        }
      }
    } catch (IndexOutOfBoundsException e) {
      System.err.println("Missing argument to option");
      printUsage();
    }

    // If started from main(), and isSilent is on, we should not output
    // any logs.
    // To turn the error log on, please set -Dtest.silent=false
    if (isSilent) {
      BasicConfigurator.resetConfiguration();
      BasicConfigurator.configure(new NullAppender());
    }

    if (planFileName == null) {
      System.err.println("Must specify Plan File Name");
      printUsage();
    }

    JobConf conf = new JobConf(ExecDriver.class);
    for (String one : jobConfArgs) {
      int eqIndex = one.indexOf('=');
      if (eqIndex != -1) {
        try {
          conf.set(one.substring(0, eqIndex), URLDecoder.decode(one
              .substring(eqIndex + 1), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
          System.err.println("Unexpected error " + e.getMessage()
              + " while encoding " + one.substring(eqIndex + 1));
          System.exit(3);
        }
      }
    }

    if (files != null) {
      conf.set("tmpfiles", files);
    }

    URI pathURI = (new Path(planFileName)).toUri();
    InputStream pathData;
    if (StringUtils.isEmpty(pathURI.getScheme())) {
      // default to local file system
      pathData = new FileInputStream(planFileName);
    } else {
      // otherwise may be in hadoop ..
      FileSystem fs = FileSystem.get(conf);
      pathData = fs.open(new Path(planFileName));
    }

    // workaround for hadoop-17 - libjars are not added to classpath. this
    // affects local
    // mode execution
    boolean localMode = HiveConf.getVar(conf, HiveConf.ConfVars.HADOOPJT)
        .equals("local");
    if (localMode) {
      String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);
      String addedJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEADDEDJARS);
      try {
        // see also - code in CliDriver.java
        ClassLoader loader = conf.getClassLoader();
        if (StringUtils.isNotBlank(auxJars)) {
          loader = Utilities.addToClassPath(loader, StringUtils.split(auxJars, ","));
        }
        if (StringUtils.isNotBlank(addedJars)) {
          loader = Utilities.addToClassPath(loader, StringUtils.split(addedJars, ","));
        }
        conf.setClassLoader(loader);
        // Also set this to the Thread ContextClassLoader, so new threads will inherit
        // this class loader, and propagate into newly created Configurations by those
        // new threads.
        Thread.currentThread().setContextClassLoader(loader);
      } catch (Exception e) {
        throw new HiveException(e.getMessage(), e);
      }
    }

    mapredWork plan = Utilities.deserializeMapRedWork(pathData, conf);
    ExecDriver ed = new ExecDriver(plan, conf, isSilent);
    int ret = ed.execute();
    if (ret != 0) {
      System.out.println("Job Failed");
      System.exit(2);
    }
  }

  /**
   * Given a Hive Configuration object - generate a command line fragment for
   * passing such configuration information to ExecDriver
   */
  public static String generateCmdLine(HiveConf hconf) {
    try {
      StringBuilder sb = new StringBuilder();
      Properties deltaP = hconf.getChangedProperties();
      boolean localMode = hconf.getVar(HiveConf.ConfVars.HADOOPJT).equals(
          "local");
      String hadoopSysDir = "mapred.system.dir";
      String hadoopWorkDir = "mapred.local.dir";

      for (Object one : deltaP.keySet()) {
        String oneProp = (String) one;

        if (localMode
            && (oneProp.equals(hadoopSysDir) || oneProp.equals(hadoopWorkDir)))
          continue;

        String oneValue = deltaP.getProperty(oneProp);

        sb.append("-jobconf ");
        sb.append(oneProp);
        sb.append("=");
        sb.append(URLEncoder.encode(oneValue, "UTF-8"));
        sb.append(" ");
      }

      // Multiple concurrent local mode job submissions can cause collisions in
      // working dirs
      // Workaround is to rename map red working dir to a temp dir in such a
      // case
      if (localMode) {
        sb.append("-jobconf ");
        sb.append(hadoopSysDir);
        sb.append("=");
        sb.append(URLEncoder.encode(hconf.get(hadoopSysDir) + "/"
            + Utilities.randGen.nextInt(), "UTF-8"));

        sb.append(" ");
        sb.append("-jobconf ");
        sb.append(hadoopWorkDir);
        sb.append("=");
        sb.append(URLEncoder.encode(hconf.get(hadoopWorkDir) + "/"
            + Utilities.randGen.nextInt(), "UTF-8"));
      }

      return sb.toString();
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public boolean hasReduce() {
    mapredWork w = getWork();
    return w.getReducer() != null;
  }

  private boolean isEmptyPath(JobConf job, String path) throws Exception {
    Path dirPath = new Path(path);
    FileSystem inpFs = dirPath.getFileSystem(job);

    if (inpFs.exists(dirPath)) {
      FileStatus[] fStats = inpFs.listStatus(dirPath);
      if (fStats.length > 0)
        return false;
    }
    return true;
  }

  /**
   * Handle a empty/null path for a given alias
   */
  private int addInputPath(String path, JobConf job, mapredWork work, String hiveScratchDir, int numEmptyPaths, boolean isEmptyPath,
                           String alias)
    throws Exception {
    // either the directory does not exist or it is empty
    assert path == null || isEmptyPath;

    // The input file does not exist, replace it by a empty file
    Class<? extends HiveOutputFormat> outFileFormat = null;

    if (isEmptyPath){
    	LOG.info("EmptyPath: " + path);
    	//LOG.info(work.getPathToPartitionInfo().get(path));
    	//LOG.info(work.getPathToPartitionInfo().get(path).getTableDesc().toString());
    	outFileFormat = work.getPathToPartitionInfo().get(path).getTableDesc().getOutputFileFormatClass();
    }
    else
      outFileFormat = (Class<? extends HiveOutputFormat>)(HiveSequenceFileOutputFormat.class);

    String newFile = hiveScratchDir + File.separator + (++numEmptyPaths);
    Path newPath = new Path(newFile);
    LOG.info("Changed input file to " + newPath.toString());

    // toggle the work
    LinkedHashMap<String, ArrayList<String>> pathToAliases = work.getPathToAliases();
    if (isEmptyPath) {
      assert path != null;
      pathToAliases.put(newPath.toUri().toString(), pathToAliases.get(path));
      pathToAliases.remove(path);
    }
    else {
      assert path == null;
      ArrayList<String> newList = new ArrayList<String>();
      newList.add(alias);
      pathToAliases.put(newPath.toUri().toString(), newList);
    }

    work.setPathToAliases(pathToAliases);

    LinkedHashMap<String,partitionDesc> pathToPartitionInfo = work.getPathToPartitionInfo();
    if (isEmptyPath) {
      pathToPartitionInfo.put(newPath.toUri().toString(), pathToPartitionInfo.get(path));
      pathToPartitionInfo.remove(path);
    }
    else {
      partitionDesc pDesc = work.getAliasToPartnInfo().get(alias).clone();
      Class<? extends InputFormat>      inputFormat = SequenceFileInputFormat.class;
      pDesc.getTableDesc().setInputFileFormatClass(inputFormat);
      pathToPartitionInfo.put(newPath.toUri().toString(), pDesc);
    }
    work.setPathToPartitionInfo(pathToPartitionInfo);

    String onefile = newPath.toString();
    // change by joeyli for column store begin
    job.setBoolean("NeedPostfix", false);
    RecordWriter recWriter = outFileFormat.newInstance().getHiveRecordWriter(job, newPath, Text.class, false, new Properties(), null);
    recWriter.close(false);
    job.setBoolean("NeedPostfix", true);
    // change by joeyli for column store end    
    FileInputFormat.addInputPaths(job, onefile);
    return numEmptyPaths;
  }

  private void addInputPaths(JobConf job, mapredWork work, String hiveScratchDir) throws Exception {
    int numEmptyPaths = 0;

    List<String> pathsProcessed = new ArrayList<String>();

    // AliasToWork contains all the aliases
    for (String oneAlias : work.getAliasToWork().keySet()) {
      LOG.info("Processing alias " + oneAlias);
      List<String> emptyPaths     = new ArrayList<String>();

      // The alias may not have any path
      String path = null;
      for (String onefile : work.getPathToAliases().keySet()) {
        List<String> aliases = work.getPathToAliases().get(onefile);
        if (aliases.contains(oneAlias)) {
          path = onefile;

          // Multiple aliases can point to the same path - it should be processed only once
          if (pathsProcessed.contains(path))
            continue;
          pathsProcessed.add(path);

          LOG.info("Adding input file " + path);

          if (!isEmptyPath(job, path))
            FileInputFormat.addInputPaths(job, path);
          else
            emptyPaths.add(path);
        }
      }

      // Create a empty file if the directory is empty
      for (String emptyPath : emptyPaths)
        numEmptyPaths = addInputPath(emptyPath, job, work, hiveScratchDir, numEmptyPaths, true, oneAlias);

      // If the query references non-existent partitions
      // We need to add a empty file, it is not acceptable to change the operator tree
      // Consider the query:
      //  select * from (select count(1) from T union all select count(1) from T2) x;
      // If T is empty and T2 contains 100 rows, the user expects: 0, 100 (2 rows)
      if (path == null)
        numEmptyPaths = addInputPath(null, job, work, hiveScratchDir, numEmptyPaths, false, oneAlias);
    }
  }
}
