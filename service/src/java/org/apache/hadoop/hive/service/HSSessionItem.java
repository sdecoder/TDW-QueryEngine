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

package org.apache.hadoop.hive.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.thrift.transport.TTransport;

public class HSSessionItem implements Comparable<HSSessionItem> {

  protected static final Log l4j = LogFactory.getLog(HSSessionItem.class.getName());
  
  public long to;
  
  public HashSet<TTransport> transSet;
  
  public final ReentrantLock lock = new ReentrantLock();
  
  private final Date date;
  
  public Date opdate;

  public enum SessionItemStatus {
    NEW, READY, JOB_SET, JOB_RUNNING, DESTROY, KILL_JOB
  };

  public enum jobType {
    ONESHOT {
      @Override
      public String toString() {
        return "ONESHOT";
      }
    }, REPEAT {
      @Override
      public String toString() {
        return "REPEAT";
      }
    },
  }

  public enum JobStatus {
    INIT {
      @Override
      public String toString() {
        return "INIT";
      }
    }, SET {
      @Override
      public String toString() {
        return "SET";
      }
    }, RUNNING {
      @Override
      public String toString() {
        return "RUNNING";
      }
    }, KILL {
      @Override
      public String toString() {
        return "KILL";
      }
    }, DONE {
      @Override
      public String toString() {
        return "DONE";
      }
    }
  };

  private HSSessionItem.JobStatus jobStatus;

  private int jobid;

  private final String home;

  private final String sessionName;

  private HSSessionItem.SessionItemStatus status;

  private CliSessionState ss;

  private HiveConf conf;

  private HSAuth auth;

  private String historyFile;

  private Thread runable;

  private JobRunner jr;

  private final SessionConfig config;

  public String suffix;

  public class SessionConfig {
    public jobType type;
    // time interval for each repeat, -1 for no repeat
    public long ti_repeat;

    public PeriodicJob pJob;

    public Timer timer;

    public SessionConfig(jobType type) {
      this.type = type;
      this.ti_repeat = -1;
      this.pJob = null;
      this.timer = null;
    }

    public void setJobType(jobType type, long ti) {
      if (type == jobType.REPEAT && ti > 0) {
        // start the periodic job here
        if (timer == null) {
          pJob = new PeriodicJob();
          timer = new Timer();
          timer.scheduleAtFixedRate(pJob, ti * 1000, ti * 1000);
          l4j.info("Set Timer to execute @ " + ti + " sec later.");
        } else if (ti > 0 && ti != this.ti_repeat) {
          // should we adjust the timer?
          cancelPeriodicJob();
          pJob = new PeriodicJob();
          timer = new Timer();
          timer.scheduleAtFixedRate(pJob, ti * 1000, ti * 1000);
          l4j.info("Set Timer to execute @ " + ti + " sec later.");
        }
      } else {
        l4j.info("Cancel the Timer now");
        cancelPeriodicJob();
      }
      this.type = type;
      this.ti_repeat = ti;
    }

    public void cancelPeriodicJob() {
      if (timer != null || pJob != null) {
        timer.cancel();
        timer = null;
        pJob = null;
      }
    }
  }

  public void configJob(jobType type, long ti) {
    config.setJobType(type, ti);
  }

  public class PeriodicJob extends TimerTask {
    @Override
    public void run() {
      // run the jobrunner here!
      // First, we should prepare the job file, just copy from the last job instance
      try {
        prepareJobFile();
      } catch (HiveServerException hex) {
        l4j.error("Trying to prepare the job file for jobid=" + jobid + " failed.");
        return;
      }

      l4j.info("Run a perodic job w/ jobid=" + jobid);
      runJob();
    }

  }

  public class JobRunner implements Runnable {

    private final String jobFile;

    private File resultFile;

    private File errorFile;

    private File infoFile;

    private int jobid;

    private String sid;

    private String svid;

    private final String user;

    private final String passwd;
    
    private final String dbName;

    private Process p;

    protected JobRunner(String jobFile, String sid, String svid, String user, String passwd, String dbName) {
      this.jobFile = jobFile;
      this.sid = sid;
      this.svid = svid;
      this.resultFile = null;
      this.errorFile = null;
      this.user = user;
      this.passwd = passwd;
      this.dbName = dbName;
    }

    public void run() {
      if (getResultFile() == null) {
        setResultFile(new File(getHome() + "/pl/TDW_PL_JOB_DEFAULT_result"));
      }
      if (getErrorFile() == null) {
        setErrorFile(new File(getHome() + "/pl/TDW_PL_JOB_DEFAULT_error"));
      }
      if (getInfoFile() == null) {
        setInfoFile(new File(getHome() + "/pl/TDW_PL_JOB_DEFAULT_info"));
      }
      String ocwd = System.getProperty("user.dir");
      String path = getHome() + "/pl/";

      List<String> cmd = new ArrayList<String>();
      cmd.add("python");
      cmd.add(ocwd + "/pl/tdw_loader.py");
      cmd.add("-f" + jobFile);
      cmd.add("-s" + sid);
      cmd.add("-a" + svid);
      cmd.add("-u" + user);
      cmd.add("-p" + passwd);
      cmd.add("-x" + path);
      cmd.add("-j" + jobid);
      cmd.add("-d" + dbName);
      cmd.add("-t" + getAuth().getPort());

      ProcessBuilder builder = new ProcessBuilder(cmd);
      Map<String, String> env = builder.environment();
      //builder.directory(new File(getHome() + "/pl"));

      setJobStatus(HSSessionItem.JobStatus.RUNNING);
      try {
        p = builder.start();

        if (runable.isInterrupted()) {
          throw new InterruptedException();
        }
        InputStream is = p.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line;

        FileOutputStream fos = new FileOutputStream(getResultFile());
        while ((line = br.readLine()) != null) {
          fos.write(line.getBytes());
          fos.write('\n');
        }

        if (runable.isInterrupted()) {
          throw new InterruptedException();
        }
        is = p.getErrorStream();
        isr = new InputStreamReader(is);
        br = new BufferedReader(isr);
        fos = new FileOutputStream(getErrorFile());
        while ((line = br.readLine()) != null) {
          fos.write(line.getBytes());
          fos.write('\n');
        }
      } catch (InterruptedException iex) {
        // write some meaningful result to the log
        String msg = "Job Killed by the Session Owner.\n";
        try {
          FileOutputStream fos = new FileOutputStream(getResultFile());
          fos.write(msg.getBytes());
          fos.write('\n');
          fos.close();
          fos = new FileOutputStream(getErrorFile());
          fos.write(msg.getBytes());
          fos.write('\n');
          fos.close();
        } catch (java.io.FileNotFoundException fex) {
          l4j.error("File Not found ", fex);
        } catch (java.io.IOException ex) {
          l4j.error("IO exception", ex);
        }
      } catch (java.io.FileNotFoundException fex) {
        l4j.error("Execute job failed w/ FEX ", fex);
      } catch (java.io.IOException ex) {
        l4j.error("Execute job failed ", ex);
      }
      setJobStatus(HSSessionItem.JobStatus.DONE);
      setStatus(HSSessionItem.SessionItemStatus.JOB_SET);
    }

    public void jobRunnerConfig(String resultFile, String errorFile, String infoFile, int jobid) {
      String cwd = System.getProperty("user.dir");

      setResultFile(new File(cwd + "/pl/" + resultFile));
      setErrorFile(new File(cwd + "/pl/" + errorFile));
      setInfoFile(new File(cwd + "/pl/" + infoFile));
      setJobid(jobid);
    }

    public void jobKillIt() {
      if (p != null) {
        p.destroy();
      }
    }

    public void setJobid(int jobid) {
      this.jobid = jobid;
    }

    public void setSid(String sid) {
      this.sid = sid;
    }

    public String getSid() {
      return sid;
    }

    public void setSvid(String svid) {
      this.svid = svid;
    }

    public String getSvid() {
      return svid;
    }

    public File getResultFile() {
      return resultFile;
    }

    public void setResultFile(File resultFile) {
      this.resultFile = resultFile;
    }

    public File getErrorFile() {
      return errorFile;
    }

    public void setErrorFile(File errorFile) {
      this.errorFile = errorFile;
    }

    public void setInfoFile(File infoFile) {
      this.infoFile = infoFile;
    }

    public File getInfoFile() {
      return infoFile;
    }
  }

  	// Creates an instance of SessionItem, set status to NEW.

  public HSSessionItem(HSAuth auth, String sessionName, TTransport trans) {
    this.jobid = 0;
    this.auth = auth;
    this.sessionName = sessionName;
    this.home = ".";
    this.runable = null;
    this.jr = null;
    this.config = new SessionConfig(jobType.ONESHOT);
    this.jobStatus = HSSessionItem.JobStatus.INIT;
    this.date = new Date();
    this.opdate = this.date;
    // default timeout value is 2 hours = 7200s
    this.to = 7200;
    // set this item to connected state
    this.transSet = new HashSet();
    if (trans != null)
    	transSet.add(trans);
    l4j.debug("HSSessionItem created");
    status = SessionItemStatus.NEW;

    l4j.debug("Wait for NEW->READY transition");
    l4j.debug("NEW->READY transition complete");
  }

  public jobType getConfigJob() {
    return config.type;
  }

  public long getConfigJobTi() {
    return config.ti_repeat;
  }

  public String getSessionName() {
    return sessionName;
  }

  public int compareTo(HSSessionItem other) {
    if (other == null) {
      return -1;
    }
    return getSessionName().compareTo(other.getSessionName());
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (!(other instanceof HSSessionItem)) {
      return false;
    }
    HSSessionItem o = (HSSessionItem) other;
    if (getSessionName().equals(o.getSessionName())) {
      return true;
    } else {
      return false;
    }
  }

  public SessionItemStatus getStatus() {
    return status;
  }

  public void setStatus(SessionItemStatus status) {
    this.status = status;
  }

  public HSAuth getAuth() {
    return auth;
  }

  protected void setAuth(HSAuth auth) {
    this.auth = auth;
  }

  protected synchronized void killIt() {
    // clear the timer now
    config.cancelPeriodicJob();
    
    if (jobStatus != HSSessionItem.JobStatus.RUNNING) {
      return;
    }
    l4j.debug(getSessionName() + " Attempting kill.");
    try {
    	if (runable != null)
    		runable.interrupt();
    	if (jr != null)
    		jr.jobKillIt();
    	if (runable != null)
    		runable.join();
    } catch (InterruptedException ie) {
      l4j.error("Try to join thread failed ", ie);
    }
    // reset jobstatus to set status
    jobStatus = HSSessionItem.JobStatus.DONE;
    status = HSSessionItem.SessionItemStatus.JOB_SET;
  }

  protected void freeIt(){
	  int i;
	  String jobFilePrefix = getHome() + "/pl/TDW_PL_JOB_" + getSessionName() + "_";
	  
	  killIt();
	  // we should free the job files and free all the resources
	  for (i = 0; i < jobid; i++) {
		  String fileName = jobFilePrefix + Integer.toString(i) + suffix;
		  File f = new File(fileName);
		  
		  try {
			  if (!f.exists()) {
				  throw new IllegalArgumentException("Delete: no such file: "  + fileName);
			  }
			  if (!f.canWrite()) {
				  throw new IllegalArgumentException("Delete: write protected: " + fileName);
			  }
			  
			  boolean success = f.delete();
			  if (!success) {
				  throw new IllegalArgumentException("Delete: deletion failed.");
			  }
		  } catch (IllegalArgumentException iae) {
			  l4j.error("EX: ", iae);
		  }
		  
		  // free pyc file
		  if (suffix != null) {
			  if (suffix.equals(".py")) {
				  fileName = jobFilePrefix + Integer.toString(i) + ".pyc";
				  f = new File(fileName);
				  
				  try {
					  if (!f.exists()) {
						  throw new IllegalArgumentException("Delete: no such file: "  + fileName);
					  }
					  if (!f.canWrite()) {
						  throw new IllegalArgumentException("Delete: write protected: " + fileName);
					  }
			  
					  boolean success = f.delete();
					  if (!success) {
						  throw new IllegalArgumentException("Delete: deletion failed.");
					  }
				  } catch (IllegalArgumentException iae) {
					  l4j.error("EX: ", iae);
				  }
			  }
		  }
		  // free result file
		  fileName = jobFilePrefix + Integer.toString(i) + "_result";
		  f = new File(fileName);
		  
		  try {
			  if (!f.exists()) {
				  throw new IllegalArgumentException("Delete: no such file: "  + fileName);
			  }
			  if (!f.canWrite()) {
				  throw new IllegalArgumentException("Delete: write protected: " + fileName);
			  }
			  
			  boolean success = f.delete();
			  if (!success) {
				  throw new IllegalArgumentException("Delete: deletion failed.");
			  }
		  } catch (IllegalArgumentException iae) {
			  l4j.error("EX: ", iae);
		  }

		  // free error file
		  fileName = jobFilePrefix + Integer.toString(i) + "_error";
		  f = new File(fileName);
		  
		  try {
			  if (!f.exists()) {
				  throw new IllegalArgumentException("Delete: no such file: "  + fileName);
			  }
			  if (!f.canWrite()) {
				  throw new IllegalArgumentException("Delete: write protected: " + fileName);
			  }
			  
			  boolean success = f.delete();
			  if (!success) {
				  throw new IllegalArgumentException("Delete: deletion failed.");
			  }
		  } catch (IllegalArgumentException iae) {
			  l4j.error("EX: ", iae);
		  }
		  
		  // free info file
		  fileName = jobFilePrefix + Integer.toString(i) + "_info";
		  f = new File(fileName);
		  
		  try {
			  if (!f.exists()) {
				  throw new IllegalArgumentException("Delete: no such file: "  + fileName);
			  }
			  if (!f.canWrite()) {
				  throw new IllegalArgumentException("Delete: write protected: " + fileName);
			  }
			  
			  boolean success = f.delete();
			  if (!success) {
				  throw new IllegalArgumentException("Delete: deletion failed.");
			  }
		  } catch (IllegalArgumentException iae) {
			  l4j.error("EX (you can ignore it): ", iae);
		  }
	  }
  }
  @Override
  public String toString() {
    return sessionName + ":" + auth.toString();
  }

  public int getCurrentJobid() {
    return jobid;
  }

  public void incJobid() {
    jobid = jobid + 1;
  }

  public boolean submitJob(String jobType, String job) {
    boolean res = true;
    FileOutputStream fos = null;

    if (status == HSSessionItem.SessionItemStatus.JOB_RUNNING) {
      return false;
    } else {
      status = HSSessionItem.SessionItemStatus.JOB_SET;
      setJobStatus(HSSessionItem.JobStatus.SET);
    }

    // we should write the job to disk file
    if (jobType.equals("python")) {
      suffix = ".py";
    } else {
      suffix = "";
    }
    String jobFile = getHome() + "/pl/TDW_PL_JOB_" + getSessionName() + "_" +
      Integer.toString(jobid) + suffix;

    try {
      fos = new FileOutputStream(new File(jobFile));
    } catch (java.io.FileNotFoundException fex) {
      l4j.error(getSessionName() + " opening jobFile " + jobFile, fex);
      res = false;
    }

    try {
      fos.write(job.getBytes());
      fos.close();
    } catch (java.io.IOException ex) {
      l4j.error(getSessionName() + " write jobFile " + jobFile, ex);
      res = false;
    }

    // we just check if we have already write to the file safely
    return res;
  }
  
  public boolean uploadModule(String user, String moduleName, String module) throws HiveServerException
  {
    boolean res = true;
    String fname = getHome() + "/pl/lib/" + user + "/" + moduleName;
    RandomAccessFile raf;
    File f;
    
    String dname = getHome() + "/pl/lib/" + user;
    File d = new File(dname);
    
    if (!d.exists()) {
      // create the user directory now
      if (!d.mkdir()) {
        l4j.error(getSessionName() + " try to mkdir " + dname + " failed.");
        throw new HiveServerException("Create user library failed.");          
      }
    }
    // ok, touch __init__.py now
    File i = new File(getHome() + "/pl/lib/" + user + "/__init__.py");
    
    if (!i.exists()) {
      try {
        i.createNewFile();
      } catch (java.io.IOException oe) {
        // ignore this exception
      }
    }
    
    try {
      f = new File(fname);
      if (!f.exists()) {
          if (!f.createNewFile()) {
            l4j.error("Try to create file " + fname + " failed.");
            throw new HiveServerException("Create user package failed.");
          }
      }
    } catch (java.io.IOException ex) {
      l4j.error(getSessionName() + " try to create file " + fname + " failed w/ " + ex);
      return false;
    }
    // check to see if we should delete the package file
    if (module.equalsIgnoreCase("")) {
      if (!f.delete()) {
        l4j.error("Try to delete file " + fname + " failed.");
        throw new HiveServerException("Delete user package " + fname + " failed.");
      } else {
        return true;
      }
    }
    
    // Step 2: try to open the file to write
    try {
      raf = new RandomAccessFile(f, "rw");
    } catch (java.io.FileNotFoundException ex) {
      l4j.error(getSessionName() + " try to open file " + fname + " failed, not found.");
      return false;
    }
    // Step 3: flush module content to this file
    try {
      // ok, truncate this file firstly
      raf.setLength(0);
      raf.seek(0);
      raf.write(module.getBytes());
    } catch (java.io.IOException ex) {
      l4j.error(getSessionName() + " try to truncate/write file " + fname + "failed w/ " + ex);
      return false;
    }
    return res;
  }
  
  public String downloadModule(String user, String moduleName) throws HiveServerException
  {
    String module = "";
    String fname = getHome() + "/pl/lib/" + user + "/" + moduleName;
    File f;
    
    String dname = getHome() + "/pl/lib/" + user;
    File d = new File(dname);
    
    // check the moduleName
    if (moduleName.startsWith("gxx_")) {
      // ok, we should change fname to global directory
      fname = getHome() + "/pl/lib/global/" + moduleName;
    } else if (moduleName.contains("/")) {
      fname = getHome() + "/pl/lib/" + moduleName;
    }
    if (!d.exists()) {
      // create the user directory now
      if (!d.mkdir()) {
        l4j.error(getSessionName() + " try to mkdir " + dname + " failed.");
        throw new HiveServerException("Create user library failed.");          
      }
    }
    
    f = new File(fname);
    if (f.canRead() == false) {
      throw new HiveServerException("Try to read file " + fname + " failed.");
    }
    
    try {
      FileInputStream fis = new FileInputStream(f);
      InputStreamReader isr = new InputStreamReader(fis);
      BufferedReader br = new BufferedReader(isr);
      String line;
      
      while ((line = br.readLine()) != null) {
        module += line + "\n";
      }
      fis.close();
    } catch (java.io.IOException ex) {
      l4j.error("IO Exception ", ex);
      throw new HiveServerException("Read file " + fname + " failed w/ " + ex);
    }
    
    return module;
  }
  
  public List<String> listModule(String user)
  {
    List<String> list = new ArrayList<String>();
    String[] content;
    File d = new File(getHome() + "/pl/lib/" + user);
    
    l4j.info("List user lib: " + d.getPath());
    if (!d.exists()) {
      l4j.info("Empty user library: " + user);
      list.add("");
    } else {
      content = d.list();
      for (int i = 0; i < content.length; i++) {
        list.add(content[i]);
      }
    }
    
    return list;
  }

  public synchronized boolean runJob() {
    boolean res = false;
    String jobFile = "TDW_PL_JOB_" + getSessionName() + "_" + Integer.toString(jobid);

    if (getStatus() != HSSessionItem.SessionItemStatus.JOB_SET) {
      return false;
    }
    setStatus(HSSessionItem.SessionItemStatus.JOB_RUNNING);
    jr = new JobRunner(jobFile, getSessionName(), getAuth().toString(),
        getAuth().getUser(), getAuth().getPasswd(), getAuth().getDbName());
    jr.jobRunnerConfig(jobFile + "_result", jobFile + "_error", jobFile + "_info", jobid);
    runable = new Thread(jr);
    runable.start();

    incJobid();
    return res;
  }

  public String getHome() {
    return home;
  }

  public void setJobStatus(HSSessionItem.JobStatus jobStatus) {
    this.jobStatus = jobStatus;
  }

  public HSSessionItem.JobStatus getJobStatus() {
    return jobStatus;
  }

  public String getHiveHistroy(int jobid) throws HiveServerException {
    String res = "";

    if (jobid == -1) {
      jobid = this.jobid - 1;
    }
    if (jobid >= this.jobid) {
      res = "The job " + jobid + " does not exist.";
      return res;
    } else if (jobid < -1) {
      res = "The jobid " + jobid + " is invalid.";
      return res;
    } else if (jobid == -1) {
      res = "There is no job upload for now.";
      return res;
    }
    String infoFile = "TDW_PL_JOB_" + getSessionName() + "_" + Integer.toString(jobid);
    String histFile;
    File infof = new File(getHome() + "/pl/" + infoFile + "_info");

    try {
      FileInputStream fis = new FileInputStream(infof);
      InputStreamReader isr = new InputStreamReader(fis);
      BufferedReader br = new BufferedReader(isr);

      histFile = br.readLine();
      if (histFile != null) {
        // we should read this file now
        fis.close();
        fis = new FileInputStream(histFile);
        isr = new InputStreamReader(fis);
        br = new BufferedReader(isr);
        String line;

        while ((line = br.readLine()) != null) {
          res += line;
        }
      }
      fis.close();
    } catch (java.io.FileNotFoundException fex) {
      l4j.error("File Not found ", fex);
    } catch (java.io.IOException ex) {
      l4j.error("IO exception", ex);
    }

    return res;
  }

  public List<String> getJobResult(int jobid) throws HiveServerException {
    List<String> res = new ArrayList<String>();

    if (jobid == -1) {
      // just return the last jobid for display
      jobid = this.jobid - 1;
    }
    if (jobid >= this.jobid) {
      res.add("The max jobid is: " + (this.jobid - 1));
      res.add("The job " + jobid + " does not exist.");
      return res;
    } else if (jobid < -1) {
      res.add("The max jobid is: " + (this.jobid - 1));
      res.add("The jobid " + jobid + " is invalid.");
      return res;
    }
    if (jobStatus != HSSessionItem.JobStatus.DONE && jobid == this.jobid - 1) {
      res.add("The max jobid is: " + (this.jobid - 1));
      res.add("The job is in state " + jobStatus.toString());
      return res;
    }

    res.add("The max jobid is: " + (this.jobid - 1));
    String jobFile = "TDW_PL_JOB_" + getSessionName() + "_" + Integer.toString(jobid);
    String cwd = System.getProperty("user.dir");
    // read the result file
    try {
      FileInputStream fis = new FileInputStream(new File(cwd + "/pl/" + jobFile + "_result"));
      InputStreamReader isr = new InputStreamReader(fis);
      BufferedReader br = new BufferedReader(isr);
      String line;

      while ((line = br.readLine()) != null) {
        res.add(line);
      }
    } catch (java.io.FileNotFoundException fex) {
      l4j.error("Read job result file ", fex);
      throw new HiveServerException("The result/error file of job " +
          Integer.toString(jobid) + " is not found");
    } catch (java.io.IOException ex) {
      l4j.error("Read job result file ", ex);
      throw new HiveServerException("IOException on reading result/error file of job " +
          Integer.toString(jobid));
    }
    // read the error file
    try {
      FileInputStream fis = new FileInputStream(new File(cwd + "/pl/" + jobFile + "_error"));
      InputStreamReader isr = new InputStreamReader(fis);
      BufferedReader br = new BufferedReader(isr);
      String line;

      while ((line = br.readLine()) != null) {
        res.add(line);
      }
    } catch (java.io.FileNotFoundException fex) {
      l4j.error("Read job error file ", fex);
    } catch (java.io.IOException ex) {
      l4j.error("Read job error file ", ex);
    }

    return res;
  }

  public boolean audit(String user, String passwd) {
    if (getAuth().getUser() == null || getAuth().getPasswd() == null) {
      return true;
    }
    if (user == null) {
      return false;
    }
    if (getAuth().getUser().equals(user)) {
      return true;
    } else if (user.equals("root")) {
      return true;
    } else {
      return false;
    }
  }

  public void prepareJobFile() throws HiveServerException {
    String lastJobFile = getHome() + "/pl/TDW_PL_JOB_" + getSessionName() + "_" +
      Integer.toString(jobid - 1) + suffix;
    String thisJobFile = getHome() + "/pl/TDW_PL_JOB_" + getSessionName() + "_" +
      Integer.toString(jobid) + suffix;

    try {
      FileInputStream fis = new FileInputStream(new File(lastJobFile));
      FileOutputStream fos = new FileOutputStream(new File(thisJobFile));
      InputStreamReader isr = new InputStreamReader(fis);
      BufferedReader br = new BufferedReader(isr);
      String line;

      while ((line = br.readLine()) != null) {
        fos.write(line.getBytes());
        fos.write('\n');
      }
    } catch (java.io.FileNotFoundException fex) {
      throw new HiveServerException("File not found @ Timer.run() ");
    } catch (java.io.IOException ex) {
      throw new HiveServerException("File IOException @ Timer.run() ");
    }
  }

  public Date getDate() {
    return date;
  }
  
  public Date getOpdate() {
    return opdate;
  }
  
  public void setOpdate() {
	  opdate = new Date();
  }
  
  public void addTrans(TTransport trans) {
	  if (trans == null)
		  return;
	  
	  lock.lock();
	  try {
		  transSet.add(trans);
	  } finally {
		  lock.unlock();
	  }
  }
  
  public void removeTrans(TTransport trans) {
	  if (trans == null)
		  return;
	  
	  lock.lock();
	  try {
		  transSet.remove(trans);
	  } finally {
		  lock.unlock();
	  }
  }
  
  public synchronized boolean isActive() {
    boolean res = false;

    lock.lock();
    Iterator<TTransport> i = transSet.iterator();
    
    while (i.hasNext()) {
      TTransport trans = (TTransport) i.next();
      
      if (trans.isOpen() == true) {
        res = true;
      } else {
        l4j.info("Session " + sessionName + " Connection disconnected!");
        i.remove();
      }
    }
    lock.unlock();
    
    return res;
  }
  
  public synchronized boolean isInactive() {
    boolean res = true;
	  Date currentDate = new Date();
	  
	  lock.lock();
	  Iterator<TTransport> i = transSet.iterator();
	  
	  // Step 1: if there is a active connection, we do not free it
	  while (i.hasNext()) {
		  TTransport trans = (TTransport) i.next();
	  
		  if (trans.isOpen() == true) {
			  res = false;
		  } else {
			  l4j.info("Session " + sessionName + " Connection disconnected!");
			  i.remove();
		  }
	  }
	  lock.unlock();
	  if (!res) {
	    return false;
	  }

	  // Step 2: if there is an active timer, we do not free it
	  if (config.timer != null) {
	    return false;
	  }
	  
	  // Step 3: if the opdate is currently updated, we do not free it
	  if (currentDate.getTime() - opdate.getTime() >= to * 1000) {
		  l4j.info("Time @ " + currentDate.toString() + " Set empty: " + Boolean.toString((transSet.isEmpty())));
		  return true;
	  } else 
		  return false;
  }
}
