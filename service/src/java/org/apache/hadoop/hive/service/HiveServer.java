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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.service.HSSessionItem.jobType;

import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobTracker;

/**
 * Thrift Hive Server Implementation
 */
public class HiveServer extends ThriftHive {
  private final static String VERSION = "0";
  private static int port = 10000;

  private static HSSessionManager sessionManager;

  /**
   * Handler which implements the Hive Interface
   * This class can be used in lieu of the HiveClient class
   * to get an embedded server
   */
  public static class HiveServerHandler extends HiveMetaStore.HMSHandler implements HiveInterface {
    public enum CheckingFlag {
      SESSION, AUDIT, SESSION_AUDIT, SESSION_KILL, ALL
    }

    private boolean audit;
    private TTransport trans;
    private String user;
    private String passwd;
    private String dbName;

    private HSSessionItem sessionItem;
    // this is just a pointer to HiveServer.sessionManager
    private HSSessionManager sm;

    private final ByteArrayOutputStream out, err;
    /**
     * Hive server uses org.apache.hadoop.hive.ql.Driver for run() and 
     * getResults() methods.
     */
    private Driver driver;

    /**
     * Stores state per connection
     */
    private SessionState session;
    
    /**
     * Flag that indicates whether the last executed command was a Hive query
     */
    private boolean isHiveQuery;

    public static final Log LOG = LogFactory.getLog(HiveServer.class.getName());

    /**
     * A constructor.
     */
    public HiveServerHandler() throws MetaException {
      super(HiveServer.class.getName());

      isHiveQuery = false;
      SessionState session = new SessionState(new HiveConf(SessionState.class));
      SessionState.start(session);
      session.in = null;
      out = new ByteArrayOutputStream();
      err = new ByteArrayOutputStream();
      session.out = new PrintStream(out);
      session.err = new PrintStream(err);
      sessionItem = null;
      audit = false;
      trans = null;
      sm = sessionManager;
      driver = new Driver();
    }
    
    public HiveServerHandler(TTransport trans) throws MetaException {
    	super(HiveServer.class.getName());

      isHiveQuery = false;
      SessionState session = new SessionState(new HiveConf(SessionState.class));
      SessionState.start(session);
      session.in = null;
      out = new ByteArrayOutputStream();
      err = new ByteArrayOutputStream();
      session.out = new PrintStream(out);
      session.err = new PrintStream(err);
      sessionItem = null;
      audit = false;
      this.trans = trans;
      sm = sessionManager;
      driver = new Driver();
    }

    public HiveServerHandler(TTransport trans, HSSessionManager sessionManager) throws MetaException {
    	super(HiveServer.class.getName());

      isHiveQuery = false;
      SessionState session = new SessionState(new HiveConf(SessionState.class));
      SessionState.start(session);
      session.in = null;
      out = new ByteArrayOutputStream();
      err = new ByteArrayOutputStream();
      session.out = new PrintStream(out);
      session.err = new PrintStream(err);
      sessionItem = null;
      audit = false;
      this.trans = trans;
      sm = sessionManager;
      driver = new Driver();
    }
        
    /**
     * Executes a query.
     *
     * @param cmd HiveQL query to execute
     */
    public String execute(String cmd) throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION_KILL);
      HiveServerHandler.LOG.info("Running the query: " + cmd);
      SessionState ss = SessionState.get();
      
      String cmd_trimmed = cmd.trim();
      String[] tokens = cmd_trimmed.split("\\s");
      String cmd_1 = cmd_trimmed.substring(tokens[0].length()).trim();
      String res = "success";
      String errorMessage = "";
      ss.setUserName(sessionItem.getAuth().getUser());
      
      int ret = 0;
      try {
        if (tokens[0].toLowerCase().equals("list")) {
          SessionState.ResourceType t;
          if(tokens.length < 2 || (t = SessionState.find_resource_type(tokens[1])) == null) {
            throw new HiveServerException("Usage: list [" +
                    StringUtils.join(SessionState.ResourceType.values(),"|") +
                    "] [<value> [<value>]*]");
          } else {
            List<String> filter = null;
            if(tokens.length >=3) {
              System.arraycopy(tokens, 2, tokens, 0, tokens.length-2);
              filter = Arrays.asList(tokens);
            }
            Set<String> s = ss.list_resource(t, filter);
            if(s != null && !s.isEmpty())
              ss.out.println(StringUtils.join(s, "\n"));
          }
        } else {
          CommandProcessor proc = CommandProcessorFactory.get(tokens[0]);
            if(proc != null) {
              if (proc instanceof Driver) {
                isHiveQuery = true;
                ret = proc.run(cmd);
                driver = (Driver)proc;
              } else {
                isHiveQuery = false;
                ret = proc.run(cmd_1);
              }
            }
        }
      } catch (Exception e) {
        HiveServerException ex = new HiveServerException("Error running query: " + e.toString());
        if (ss != null) {
        	ss.out.flush();
        	ss.err.flush();
        }
        out.reset();
        err.reset();
        throw ex;
      }

      if (ss != null) {
    	  ss.get().out.flush();
    	  ss.get().err.flush();
      }
      if (!isHiveQuery) {
        res = out.toString();
        res += err.toString();
      }
      if (ret != 0) {
        errorMessage = err.toString();
      }
      out.reset();
      err.reset();

      if (ret != 0) {
        throw new HiveServerException("Query w/ errno: " + ret + " " + errorMessage);
      }
      return res;
    }

    /**
     * Return the status information about the Map-Reduce cluster
     */
    public HiveClusterStatus getClusterStatus() throws HiveServerException, TException {
    	generic_checking(CheckingFlag.SESSION);
      HiveClusterStatus hcs;
      try {
        ClusterStatus cs = driver.getClusterStatus();
        JobTracker.State jbs = cs.getJobTrackerState();
        
        // Convert the ClusterStatus to its Thrift equivalent: HiveClusterStatus
        int state;
        switch (jbs) {
          case INITIALIZING:
            state = JobTrackerState.INITIALIZING;
            break;
          case RUNNING:
            state = JobTrackerState.RUNNING;
            break;
          default:
            String errorMsg = "Unrecognized JobTracker state: " + jbs.toString();
            throw new Exception(errorMsg);
        }
        
        hcs = new HiveClusterStatus(
            cs.getTaskTrackers(),
            cs.getMapTasks(),
            cs.getReduceTasks(),
            cs.getMaxMapTasks(),
            cs.getMaxReduceTasks(),
            state);
      }
      catch (Exception e) {
        LOG.error(e.toString());
        e.printStackTrace();
        throw new HiveServerException("Unable to get cluster status: " + e.toString());
      }
      return hcs;
    }
    
    /**
     * Return the Hive schema of the query result
     */
    public Schema getSchema() throws HiveServerException, TException {
    	generic_checking(CheckingFlag.SESSION);
      if (!isHiveQuery)
        // Return empty schema if the last command was not a Hive query
        return new Schema();	
    	
      try {
        Schema schema = driver.getSchema();
        if (schema == null) {
          schema = new Schema();
        }
        LOG.info("Returning schema: " + schema);
        return schema;
      }
      catch (Exception e) {
        LOG.error(e.toString());
        e.printStackTrace();
        throw new HiveServerException("Unable to get schema: " + e.toString());
      }
    }
    
    /**
     * Return the Thrift schema of the query result
     */
    public Schema getThriftSchema() throws HiveServerException, TException {
    	generic_checking(CheckingFlag.SESSION);
      if (!isHiveQuery)
        // Return empty schema if the last command was not a Hive query
        return new Schema();
    	
      try {
        Schema schema = driver.getThriftSchema();
        if (schema == null) {
          schema = new Schema();
        }
        LOG.info("Returning schema: " + schema);
        return schema;
      }
      catch (Exception e) {
        LOG.error(e.toString());
        e.printStackTrace();
        throw new HiveServerException("Unable to get schema: " + e.toString());
      }
    }
    
    
    /** 
     * Fetches the next row in a query result set.
     * 
     * @return the next row in a query result set. null if there is no more row to fetch.
     */
    public String fetchOne() throws HiveServerException, TException {
    	generic_checking(CheckingFlag.SESSION);
      if (!isHiveQuery)
        // Return no results if the last command was not a Hive query
        return "";
      
      Vector<String> result = new Vector<String>();
      driver.setMaxRows(1);
      try {
        if (driver.getResults(result)) {
          return result.get(0);
        }
        // TODO: Cannot return null here because thrift cannot handle nulls
        // TODO: Returning empty string for now. Need to figure out how to
        // TODO: return null in some other way
        return "";
      } catch (IOException e) {
        throw new HiveServerException(e.getMessage());
      }
    }

    /**
     * Fetches numRows rows.
     *
     * @param numRows Number of rows to fetch.
     * @return A list of rows. The size of the list is numRows if there are at least 
     *         numRows rows available to return. The size is smaller than numRows if
     *         there aren't enough rows. The list will be empty if there is no more 
     *         row to fetch or numRows == 0. 
     * @throws HiveServerException Invalid value for numRows (numRows < 0)
     */
    public List<String> fetchN(int numRows) throws HiveServerException, TException {
    	generic_checking(CheckingFlag.SESSION);
      if (numRows < 0) {
        throw new HiveServerException("Invalid argument for number of rows: " + numRows);
      } 
      if (!isHiveQuery)
      	// Return no results if the last command was not a Hive query
        return new Vector<String>();
      
      Vector<String> result = new Vector<String>();      
      driver.setMaxRows(numRows);
      try {
        driver.getResults(result);
      } catch (IOException e) {
        throw new HiveServerException(e.getMessage());
      }
      return result;
    }

    /**
     * Fetches all the rows in a result set.
     *
     * @return All the rows in a result set of a query executed using execute method.
     *
     * TODO: Currently the server buffers all the rows before returning them 
     * to the client. Decide whether the buffering should be done in the client.
     */
    public List<String> fetchAll() throws HiveServerException, TException {
    	generic_checking(CheckingFlag.SESSION);
      if (!isHiveQuery)
        // Return no results if the last command was not a Hive query
        return new Vector<String>();
      
      Vector<String> rows = new Vector<String>();
      Vector<String> result = new Vector<String>();
      try {
        while (driver.getResults(result)) {
          rows.addAll(result);
          result.clear();
        }
      } catch (IOException e) {
        throw new HiveServerException(e.getMessage());
      }
      return rows;
    }
    
    /**
     * Return the status of the server
     */
    @Override
    public int getStatus() {
      return 0;
    }

    /**
     * Return the version of the server software
     */
    @Override
    public String getVersion() {
      return VERSION;
    }
	
    public void setSessionItem(HSSessionItem sessionItem) {
      this.sessionItem = sessionItem;
    }

    public HSSessionItem getSessionItem() {
      return sessionItem;
    }

    @Override
    public List<String> createSession(String sessionName) throws HiveServerException, TException {
      List<String> l = new ArrayList<String>();
      String name;
      String errorMsg;

      generic_checking(CheckingFlag.AUDIT);
      if (sessionItem != null) {
        errorMsg = "You already connected to a session: " + sessionItem.toString();
        throw new HiveServerException(errorMsg);
      }
      HSAuth auth = new HSAuth(getUser(), getPasswd(), getDbName(), port);
      auth.setAuthid(Double.toString(Math.random()));
      if (sessionName == null || sessionName.equals("")) {
        name = Double.toString(Math.random()).substring(2);
      } else {
        name = sessionName;
      }
      sessionItem = new HSSessionItem(auth, name, trans);
      if (!sm.register(sessionItem)) {
        errorMsg = "Register Session: " + name + " failed, name conflicts.";
        sessionItem = null;
        throw new HiveServerException(errorMsg);
      }
      HiveServerHandler.LOG.info("Create a new Session: " + name +
          " @ " + sessionItem.getDate().toString());

      l.add(sessionItem.getSessionName());
      l.add(sessionItem.getAuth().toString());
      return l;
    }

    @Override
    public int dropSession(String sid, String svid) throws HiveServerException, TException {
    	generic_checking(CheckingFlag.SESSION);
      if (sessionItem == null) {
        HiveServerHandler.LOG.info("Unconnected clients could not do any dropSession operations.");
        return -1;
      }
      
      sessionItem.removeTrans(trans);
      if (sessionItem.isActive()) {
        HiveServerHandler.LOG.info("Another client attached to this session either, you cant discard it!");
        // just do it as 'detach'
        sessionItem = null;
        return -1;
      }
      configJob("type=ONESHOT;ti=0;");
      sessionItem.killIt();
      sm.unregister(sessionItem);
      sessionItem = null;

      return 0;
    }

    @Override
    public String requireSession(String sid, String svid) throws HiveServerException, TException {
      String logstr;
      generic_checking(CheckingFlag.AUDIT);
      if (sessionItem != null) {
        HiveServerHandler.LOG.info("You have already connected to a Session: " +
            sessionItem.getSessionName());
        return "";
      }
      // it is ok to lookup the session now
      sessionItem = sm.lookup(sid, this.user);
      if (sessionItem == null) {
        logstr = "require: " + sid + ":" + svid + " failed.";
        HiveServerHandler.LOG.info(logstr);
        return "";
      } else if (sessionItem.audit(user, passwd)) {
        logstr = "require: " + sid + ":" + svid + " granted.";
        HiveServerHandler.LOG.info(logstr);
        sessionItem.addTrans(trans);
        return sessionItem.getSessionName();
      } else {
        logstr = "require: " + sid + ":" + svid + " failed.";
        HiveServerHandler.LOG.info(logstr);
        String msg = "Bad user name or password for session " + sessionItem.getSessionName();
        sessionItem = null;
        throw new HiveServerException(msg);
      }
    }

    @Override
    public int detachSession(String sid, String svid) throws HiveServerException, TException {
    	generic_checking(CheckingFlag.SESSION);
    	sessionItem.removeTrans(trans);
      sessionItem = null;
      return 0;
    }

    public void generic_checking(CheckingFlag flag) throws HiveServerException, TException {
      if (flag == CheckingFlag.AUDIT || flag == CheckingFlag.SESSION_AUDIT ||
          flag == CheckingFlag.ALL) {
        if (!isAudit()) {
          String msg = "Unauthorized connection!";
          throw new HiveServerException(msg);
        }
      }
      if (flag == CheckingFlag.SESSION || flag == CheckingFlag.SESSION_AUDIT ||
          flag == CheckingFlag.SESSION_KILL || flag == CheckingFlag.ALL) {
        if (sessionItem == null) {
          String msg = "Detached Session, reject any queries!";
          throw new HiveServerException(msg);
        }
      }
      if (flag == CheckingFlag.SESSION_KILL) {
        if (sessionItem.getJobStatus() == HSSessionItem.JobStatus.KILL) {
          String msg = "Job Killed by the session attacher!";
          throw new HiveServerException(msg);
        }
      }
      // update the date info
      if (sessionItem != null) {
    	  sessionItem.setOpdate();
      }
    }

    @Override
    public List<String> getEnv() throws HiveServerException, TException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<String> getJobStatus(int jobid) throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION);
      return sessionItem.getJobResult(jobid);
    }

    @Override
    public List<String> showSessions() throws HiveServerException, TException {
      // we do not check any session state here, but we should check the audit info
      generic_checking(CheckingFlag.AUDIT);
      return sm.showSessions(sessionItem);
    }

    @Override
    public int uploadJob(String job) throws HiveServerException, TException {
      // For now, we got the job from the client. We should save it to a local file
      generic_checking(CheckingFlag.SESSION);
      if (sessionItem.submitJob("python", job)) {
        HiveServerHandler.LOG.info("Submit job succeed.");
      } else {
        String msg = "Submit Job failed, another job is running or files failed to open.";
        throw new HiveServerException(msg);
      }

      // now, we run the job
      sessionItem.runJob();

      return 0;
    }

    public boolean isAudit() {
      return audit;
    }

    public void setAudit(boolean audit) {
      this.audit = audit;
    }

    public void setUser(String user) {
      this.user = user;
    }
    
    public String getUser() {
      return user;
    }

    public void setPasswd(String passwd) {
      this.passwd = passwd;
    }

    public String getPasswd() {
      return passwd;
    }
    
    public void setDbName(String dbName) {
      this.dbName = dbName;
    }
    
    public String getDbName() {
      return dbName;
    }

    @Override
    public int audit(String user, String passwd, String dbName) throws HiveServerException, TException {
      Hive hive;
      boolean res;
      
    	if (sessionItem != null || isAudit() == true) {
    		setAudit(false);
    		String errorMessage = "Already connected or already authorized.";
    		throw new HiveServerException("Auth returned: " + errorMessage);
    	}
      setUser(user);
      setPasswd(passwd);
      SessionState ss = SessionState.get();
      // We should call brantzhang's API here to verify the username and password
      try {
        hive = Hive.get();
        res = hive.isAUser(user, passwd);
      } catch (HiveException e) {
        setAudit(false);
        String errorMessage = "Hive.get() failed or user is not permitted.\n";
        throw new HiveServerException("Auth returned: " + errorMessage);
      }
      
      if (res) {
        setAudit(true);
        setDbName(dbName);
        ss.setDbName(dbName);
        return 0;
      } else {
        setAudit(false);
        return -1;
      }
    }

    @Override
    public int configJob(String config) throws HiveServerException, TException {
      int idx, end;
      String type, typeValue, ti, tiValue;

      generic_checking(CheckingFlag.SESSION);
      idx = config.indexOf("type=") + 5;
      if (idx != 4) {
        type = config.substring(idx);

        end = type.indexOf(';');
        if (end == -1) {
          // get the current type
          typeValue = sessionItem.getConfigJob().toString();
        } else {
          typeValue = type.substring(0, end);
        }
      } else {
        // get the current type
        typeValue = sessionItem.getConfigJob().toString();
      }

      idx = config.indexOf("ti=") + 3;
      if (idx != 2) {
        ti = config.substring(idx);
        end =ti.indexOf(';');
        if (end == -1) {
          // get the current ti value
          tiValue = Long.toString(sessionItem.getConfigJobTi());
        } else {
          tiValue = ti.substring(0, end);
        }
      } else {
        // get the current ti value
        tiValue = Long.toString(sessionItem.getConfigJobTi());
      }

      HiveServerHandler.LOG.info("Got type=" + typeValue + "; ti=" + tiValue);

      jobType jType;

      if (typeValue.equals(jobType.ONESHOT.toString())) {
        jType = jobType.ONESHOT;
      } else if (typeValue.equals(jobType.REPEAT.toString())) {
        jType = jobType.REPEAT;
      } else {
        jType = jobType.ONESHOT;
      }

      sessionItem.configJob(jType, Integer.parseInt(tiValue));
      return 0;
    }

    @Override
    public int killJob() throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION);
      sessionItem.killIt();
      return 0;
    }

    @Override
    public void setHistory(String sid, int jobid) throws HiveServerException, TException {
      // we should set the hive history file name to job's info file
      generic_checking(CheckingFlag.SESSION);
      String infoFile = "TDW_PL_JOB_" + sessionItem.getSessionName() + "_" + Integer.toString(jobid);
      File infof = new File(sessionItem.getHome() + "/pl/" + infoFile + "_info");

      try {
        FileOutputStream fos = new FileOutputStream(infof);
        fos.write(SessionState.get().getHiveHistory().getHistFileName().getBytes());
        fos.write('\n');
        fos.close();
      } catch (java.io.FileNotFoundException fex) {
        HiveServerHandler.LOG.info("File Not found ", fex);
      } catch (java.io.IOException ex) {
        HiveServerHandler.LOG.info("IO exception", ex);
      }
    }

    @Override
    public String getHistory(int jobid) throws HiveServerException, TException {
      // get the job history file
      generic_checking(CheckingFlag.SESSION);
      return sessionItem.getHiveHistroy(jobid);
    }

    @Override
    public String compile(String cmd) throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION);
      HiveServerHandler.LOG.info("Compile the query: " + cmd);
      SessionState ss = SessionState.get();

      // TODO: Note that we should set the session state to compile mode here!
      ss.setCheck(true);
      ss.setUserName(sessionItem.getAuth().getUser());
      
      String cmd_trimmed = cmd.trim();
      String[] tokens = cmd_trimmed.split("\\s");
      String errorMessage = "";
      String res = "success";

      int ret = 0;

      try {
        CommandProcessor proc = CommandProcessorFactory.get(tokens[0]);
        if (proc != null) {
          if (proc instanceof Driver) {
            isHiveQuery = true;
            ret = proc.run(cmd);
          } else {
            isHiveQuery = false;
            ret = -1;
            errorMessage = " This is not a valid SQL statement.";
          }
        }
      } catch (Exception e) {
        HiveServerException ex = new HiveServerException();
        ex.setMessage("Error compiling query: " + e.toString());
        SessionState.get().out.flush();
        SessionState.get().err.flush();
        out.reset();
        err.reset();
        ss.setCheck(false);
        throw ex;
      }

      SessionState.get().out.flush();
      SessionState.get().err.flush();
      ss.setCheck(false);
      
      // we know that the return value is not very useful, however, we can use 
      // the error message dumped by the ql.Driver
      if (ret != 0) {
        errorMessage = err.toString();
      }
      out.reset();
      err.reset();

      if (ret != 0) {
        throw new HiveServerException("Compile w/ errno: " + ret + " " + errorMessage);
      }
      return res;
    }

    @Override
    public String downloadModule(String user, String moduleName)
        throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION);
      
      if (moduleName.contains("..")) {
        throw new HiveServerException("Error, you can not issue moduleName contains '..'");
      }
      return sessionItem.downloadModule(user, moduleName);
    }

    @Override
    public List<String> listModule(String user) throws HiveServerException,
        TException {
      generic_checking(CheckingFlag.SESSION);
          
      return sessionItem.listModule(user);
    }

    @Override
    public String uploadModule(String user, String moduleName, String module)
        throws HiveServerException, TException {
      String res = "ok";
      
      generic_checking(CheckingFlag.SESSION);
      
      if (moduleName.contains("..")) {
        throw new HiveServerException("Error, you can not issue moduleName contains '..'");
      }
      // Step 1: check if this user is root
      if (!getUser().equalsIgnoreCase("root") && !getUser().equalsIgnoreCase(user)) {
        throw new HiveServerException("Error, you can only access your (" + getUser() + ") own library.");
      }
      if (!getUser().equalsIgnoreCase("root")) {
        user = getUser();
      }

      // Step 2: throw this file to sessionItem now
      if (sessionItem.uploadModule(user, moduleName, module) == false) {
        throw new HiveServerException("Upload module " + moduleName + " failed.");
      }
      
      return res;
    }

    @Override
    public String getRowCount() throws HiveServerException, TException {
      String res = "0";
      //TODO: get the rowcount from the sessionstate
      return res;
    }
  }

  public static class ThriftHiveProcessorFactory extends TProcessorFactory {
    public ThriftHiveProcessorFactory (TProcessor processor) {
      super(processor);
    }

    public TProcessor getProcessor(TTransport trans) {
      try {
        Iface handler = new HiveServerHandler(trans);
        return new ThriftHive.Processor(handler);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void main(String[] args) {
    try {
     
      if (args.length >= 1) {
        port = Integer.parseInt(args[0]);
      }
      SessionState.initHiveLog4j();
      sessionManager = new HSSessionManager();
      Thread t = new Thread(sessionManager);
      t.start();
      TServerTransport serverTransport = new TServerSocket(port);
      ThriftHiveProcessorFactory hfactory = new ThriftHiveProcessorFactory(null);
      TThreadPoolServer.Options options = new TThreadPoolServer.Options();
      TServer server = new TThreadPoolServer(hfactory, serverTransport,
          new TTransportFactory(), new TTransportFactory(),
          new TBinaryProtocol.Factory(), new TBinaryProtocol.Factory(), options);
      HiveServerHandler.LOG.info("Starting hive server on port " + port);
      server.serve();
    } catch (Exception x) {
      x.printStackTrace();
    }
  }
}
