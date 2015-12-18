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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.service.HiveServer.HiveServerHandler;


public class HSSessionManager implements Runnable {

  protected static final Log l4j = LogFactory.getLog(HSSessionManager.class.getName());

  private final boolean goOn;
  //private final TreeMap<HSSessionItem, HSAuth> items;
  private final Map items;
  
  protected HSSessionManager() {
    goOn = true;
    items = Collections.synchronizedMap(new TreeMap<HSSessionItem, HSAuth>());
  }

  public void run() {
    l4j.debug("Entered run() thread has started");
    HiveServerHandler.LOG.info("Start the HSSessionManager");
    while (goOn) {
      Set s = items.keySet();
      
      synchronized (items) {
        Iterator<HSSessionItem> it = s.iterator();
        HSSessionItem i;
        while (it.hasNext()) {
          i = it.next();
          if (i.getStatus() == HSSessionItem.SessionItemStatus.DESTROY) {
            HiveServerHandler.LOG.info("Destroy Session: " + i.getSessionName() + ":" +
                i.getAuth().toString());
            it.remove();
        	  i.freeIt();
          } else if (i.getStatus() == HSSessionItem.SessionItemStatus.KILL_JOB) {
            l4j.debug("Killing item: " + i.getSessionName());
            i.freeIt();
            it.remove();
          } else {
        	  if (i.isInactive() == true) {
        		  l4j.info("Create @ " + i.getOpdate().toString() + " Inactive session " + 
        				  i.getSessionName() + ", free it.");
        		  it.remove();
        		  i.freeIt();
        	  }
          }
        }
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException ex) {
        l4j.error("Could not sleep ", ex);
      }
    }
    l4j.debug("goOn is false, loop has ended.");
  }

  public HSSessionItem lookup(String name, String user) {
    Set s = items.keySet();
    
    synchronized (items) {
      Iterator<HSSessionItem> it = s.iterator();
      HSSessionItem i;
      while (it.hasNext()) {
        i = it.next();
        HiveServerHandler.LOG.debug(i.getSessionName() + " vs " + name + ":" + 
            i.getAuth().getUser() + " vs " + user);        
        // If the user is trusted, we do not check any authid
        //if (i.getSessionName().equals(name) &&
        //    i.getAuth().toString().equals(authid)) {
        if (i.getSessionName().equals(name)) {
          return i;
        }
      }
    }
    return null;
  }

  public boolean register(HSSessionItem sessionItem) {
    HSSessionItem tmp;

    tmp = lookup(sessionItem.getSessionName(), sessionItem.getAuth().getUser());
    if (sessionItem.equals(tmp)) {
      return false;
    } else if (tmp == null) {
      // add the sessionItem to the treemap
      synchronized (items) {
        items.put(sessionItem, sessionItem.getAuth());
      }
      return true;
    } else {
      return false;
    }
  }

  public boolean unregister(HSSessionItem sessionItem) {
    HSSessionItem tmp;
    boolean hit = false;

    tmp = lookup(sessionItem.getSessionName(), sessionItem.getAuth().getUser());
    if (sessionItem.equals(tmp)) {
      hit = true;
    }
    if (!hit) {
      return hit;
    }
    // we just set the session status to destroy or kill
    synchronized (items) {
      if (sessionItem.getStatus() == HSSessionItem.SessionItemStatus.JOB_RUNNING) {
        // TODO: stop the job first
        sessionItem.setStatus(HSSessionItem.SessionItemStatus.KILL_JOB);
      } else {
        sessionItem.setStatus(HSSessionItem.SessionItemStatus.DESTROY);
      }
    }
    return true;
  }

  public List<String> showSessions(HSSessionItem session) {
    List<String> list = new ArrayList<String>();
    String user = null;
    boolean isRoot = false;
    boolean isValidUser = false;

    if (session != null) {
      if (session.getAuth().getUserType() == HSAuth.UserType.ROOT) {
        isRoot = true;
      }
      if (session.getAuth().getUser() != null) {
        isValidUser = true;
        user = session.getAuth().getUser();
      }
    }
    Set s = items.keySet();
    
    synchronized (items) {
      Iterator<HSSessionItem> it = s.iterator();
      HSSessionItem i;
      while (it.hasNext()) {
        i = it.next();
        if (isRoot) {
          list.add(i.getAuth().getUser() + ":" + i.getSessionName() + ":" + i.getAuth().toString());
        } else if (isValidUser){
          if (i.getAuth().getUser().equals(user)) {
            list.add(i.getAuth().getUser() + ":" + i.getSessionName() + ":" + i.getAuth().toString());
          } else {
            list.add(i.getAuth().getUser() + ":" + i.getSessionName());
          }
        } else {
          list.add(i.getAuth().getUser() + ":" + i.getSessionName());
        }
      }
    }
    return list;
  }

}

