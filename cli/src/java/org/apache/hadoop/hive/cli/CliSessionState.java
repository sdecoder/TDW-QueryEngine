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

package org.apache.hadoop.hive.cli;

import java.util.Properties;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;

public class CliSessionState extends SessionState {
  /**
   * -e option if any that the session has been invoked with
   */ 
  public String execString;

  /**
   * -f option if any that the session has been invoked with
   */
  public String fileName;
  
  public String passwd;


  /**
   * properties set from -hiveconf via cmdline
   */
  public Properties cmdProperties = new Properties();


  public CliSessionState() {
    super();
  }

  public CliSessionState (HiveConf conf) {
    super(conf);
  }
  
  public CliSessionState (HiveConf conf, Hive db) {
    super(conf, db);
  }
}
