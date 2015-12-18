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

package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.conf.HiveConf;


public class SemanticAnalyzerFactory {

  public static BaseSemanticAnalyzer get(HiveConf conf, ASTNode tree) throws SemanticException {
    if(tree.getToken() == null) {
      throw new RuntimeException ("Empty Syntax Tree");
    } else {
      switch (tree.getToken().getType()) {
      case HiveParser.TOK_EXPLAIN: return new ExplainSemanticAnalyzer(conf);
      case HiveParser.TOK_LOAD: return new LoadSemanticAnalyzer(conf);
      case HiveParser.TOK_CREATETABLE: 
      case HiveParser.TOK_DROPTABLE: 
      case HiveParser.TOK_TRUNCATETABLE: 
      case HiveParser.TOK_DESCTABLE:
      case HiveParser.TOK_DESCFUNCTION:
      case HiveParser.TOK_MSCK:
      case HiveParser.TOK_ALTERTABLE_ADDCOLS:
      case HiveParser.TOK_ALTERTABLE_ADDINDEX:
      case HiveParser.TOK_ALTERTABLE_DROPINDEX:
      case HiveParser.TOK_ALTERTABLE_REPLACECOLS:
      case HiveParser.TOK_ALTERTABLE_RENAME:
      case HiveParser.TOK_ALTERTABLE_DROPPARTS:
      case HiveParser.TOK_ALTERTABLE_ADDPARTS:
      case HiveParser.TOK_ALTERTABLE_ADDSUBPARTS:
      case HiveParser.TOK_ALTERTABLE_PROPERTIES:
      case HiveParser.TOK_ALTERTABLE_SERIALIZER:
      case HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES:
      case HiveParser.TOK_SHOWTABLES:
      case HiveParser.TOK_SHOWFUNCTIONS:
      case HiveParser.TOK_SHOWPARTITIONS:
      case HiveParser.TOK_ALTERTABLE_ADDDEFAULTPARTITION:
      case HiveParser.TOK_ALTERTABLE_TRUNCATE_PARTITION:
      case HiveParser.TOK_CREATE_DATABASE:
      case HiveParser.TOK_DROP_DATABASE:
      case HiveParser.TOK_SHOW_DATABASES:
      case HiveParser.TOK_USE_DATABASE:
      case HiveParser.TOK_SHOWTABLEINDEXS:
      case HiveParser.TOK_SHOWALLINDEXS:
        return new DDLSemanticAnalyzer(conf);
      case HiveParser.TOK_CREATEFUNCTION: 
      case HiveParser.TOK_DROPFUNCTION:
        return new FunctionSemanticAnalyzer(conf);
      case HiveParser.TOK_REVOKE_PRI:
      case HiveParser.TOK_GRANT_ROLE:
      case HiveParser.TOK_GRANT_PRIS:
      case HiveParser.TOK_SHOW_ROLES:
      case HiveParser.TOK_REVOKE_ROLE:
      case HiveParser.TOK_SHOW_GRANTS:
      case HiveParser.TOK_DROP_ROLE:
      case HiveParser.TOK_CREATE_ROLE:
      case HiveParser.TOK_CREATE_USER:
      case HiveParser.TOK_DROP_USER:
      case HiveParser.TOK_SHOW_USERS:
      case HiveParser.TOK_SET_PWD:
      case HiveParser.TOK_CHANGE_USER:
    	 return new ACLSemanticAnalyzer(conf); 
      default: return new SemanticAnalyzer(conf);
      }
    }
  }
}
