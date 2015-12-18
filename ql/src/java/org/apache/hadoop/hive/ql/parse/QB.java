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

import java.util.*;

import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.parse.QBMetaData;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implementation of the query block
 *
 **/

public class QB {
	
	public enum PartRefType{
		COMP,
		PRI,
		SUB
	};
	
	public static  class tableRef{
		
		String dbName;
		
		String tblName;
		String PriPart;
		String SubPart;
		PartRefType prt;
		
		
		//this ctor will be discard,use the next!
//		public tableRef(String tblName, PartRefType prt,String priPart, String subPart) {
//			super();
//			this.tblName = tblName;
//			this.prt = prt;
//			PriPart = priPart;
//			SubPart = subPart;
//		}
		
		public tableRef(String dbName, String tblName, PartRefType prt, String priPart,
				String subPart) {
			super();
			this.dbName = dbName;
			this.tblName = tblName;
			PriPart = priPart;
			SubPart = subPart;
			this.prt = prt;
		}

		
		public String getDbName() {
			return dbName;
		}

		public void setDbName(String dbName) {
			this.dbName = dbName;
		}

		public PartRefType getPrt() {
			return prt;
		}

		public void setPrt(PartRefType prt) {
			this.prt = prt;
		}

		public String getTblName() {
			return tblName;
		}
		public void setTblName(String tblName) {
			this.tblName = tblName;
		}
		public String getPriPart() {
			return PriPart;
		}
		public void setPriPart(String priPart) {
			PriPart = priPart;
		}
		public String getSubPart() {
			return SubPart;
		}
		public void setSubPart(String subPart) {
			SubPart = subPart;
		}
		
	}

  private static final Log LOG = LogFactory.getLog("hive.ql.parse.QB");

  private int numJoins = 0;
  private int numGbys = 0;
  private int numSels = 0;
  private int numSelDi = 0;
  private LinkedHashMap<String, tableRef> aliasToTabs;   //Edited by Brantzhang for Hash map join
  private HashMap<String, QBExpr> aliasToSubq;
  
  private HashMap<String, tableRef> UserAliasToTabs;
  
  private HashMap<String,ArrayList<String> > DBTB2UserAlias;
  
  private HashSet<String> DBTBs;
  
  private QBParseInfo qbp;
  private QBMetaData qbm;
  private QBJoinTree qbjoin;
  private String id;
  private boolean isQuery;

  public void print(String msg) {
    LOG.info(msg + "alias=" + qbp.getAlias());
    for(String alias: getSubqAliases()) {
      QBExpr qbexpr = getSubqForAlias(alias);
      LOG.info(msg+"start subquery " + alias);
      qbexpr.print(msg+" ");
      LOG.info(msg+"end subquery " + alias);
    }
  }

  public QB() {  
  }
  
  public QB(String outer_id, String alias, boolean isSubQ) {
    aliasToTabs = new LinkedHashMap<String, tableRef>();  //Edited by Brantzhang for Hash map join
    aliasToSubq = new HashMap<String, QBExpr>();
    UserAliasToTabs = new HashMap<String, tableRef>();
    DBTB2UserAlias = new HashMap<String,ArrayList<String> >();
    
    DBTBs = new HashSet<String>();
    
    if (alias != null) {
      alias = alias.toLowerCase();
    }
    qbp = new QBParseInfo(alias, isSubQ);
    qbm = new QBMetaData();
    this.id = (outer_id == null ? alias : outer_id + ":" + alias);
  }

  public void putDBTB(String dbtb){
	  DBTBs.add(dbtb.toLowerCase());
  }
  public boolean exisitsDBTB(String dbtb){
	  return DBTBs.contains(dbtb.toLowerCase());
  }
  public HashMap<String, ArrayList<String>> getDBTB2UserAlias() {
	return DBTB2UserAlias;
}

public void setDBTB2UserAlias(HashMap<String, ArrayList<String>> dBTB2UserAlias) {
	DBTB2UserAlias = dBTB2UserAlias;
}

public void putDBTB2UserAlias(String DBTB,String UserAlias){
	ArrayList<String > aliases = DBTB2UserAlias.get(DBTB.toLowerCase());
	if(null == DBTB2UserAlias.get(DBTB.toLowerCase())){
		aliases = new ArrayList<String>();
		aliases.add(UserAlias.toLowerCase());
		DBTB2UserAlias.put(DBTB.toLowerCase(), aliases);
	}else{
		aliases.add(UserAlias.toLowerCase());
	}
}

public ArrayList<String> getUserAliasFromDBTB(String DBTB){
	return DBTB2UserAlias.get(DBTB.toLowerCase());
}

public HashMap<String, tableRef> getUserAliasToTabs() {
	return UserAliasToTabs;
}

public void setUserAliasToTabs(String userAlias,tableRef tr) {
	UserAliasToTabs.put(userAlias.toLowerCase(), tr);
}

public tableRef getTableRefFromUserAlias(String userAlias){
	return UserAliasToTabs.get(userAlias.toLowerCase());
}

public Set<String> getUserTabAlias(){
	return UserAliasToTabs.keySet();
}



public boolean existsUserAlias(String UserAlias) {
    UserAlias = UserAlias.toLowerCase();
    if (UserAliasToTabs.get(UserAlias) != null || aliasToSubq.get(UserAlias) != null)
      return true;

    return false;
  }


public QBParseInfo getParseInfo() {
    return qbp;
  }

  public QBMetaData getMetaData() {
    return qbm;
  }

  public void setQBParseInfo(QBParseInfo qbp) {
    this.qbp = qbp;
  }

  public void countSelDi() {
    numSelDi++;
  }

  public void countSel() {
    numSels++;
  }

  public boolean exists(String alias) {
    alias = alias.toLowerCase();
    if (aliasToTabs.get(alias) != null/* || aliasToSubq.get(alias) != null*/)
      return true;

    return false;
  }

  public void setTabAlias(String alias, tableRef tblref) {
    aliasToTabs.put(alias.toLowerCase(), tblref);
  }
  
  public tableRef getTableRef(String alias){
	  return aliasToTabs.get(alias.toLowerCase());
	  
  }

  
  public HashMap<String, tableRef> getAliasToTabs() {
	return aliasToTabs;
}

public void setAliasToTabs(LinkedHashMap<String, tableRef> aliasToTabs) {
	this.aliasToTabs = aliasToTabs;
}

public void setSubqAlias(String alias, QBExpr qbexpr) {
    aliasToSubq.put(alias.toLowerCase(), qbexpr);
  }

  public String getId() {
    return id;
  }

  public int getNumGbys() {
    return numGbys;
  }

  public int getNumSelDi() {
    return numSelDi;
  }

  public int getNumSels() {
    return numSels;
  }

  public int getNumJoins() {
    return numJoins;
  }

  public Set<String> getSubqAliases() {
    return aliasToSubq.keySet();
  }

  public Set<String> getTabAliases() {
    return aliasToTabs.keySet();
  }

  public QBExpr getSubqForAlias(String alias) {
    return aliasToSubq.get(alias.toLowerCase());
  }

  public String getTabNameForAlias(String alias) {
    return aliasToTabs.get(alias.toLowerCase()).getTblName();
  }

  public QBJoinTree getQbJoinTree() {
    return qbjoin;
  }

  public void setQbJoinTree(QBJoinTree qbjoin) {
    this.qbjoin = qbjoin;
  }

  public void setIsQuery(boolean isQuery) {
    this.isQuery = isQuery;
  }

  public boolean getIsQuery() {
    return isQuery;
  }

  public boolean isSelectStarQuery() {
    return qbp.isSelectStarQuery() && aliasToSubq.isEmpty();
  }
}
