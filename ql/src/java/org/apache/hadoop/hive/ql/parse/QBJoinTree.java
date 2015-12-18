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

import java.util.HashMap;   //Added by Brantzhang for patch HIVE-870
import java.util.Vector;
import java.util.List;
import java.util.ArrayList; //Added by Brantzhang for patch HIVE-870
import java.util.Map.Entry; //Added by Brantzhang for patch HIVE-870

/**
 * Internal representation of the join tree
 *
 */
public class QBJoinTree 
{
  private String        leftAlias;
  private String[]      rightAliases;
  private String[]      leftAliases;
  private QBJoinTree    joinSrc;
  private String[]      baseSrc;
  private int           nextTag;
  private joinCond[]    joinCond;
  private boolean       noOuterJoin;
  private boolean       noSemiJoin;   //Added by Brantzhang for patch HIVE-870
  
  // keeps track of the right-hand-side table name of the left-semi-join, and its list of join keys   //Added by Brantzhang for patch HIVE-870
  private HashMap<String, ArrayList<ASTNode>> rhsSemijoin;   //Added by Brantzhang for patch HIVE-870
  
  // join conditions
  private Vector<Vector<ASTNode>> expressions;

  // filters
  private Vector<Vector<ASTNode>> filters;

  // user asked for map-side join
  private  boolean        mapSideJoin;
  private  List<String>   mapAliases;
  
  //big tables that should be streamed       //Added by Brantzhang for patch HIVE-853
  private  List<String>   streamAliases;     //Added by Brantzhang for patch HIVE-853
  
  /**
   * constructor 
   */
  //public QBJoinTree() { nextTag = 0;}   //Removed by Brantzhang for patch HIVE-870
  //Added by Brantzhang for patch HIVE-853 Begin
  public QBJoinTree() { 
	nextTag = 0;
	noOuterJoin = true;
	noSemiJoin  = true;
	rhsSemijoin = new HashMap<String, ArrayList<ASTNode>>();
  }
  //Added by Brantzhang for patch HIVE-853 End

  /**
   * returns left alias if any - this is used for merging later on
   * @return left alias if any
   */
  public String getLeftAlias() {
    return leftAlias;
  }

  /**
   * set left alias for the join expression
   * @param leftAlias String
   */
  public void setLeftAlias(String leftAlias) {
    this.leftAlias = leftAlias;
  }

  public String[] getRightAliases() {
    return rightAliases;
  }

  public void setRightAliases(String[] rightAliases) {
    this.rightAliases = rightAliases;
  }

  public String[] getLeftAliases() {
    return leftAliases;
  }

  public void setLeftAliases(String[] leftAliases) {
    this.leftAliases = leftAliases;
  }

  public Vector<Vector<ASTNode>> getExpressions() {
    return expressions;
  }

  public void setExpressions(Vector<Vector<ASTNode>> expressions) {
    this.expressions = expressions;
  }

  public String[] getBaseSrc() {
    return baseSrc;
  }

  public void setBaseSrc(String[] baseSrc) {
    this.baseSrc = baseSrc;
  }

  public QBJoinTree getJoinSrc() {
    return joinSrc;
  }

  public void setJoinSrc(QBJoinTree joinSrc) {
    this.joinSrc = joinSrc;
  }

  public int getNextTag() {
    return nextTag++;
  }

  public String getJoinStreamDesc() {
    return "$INTNAME";
  }

  public joinCond[] getJoinCond() {
    return joinCond;
  }

  public void setJoinCond(joinCond[] joinCond) {
    this.joinCond = joinCond;
  }

  public boolean getNoOuterJoin() {
    return noOuterJoin;
  }

  public void setNoOuterJoin(boolean noOuterJoin) {
    this.noOuterJoin = noOuterJoin;
  }
  
  //Added by Brantzhang for patch HIVE-870 Begin
  public boolean getNoSemiJoin() {
	return noSemiJoin;
  }
  //Added by Brantzhang for patch HIVE-870 End

  
	///**                        //Removed by Brantzhang for patch HIVE-870
	 //* @return the filters     //Removed by Brantzhang for patch HIVE-870
	 //*/                        //Removed by Brantzhang for patch HIVE-870
	//public Vector<Vector<ASTNode>> getFilters() {   //Removed by Brantzhang for patch HIVE-870
	//	return filters;          //Removed by Brantzhang for patch HIVE-870
	//}                          //Removed by Brantzhang for patch HIVE-870
  
  //Added by Brantzhang for patch HIVE-870 Begin
  public void setNoSemiJoin(boolean semi) {  
	this.noSemiJoin = semi;
  }
  //Added by Brantzhang for patch HIVE-870 End

	///**                                  //Removed by Brantzhang for patch HIVE-870
	 //* @param filters the filters to set //Removed by Brantzhang for patch HIVE-870
	 //*/                                  //Removed by Brantzhang for patch HIVE-870
	//public void setFilters(Vector<Vector<ASTNode>> filters) {  //Removed by Brantzhang for patch HIVE-870
		//this.filters = filters;                                //Removed by Brantzhang for patch HIVE-870
	//}                                                          //Removed by Brantzhang for patch HIVE-870

  //Added by Brantzhang for patch HIVE-870 Begin
  /**
   * @return the filters
   */
  public Vector<Vector<ASTNode>> getFilters() {
    return filters;
  }
   
  /**
   * @param filters the filters to set
   */
  public void setFilters(Vector<Vector<ASTNode>> filters) {
    this.filters = filters;
  }
  //Added by Brantzhang for patch HIVE-870 End
  
  /**
   * @return the mapSidejoin
   */
  public boolean isMapSideJoin() {
    return mapSideJoin;
  }

  /**
   * @param mapSideJoin the mapSidejoin to set
   */
  public void setMapSideJoin(boolean mapSideJoin) {
    this.mapSideJoin = mapSideJoin;
  }

  /**
   * @return the mapAliases
   */
  public List<String> getMapAliases() {
    return mapAliases;
  }

  /**
   * @param mapAliases the mapAliases to set
   */
  public void setMapAliases(List<String> mapAliases) {
    this.mapAliases = mapAliases;
  }
  
  //Added by Brantzhang for patch HIVE-853 Begin
  public List<String> getStreamAliases() {
	return streamAliases;
  }
	
  public void setStreamAliases(List<String> streamAliases) {
	this.streamAliases = streamAliases;
  }
  //Added by Brantzhang for patch HIVE-853 End
  
  //Added by Brantzhang for patch HIVE-870 Begin
  /**
   * Insert only a key to the semijoin table name to column names map. 
   * @param alias table name alias.
   */
  public void addRHSSemijoin(String alias) {
    if ( ! rhsSemijoin.containsKey(alias) ) {
      rhsSemijoin.put(alias, null);
    }
  }
    
  /**
   * Remeber the mapping of table alias to set of columns.
   * @param alias
   * @param columns
   */
  public void addRHSSemijoinColumns(String alias, ArrayList<ASTNode> columns) {
    ArrayList<ASTNode> cols = rhsSemijoin.get(alias);
    if ( cols == null ) {
      rhsSemijoin.put(alias, columns);
    } else {
      cols.addAll(columns);
    }
  }
    
  /**
   * Remeber the mapping of table alias to set of columns.
   * @param alias
   * @param columns
   */
  public void addRHSSemijoinColumns(String alias, ASTNode column) {
    ArrayList<ASTNode> cols = rhsSemijoin.get(alias);
    if ( cols == null ) {
      cols = new ArrayList<ASTNode>();
      cols.add(column);
      rhsSemijoin.put(alias, cols);
    } else {
      cols.add(column);
    }
  }
    
  public ArrayList<ASTNode> getRHSSemijoinColumns(String alias) {
    return rhsSemijoin.get(alias);
  }
    
  /**
   * Merge the rhs tables from another join tree.
   * @param src the source join tree
   */
  public void mergeRHSSemijoin(QBJoinTree src) {
    for (Entry<String, ArrayList<ASTNode>> e: src.rhsSemijoin.entrySet()) {
      String key = e.getKey();
      ArrayList<ASTNode> value = this.rhsSemijoin.get(key);
      if ( value == null ) {
        this.rhsSemijoin.put(key, e.getValue());
      } else {
        value.addAll(e.getValue());
      }
    }
  }
  //Added by Brantzhang for patch HIVE-870 End
}


