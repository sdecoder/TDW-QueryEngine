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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.Vector;          //Added by Brantzhang for patch HIVE-591
import org.apache.hadoop.hive.ql.parse.joinType;       //Added by Brantzhang for patch HIVE-870

/**
 * Join conditions Descriptor implementation.
 * 
 */
public class joinCond implements Serializable {
  private static final long serialVersionUID = 1L;
  private int left;
  private int right;
  private int type;
  private boolean preserved;     //Added by Brantzhang for patch HIVE-591

  public joinCond() {}

  public joinCond(int left, int right, int type) {
    this.left  = left;
    this.right = right;
    this.type  = type;
  }

  public joinCond(org.apache.hadoop.hive.ql.parse.joinCond condn) {
    //this.left     = condn.getLeft();        //Removed by Brantzhang for patch HIVE-591
    //this.right    = condn.getRight();       //Removed by Brantzhang for patch HIVE-591
	this.left       = condn.getLeft();        //Added by Brantzhang for patch HIVE-591
	this.right      = condn.getRight();       //Added by Brantzhang for patch HIVE-591
	this.preserved  = condn.getPreserved();   //Added by Brantzhang for patch HIVE-591   
    //org.apache.hadoop.hive.ql.parse.joinType itype = condn.getJoinType();  //Removed by Brantzhang for patch HIVE-870
    //if (itype == org.apache.hadoop.hive.ql.parse.joinType.INNER)           //Removed by Brantzhang for patch HIVE-870
	switch ( condn.getJoinType() ) {  //Added by Brantzhang for patch HIVE-870
    case INNER:                     //Added by Brantzhang for patch HIVE-870
      this.type = joinDesc.INNER_JOIN;
    //else if (itype == org.apache.hadoop.hive.ql.parse.joinType.LEFTOUTER)  //Removed by Brantzhang for patch HIVE-870
      break;         //Added by Brantzhang for patch HIVE-870
    case LEFTOUTER:  //Added by Brantzhang for patch HIVE-870
      this.type = joinDesc.LEFT_OUTER_JOIN;
    //else if (itype == org.apache.hadoop.hive.ql.parse.joinType.RIGHTOUTER)  //Removed by Brantzhang for patch HIVE-870
      break;         //Added by Brantzhang for patch HIVE-870
    case RIGHTOUTER: //Added by Brantzhang for patch HIVE-870
      this.type = joinDesc.RIGHT_OUTER_JOIN;
    //else if (itype == org.apache.hadoop.hive.ql.parse.joinType.FULLOUTER)   //Removed by Brantzhang for patch HIVE-870
      break;         //Added by Brantzhang for patch HIVE-870
    case FULLOUTER:  //Added by Brantzhang for patch HIVE-870
      this.type = joinDesc.FULL_OUTER_JOIN;
    //else if (itype == org.apache.hadoop.hive.ql.parse.joinType.UNIQUE)  //Added by Brantzhang for patch HIVE-591   //Removed by Brantzhang for patch HIVE-870
      break;         //Added by Brantzhang for patch HIVE-870
    case UNIQUE:     //Added by Brantzhang for patch HIVE-870
      this.type = joinDesc.UNIQUE_JOIN;                                 //Added by Brantzhang for patch HIVE-591
    //else           //Removed by Brantzhang for patch HIVE-870
      break;                                    //Added by Brantzhang for patch HIVE-870
    case LEFTSEMI:                              //Added by Brantzhang for patch HIVE-870
      this.type = joinDesc.LEFT_SEMI_JOIN;      //Added by Brantzhang for patch HIVE-870
      break;                                    //Added by Brantzhang for patch HIVE-870
    default:                                    //Added by Brantzhang for patch HIVE-870
      assert false;
	}               //Added by Brantzhang for patch HIVE-870
  }
  
  //Added by Brantzhang for patch HIVE-591 Begin
  /**
   * @return true if table is preserved, false otherwise
   */
  public boolean getPreserved() {
    return this.preserved;
  }
    
  /**
   * @param preserved if table is preserved, false otherwise
   */
  public void setPreserved(final boolean preserved) {
    this.preserved = preserved;
  }
  //Added by Brantzhang for patch HIVE-591 End
  
  public int getLeft() {
    return this.left;
  }

  public void setLeft(final int left) {
    this.left = left;
  }

  public int getRight() {
    return this.right;
  }

  public void setRight(final int right) {
    this.right = right;
  }

  public int getType() {
    return this.type;
  }

  public void setType(final int type) {
    this.type = type;
  }
  
  @explain
  public String getJoinCondString() {
    StringBuilder sb = new StringBuilder();
    
    switch(type) {
    case joinDesc.INNER_JOIN:
      sb.append("Inner Join ");
      break;
    case joinDesc.FULL_OUTER_JOIN:
      sb.append("Outer Join ");
      break;
    case joinDesc.LEFT_OUTER_JOIN:
      sb.append("Left Outer Join");
      break;
    case joinDesc.RIGHT_OUTER_JOIN:
      sb.append("Right Outer Join");
      break;
    case joinDesc.UNIQUE_JOIN:   //Added by Brantzhang for patch HIVE-591
      sb.append("Unique Join");  //Added by Brantzhang for patch HIVE-591
      break;                     //Added by Brantzhang for patch HIVE-591
    case joinDesc.LEFT_SEMI_JOIN:     //Added by Brantzhang for patch HIVE-870
      sb.append("Left Semi Join ");   //Added by Brantzhang for patch HIVE-870
      break;                          //Added by Brantzhang for patch HIVE-870
    default:
      sb.append("Unknow Join");
      break;
    }
    
    sb.append(left);
    sb.append(" to ");
    sb.append(right);
    
    return sb.toString();
  }
}
