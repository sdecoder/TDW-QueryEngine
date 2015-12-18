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

public class DDLWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private createTableDesc      createTblDesc;
  private createTableLikeDesc  createTblLikeDesc;
  private dropTableDesc        dropTblDesc;
  private truncateTableDesc    truncTblDesc;
  private alterTableDesc       alterTblDesc;
  private showTablesDesc       showTblsDesc;
  private showFunctionsDesc    showFuncsDesc;
  private descFunctionDesc     descFunctionDesc;
  private showPartitionsDesc   showPartsDesc;
  private descTableDesc        descTblDesc;
  private AddPartitionDesc addPartitionDesc;
  private MsckDesc msckDesc;
  
  private addDefaultPartitionDesc addDefaultPartDesc;
  private truncatePartitionDesc truncatePartDesc;
  
  private createDatabaseDesc createDbDesc;
  private dropDatabaseDesc dropDbDesc;
  
  private useDatabaseDesc useDbDesc;
  
  private showDBDesc showDbDesc;

  private showIndexDesc  showIndexsDesc;
  public DDLWork() { }

  
  public DDLWork(createDatabaseDesc createDbDesc) {
	super();
	this.createDbDesc = createDbDesc;
}

  public DDLWork(showDBDesc desc) {
		super();
		this.showDbDesc = desc;
	}

public showDBDesc getShowDbDesc() {
	return showDbDesc;
}


public void setShowDbDesc(showDBDesc showDbDesc) {
	this.showDbDesc = showDbDesc;
}


public useDatabaseDesc getUseDbDesc() {
	return useDbDesc;
}


public void setUseDbDesc(useDatabaseDesc useDbDesc) {
	this.useDbDesc = useDbDesc;
}


public DDLWork(useDatabaseDesc useDbDesc) {
	super();
	this.useDbDesc = useDbDesc;
}


public createDatabaseDesc getCreateDbDesc() {
	return createDbDesc;
}


public void setCreateDbDesc(createDatabaseDesc createDbDesc) {
	this.createDbDesc = createDbDesc;
}


public dropDatabaseDesc getDropDbDesc() {
	return dropDbDesc;
}


public void setDropDbDesc(dropDatabaseDesc dropDbDesc) {
	this.dropDbDesc = dropDbDesc;
}


public DDLWork(dropDatabaseDesc dropDbDesc) {
	super();
	this.dropDbDesc = dropDbDesc;
}


/**
   * @param alterTblDesc alter table descriptor
   */
  public DDLWork(alterTableDesc alterTblDesc) {
    this.alterTblDesc = alterTblDesc;
  }

  public DDLWork(truncatePartitionDesc tpd) {
	    this.truncatePartDesc = tpd;
	  }
  
  
  public truncatePartitionDesc getTruncatePartDesc() {
	return truncatePartDesc;
}

public void setTruncatePartDesc(truncatePartitionDesc truncatePartDesc) {
	this.truncatePartDesc = truncatePartDesc;
}

public DDLWork(addDefaultPartitionDesc adpd) {
	    this.addDefaultPartDesc = adpd;
	  }

  /**
   * @param createTblDesc create table descriptor
   */
  public DDLWork(createTableDesc createTblDesc) {
    this.createTblDesc = createTblDesc;
  }

  /**
   * @param createTblLikeDesc create table dlike escriptor
   */
  public DDLWork(createTableLikeDesc createTblLikeDesc) {
    this.createTblLikeDesc = createTblLikeDesc;
  }

  /**
   * @param dropTblDesc drop table descriptor
   */
  public DDLWork(dropTableDesc dropTblDesc) {
    this.dropTblDesc = dropTblDesc;
  }

  /**
   * @param truncTblDesc truncate table descriptor
   */
  public DDLWork(truncateTableDesc truncTblDesc) {
    this.truncTblDesc = truncTblDesc;
  }

  /**
   * @param descTblDesc
   */
  public DDLWork(descTableDesc descTblDesc) {
    this.descTblDesc = descTblDesc;
  }

  /**
   * @param showTblsDesc
   */
  public DDLWork(showTablesDesc showTblsDesc) {
    this.showTblsDesc = showTblsDesc;
  }

  /**
   * @param showFuncsDesc
   */
  public DDLWork(showFunctionsDesc showFuncsDesc) {
    this.showFuncsDesc = showFuncsDesc;
  }
  
  /**
   * @param descFuncDesc
   */
  public DDLWork(descFunctionDesc descFuncDesc) {
    this.descFunctionDesc = descFuncDesc;
  }

  /**
   * @param showPartsDesc
   */
  public DDLWork(showPartitionsDesc showPartsDesc) {
    this.showPartsDesc = showPartsDesc;
  }
  
  /**
   * @param addPartitionDesc information about the partitions
   * we want to add.
   */
  public DDLWork(AddPartitionDesc addPartitionDesc) {
    this.addPartitionDesc = addPartitionDesc;
  }

  public DDLWork(MsckDesc checkDesc) {
    this.msckDesc = checkDesc;
  }

  /**
   * @return the createTblDesc
   */
  @explain(displayName="Create Table Operator")
  public createTableDesc getCreateTblDesc() {
    return createTblDesc;
  }

  /**
   * @param createTblDesc the createTblDesc to set
   */
  public void setCreateTblDesc(createTableDesc createTblDesc) {
    this.createTblDesc = createTblDesc;
  }

  /**
   * @return the createTblDesc
   */
  @explain(displayName="Create Table Operator")
  public createTableLikeDesc getCreateTblLikeDesc() {
    return createTblLikeDesc;
  }

  /**
   * @param createTblLikeDesc the createTblDesc to set
   */
  public void setCreateTblLikeDesc(createTableLikeDesc createTblLikeDesc) {
    this.createTblLikeDesc = createTblLikeDesc;
  }

  /**
   * @return the dropTblDesc
   */
  @explain(displayName="Drop Table Operator")
  public dropTableDesc getDropTblDesc() {
    return dropTblDesc;
  }

  /**
   * @param dropTblDesc the dropTblDesc to set
   */
  public void setDropTblDesc(dropTableDesc dropTblDesc) {
    this.dropTblDesc = dropTblDesc;
  }

  /**
   * @return the truncTblDesc
   */
  @explain(displayName="Truncate Table Operator")
  public truncateTableDesc getTruncTblDesc() {
    return truncTblDesc;
  }

  /**
   * @param truncTblDesc the truncTblDesc to set
   */
  public void setTruncTblDesc(truncateTableDesc truncTblDesc) {
    this.truncTblDesc = truncTblDesc;
  }

  /**
   * @return the alterTblDesc
   */
  @explain(displayName="Alter Table Operator")
  public alterTableDesc getAlterTblDesc() {
    return alterTblDesc;
  }

  /**
   * @param alterTblDesc the alterTblDesc to set
   */
  public void setAlterTblDesc(alterTableDesc alterTblDesc) {
    this.alterTblDesc = alterTblDesc;
  }

  /**
   * @return the showTblsDesc
   */
  @explain(displayName="Show Table Operator")
  public showTablesDesc getShowTblsDesc() {
    return showTblsDesc;
  }

  /**
   * @param showTblsDesc the showTblsDesc to set
   */
  public void setShowTblsDesc(showTablesDesc showTblsDesc) {
    this.showTblsDesc = showTblsDesc;
  }

  /**
   * @return the showFuncsDesc
   */
  @explain(displayName="Show Function Operator")
  public showFunctionsDesc getShowFuncsDesc() {
    return showFuncsDesc;
  }
  
  /**
   * @return the descFuncDesc
   */
  @explain(displayName="Show Function Operator")
  public descFunctionDesc getDescFunctionDesc() {
    return descFunctionDesc;
  }

  /**
   * @param showFuncsDesc the showFuncsDesc to set
   */
  public void setShowFuncsDesc(showFunctionsDesc showFuncsDesc) {
    this.showFuncsDesc = showFuncsDesc;
  }
  
  /**
   * @param descFuncDesc the showFuncsDesc to set
   */
  public void setDescFuncDesc(descFunctionDesc descFuncDesc) {
    this.descFunctionDesc = descFuncDesc;
  }

  /**
   * @return the showPartsDesc
   */
  @explain(displayName="Show Partitions Operator")
  public showPartitionsDesc getShowPartsDesc() {
    return showPartsDesc;
  }

  /**
   * @param showPartsDesc the showPartsDesc to set
   */
  public void setShowPartsDesc(showPartitionsDesc showPartsDesc) {
    this.showPartsDesc = showPartsDesc;
  }

  /**
   * @return the descTblDesc
   */
  @explain(displayName="Describe Table Operator")
  public descTableDesc getDescTblDesc() {
    return descTblDesc;
  }

  /**
   * @param descTblDesc the descTblDesc to set
   */
  public void setDescTblDesc(descTableDesc descTblDesc) {
    this.descTblDesc = descTblDesc;
  }

  /**
   * @return information about the partitions
   * we want to add.
   */
  public AddPartitionDesc getAddPartitionDesc() {
    return addPartitionDesc;
  }

  /**
   * @param addPartitionDesc information about the partitions
   * we want to add.
   */
  public void setAddPartitionDesc(AddPartitionDesc addPartitionDesc) {
    this.addPartitionDesc = addPartitionDesc;
  }

  public DDLWork(showIndexDesc showIndexsDesc)
  {
      this.showIndexsDesc = showIndexsDesc;
  }
  
  public showIndexDesc getShowIndexDesc()
  {
      return showIndexsDesc;
  }
    
  public void setShowIndexDesc(showIndexDesc showIndexsDesc)
  {
      this.showIndexsDesc = showIndexsDesc;
  }
  
  /**
   * @return Metastore check description
   */
  public MsckDesc getMsckDesc() {
    return msckDesc;
  }

  /**
   * @param msckDesc metastore check description
   */
  public void setMsckDesc(MsckDesc msckDesc) {
    this.msckDesc = msckDesc;
  }

public addDefaultPartitionDesc getAddDefaultPartDesc() {
	return addDefaultPartDesc;
}

public void setAddDefaultPartDesc(addDefaultPartitionDesc addDefaultPartDesc) {
	this.addDefaultPartDesc = addDefaultPartDesc;
}
  
}
