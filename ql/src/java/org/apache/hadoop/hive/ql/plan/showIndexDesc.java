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
import org.apache.hadoop.fs.Path;

@explain(displayName = "Show Indexs")
public class showIndexDesc extends ddlDesc implements Serializable
{
    private static final long serialVersionUID = 1L;

    public static enum showIndexTypes
    {
        SHOWTABLEINDEX, SHOWALLINDEX
    };

    String dbName;
    String tblName;
    showIndexTypes op;

    Path resFile;

    /**
     * @param tabName
     *            Name of the table whose partitions need to be listed
     * @param resFile
     *            File to store the results in
     */
    public showIndexDesc(String dbName, String tblName, showIndexTypes op)
    {
        this.tblName = tblName;
        this.dbName = dbName;
        this.op = op;
    }

    /**
     * @return the name of the table
     */
    @explain(displayName = "table")
    public String getTblName()
    {
        return tblName;
    }

    /**
     * @param tabName
     *            the table whose partitions have to be listed
     */
    public void setTblName(String tblName)
    {
        this.tblName = tblName;
    }

    /**
     * @return the name of the table
     */
    @explain(displayName = "db")
    public String getDBName()
    {
        return dbName;
    }

    /**
     * @param tabName
     *            the table whose partitions have to be listed
     */
    public void setDBName(String dbName)
    {
        this.dbName = dbName;
    }
    
    public showIndexTypes getOP()
    {
        return op;
    }
}
