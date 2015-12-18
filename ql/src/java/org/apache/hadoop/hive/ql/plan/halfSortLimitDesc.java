/** 
 * halfSortLimitDesc.java
 *
 * @by xinjieli
 */

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;

@explain(displayName="HalfSortLimit")
public class halfSortLimitDesc implements Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * Columns using for sort.
	 */
	private ArrayList<exprNodeDesc> sortCols;
	private String order;
	private int limit;

	public halfSortLimitDesc() {}

	public halfSortLimitDesc(final ArrayList<exprNodeDesc> sortCols, final String order, final int limit) {
		this.sortCols = sortCols;
		this.order = order;
		this.limit = limit;
	}

	public int getLimit() {
		return this.limit;
	}
	public void setLimit(final int limit) {
		this.limit = limit;
	}

	public String getOrder() {
		return this.order;
	}
	public void setOrder(final String order) {
		this.order = order;
	}

	public ArrayList<exprNodeDesc> getSortCols() {
		return sortCols;
	}
	public void setSortCols(final ArrayList<exprNodeDesc> sortCols) {
		this.sortCols = sortCols;
	}
}

