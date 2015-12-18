package org.apache.hadoop.hive.ql.parse;

public enum PartitionType{
	RANGE_PARTITION,//should PARTITION
	LIST_PARTITION,
	HASH_PARTITION//modified by Brantzhang
};
