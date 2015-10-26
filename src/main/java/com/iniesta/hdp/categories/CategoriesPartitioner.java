package com.iniesta.hdp.categories;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CategoriesPartitioner extends Partitioner<CategoriesKey, Writable> {

	@Override
	public int getPartition(CategoriesKey key, Writable value, int numPartitions) {
		return key.getMonth();
	}

}
