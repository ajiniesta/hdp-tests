package com.iniesta.hdp.categories;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CategoriesPartitioner extends Partitioner<CategoriesKey, DoubleWritable> {

	@Override
	public int getPartition(CategoriesKey key, DoubleWritable value, int numPartitions) {
		return key.getMonth();
	}

}
