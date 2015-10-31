package com.iniesta.hdp.secsort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecSortPartitioner extends Partitioner<CompositeKey, DoubleWritable>{

	@Override
	public int getPartition(CompositeKey key, DoubleWritable value, int numPartitions) {
		return key.getZip() % 2;
	}

}
