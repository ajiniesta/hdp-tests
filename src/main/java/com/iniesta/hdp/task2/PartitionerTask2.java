package com.iniesta.hdp.task2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerTask2 extends Partitioner<DateWritable, DoubleWritable> {

	private final static Decades decades = new Decades(1960, 2015);
	
	@Override
	public int getPartition(DateWritable key, DoubleWritable value, int numPartitions) {
		return decades.inWhichDecadeAmI(key.getYearOfTheDate());
	}

}
