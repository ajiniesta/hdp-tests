package com.iniesta.hdp.task22;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.StringUtils;

public class ExamPartitioner extends Partitioner<DateWritable, DoubleWritable>{

	@Override
	public int getPartition(DateWritable key, DoubleWritable value, int numPartitions) {
		int partition = 0;
		String date = key.getDate();
		if(date!=null){
			int year = getYear(date);
			partition = getPartition(year);
		}
		return partition;
	}

	private int getPartition(int year) {
		int partition = 0;
		if(year >= 1970 && year < 1980 ){
			partition = 1;
		} else if(year >= 1980 && year < 1990 ){
			partition = 2;
		} else if(year >= 1990 && year < 2000 ){
			partition = 3;
		} else if(year >= 2000 && year < 2010 ){
			partition = 4;
		} else if(year >= 2010 ){
			partition = 5;
		}
		return partition;
	}

	private int getYear(String date) {
		String[] splits = StringUtils.split(date, '/');
		return Integer.parseInt(splits[0]);		
	}

	
}
