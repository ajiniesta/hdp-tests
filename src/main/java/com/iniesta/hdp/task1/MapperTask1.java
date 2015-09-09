package com.iniesta.hdp.task1;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTask1 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void setup(Mapper.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		Path[] cache = context.getLocalCacheFiles();
//		URI[] files = context.getCacheFiles();
	}

	@Override
	protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.map(key, value, context);
	}

	
	
}
