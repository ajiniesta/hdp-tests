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
		super.setup(context);
		URI[] cache = context.getCacheFiles();
		for (URI uri : cache) {
			System.out.println(uri);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
		super.map(key, value, context);
		
		
	}

	
	
}
