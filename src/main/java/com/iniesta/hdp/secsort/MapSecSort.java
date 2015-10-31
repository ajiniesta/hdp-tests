package com.iniesta.hdp.secsort;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapSecSort extends Mapper<LongWritable, Text, CompositeKey, DoubleWritable>{

	private DoubleWritable doubleValue = new DoubleWritable();
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
					throws IOException, InterruptedException {
		String[] split = value.toString().split(",");
		CompositeKey cKey = new CompositeKey();
		cKey.setId(Integer.parseInt(split[0]));
		cKey.setZip(Integer.parseInt(split[2]));
		doubleValue.set(Double.parseDouble(split[3]));
		context.write(cKey, doubleValue);
	}

	
}
