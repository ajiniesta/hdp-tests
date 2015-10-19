package com.iniesta.hdp.inmap;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerInMap extends Reducer<Text, LongWritable, Text, LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long acum = 0L;
		for (LongWritable value : values) {
			acum += value.get();
		}
		context.write(key, new LongWritable(acum));
	}

}
