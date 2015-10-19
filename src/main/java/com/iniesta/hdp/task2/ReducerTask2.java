package com.iniesta.hdp.task2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerTask2 extends Reducer<DateWritable, DoubleWritable, DateWritable, DoubleWritable>{

	private Text empty = new Text("");
	
	@Override
	protected void reduce(DateWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double acum = 0;
		for (DoubleWritable i : values) {
			acum += i.get();
		}
		
		context.write(key, new DoubleWritable(acum));
	}

	
}
