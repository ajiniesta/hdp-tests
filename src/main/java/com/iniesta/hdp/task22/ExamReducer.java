package com.iniesta.hdp.task22;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ExamReducer extends Reducer<DateWritable, DoubleWritable, DateWritable, DoubleWritable> {

	private DoubleWritable outValue = new DoubleWritable();

	@Override
	protected void reduce(DateWritable key, Iterable<DoubleWritable> values, Context ctx)
			throws IOException, InterruptedException {
		double acum = 0;
		for (DoubleWritable value : values) {
			acum += value.get();
		}
		outValue.set(acum);
		ctx.write(key, outValue);
	}
}
