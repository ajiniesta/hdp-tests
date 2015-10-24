package com.iniesta.hdp.categories;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CategoriesReducer extends Reducer<CategoriesKey, DoubleWritable, CategoriesKey, DoubleWritable> {

	@Override
	protected void reduce(CategoriesKey key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		double acum = 0;
		long count = 0;
		for (DoubleWritable doubleValue : values) {
			acum += doubleValue.get();
			count++;
		}
		DoubleWritable value = new DoubleWritable(acum/count);
		context.write(key, value);
	}

}
