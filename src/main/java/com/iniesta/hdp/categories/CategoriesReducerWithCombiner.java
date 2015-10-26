package com.iniesta.hdp.categories;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CategoriesReducerWithCombiner extends Reducer<CategoriesKey, CategoriesValue, CategoriesKey, DoubleWritable> {

	@Override
	protected void reduce(CategoriesKey key, Iterable<CategoriesValue> values, Context context)
			throws IOException, InterruptedException {
		double acum = 0;
		long count = 0;
		for (CategoriesValue value : values) {
			acum += value.getAcum();
			count += value.getCount();
		}
		DoubleWritable value = new DoubleWritable(acum/count);
		context.write(key, value);
	}

}
