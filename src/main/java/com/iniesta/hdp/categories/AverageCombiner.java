package com.iniesta.hdp.categories;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class AverageCombiner extends Reducer<CategoriesKey, CategoriesValue, CategoriesKey, CategoriesValue> {

	@Override
	protected void reduce(CategoriesKey key, Iterable<CategoriesValue> values,
			Context context)
					throws IOException, InterruptedException {
		double acum = 0;
		long count = 0;
		for (CategoriesValue val : values) {
			acum += val.getValue();
			count++;
		}
		CategoriesValue valAcum = new CategoriesValue();
		valAcum.setAcum(acum);
		valAcum.setCount(count);
		context.write(key, valAcum);
	}

	
}
