package com.iniesta.hdp.total;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerTotal extends Reducer<TotalKey, TotalValue, TotalKey, DoubleWritable> {

	@Override
	protected void reduce(TotalKey key, Iterable<TotalValue> values, Context context)
			throws IOException, InterruptedException {
		double acum = 0;		
		for (TotalValue totalValue : values) {
			acum += totalValue.getValueA() + totalValue.getValueB(); 
		}
		context.write(key, new DoubleWritable(acum));
	}

}
