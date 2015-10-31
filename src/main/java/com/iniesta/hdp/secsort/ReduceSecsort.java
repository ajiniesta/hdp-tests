package com.iniesta.hdp.secsort;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceSecsort extends Reducer<CompositeKey, DoubleWritable, CompositeKey, DoubleWritable>{

	private DoubleWritable value = new DoubleWritable();
	@Override
	protected void reduce(CompositeKey key, Iterable<DoubleWritable> values,
			Context context)
					throws IOException, InterruptedException {
		double acum = 0;
		for (DoubleWritable doubleWritable : values) {
			acum += doubleWritable.get();
		}
		value.set(acum);
		context.write(key, value);
	}

	
}
