package com.iniesta.hdp.join;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DoubleSumReducer extends Reducer<JoinKey, DoubleWritable, JoinKey, DoubleWritable> {

	@Override
	protected void reduce(JoinKey key, Iterable<DoubleWritable> values, Context ctx)
			throws IOException, InterruptedException {
		double acum = 0;
		for (DoubleWritable val : values) {
			acum += val.get();
		}
		ctx.write(key, new DoubleWritable(acum));
	}

}
