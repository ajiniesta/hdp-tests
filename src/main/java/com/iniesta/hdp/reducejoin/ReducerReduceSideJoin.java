package com.iniesta.hdp.reducejoin;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class ReducerReduceSideJoin extends Reducer<ReduceJoinKey, ReduceJoinValue, ReduceJoinKey, ReduceJoinValue> {

	@Override
	protected void reduce(ReduceJoinKey key, Iterable<ReduceJoinValue> values, Context ctx)
			throws IOException, InterruptedException {
		double acum = 0;
		String name = "";
		for (ReduceJoinValue value : values) {
			acum += value.getValue();
			if(!"".equals(value.getName())){
				name = value.getName();
			}
		}
		if(!"".equals(name)){
			ReduceJoinValue value = new ReduceJoinValue();
			value.setName(name);
			value.setValue(acum);		
			ctx.write(key, value);
		}
	}

}
