package com.iniesta.hdp.task1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerTask1 extends Reducer<Task1Key, Task1Value, Task1Value, Text> {

	private final static Text empty = new Text("");
	
	@Override
	protected void reduce(Task1Key key, Iterable<Task1Value> values, Context context) throws IOException, InterruptedException {
		for (Task1Value value : values) {
			context.write(value, empty);
		}
	}
	
}
