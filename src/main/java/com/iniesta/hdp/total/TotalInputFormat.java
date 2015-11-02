package com.iniesta.hdp.total;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class TotalInputFormat extends FileInputFormat<TotalKey, TotalValue>{

	@Override
	public RecordReader<TotalKey, TotalValue> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new TotalRecordReader();
	}

}
