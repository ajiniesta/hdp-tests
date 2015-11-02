package com.iniesta.hdp.total;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TotalRecordReader extends RecordReader<TotalKey, TotalValue> {

	private FSDataInputStream fsInput;
	private BufferedReader in;
	private TotalKey tk = new TotalKey();
	private TotalValue tv = new TotalValue();
	private long start, end, pos;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit)split;
		Configuration conf = context.getConfiguration();
		Path path = fileSplit.getPath();
		FileSystem fs = path.getFileSystem(conf);
		fsInput = fs.open(path);
		in = new BufferedReader(new InputStreamReader(fsInput));
		start = fileSplit.getStart();
		end = start + fileSplit.getLength();
//		fsInput.seek(start);
//		if(start!=0){
//			start += fsInput.read(0, new byte[]{}, 0, (int)Math.min(Integer.MAX_VALUE, end-start));
//		}
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean next = false;
		String line = in.readLine();
		if(line!=null){
			String[] words = line.split(",");
			tk.setId(Integer.parseInt(words[0]));
			tk.setDate(words[1]);
			tk.setZip(Integer.parseInt(words[2]));
			tv.setValueA(Double.parseDouble(words[3]));
			tv.setValueB(Double.parseDouble(words[4]));
			next = true;
			pos += line.length();
		}
		return next;
	}

	@Override
	public TotalKey getCurrentKey() throws IOException, InterruptedException {
		return tk;
	}

	@Override
	public TotalValue getCurrentValue() throws IOException, InterruptedException {
		return tv;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return Math.min(1.0f, (pos - start)/(float)(end-start));
	}

	@Override
	public void close() throws IOException {
		fsInput.close();
	}

}
