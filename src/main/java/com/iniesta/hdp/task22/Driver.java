package com.iniesta.hdp.task22;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),  new Driver(), args);
		System.exit(exitCode);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		
		Job job = Job.getInstance(conf, "Another example for the task2");
		job.setJarByClass(Driver.class);
		
		String input = getClass().getClassLoader().getResource("hdp-task2/dataset/1-year").toString();
		FileInputFormat.addInputPath(job, new Path(input));		
		input = getClass().getClassLoader().getResource("hdp-task2/dataset/5-year").toString();
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path("output/task2_"+System.currentTimeMillis()));
		
		job.setMapperClass(ExamMapper.class);
		job.setCombinerClass(ExamReducer.class);
		job.setReducerClass(ExamReducer.class);
		
		job.setOutputKeyClass(DateWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setPartitionerClass(ExamPartitioner.class);
		
		job.setNumReduceTasks(6);
		
		boolean ok = job.waitForCompletion(true);
		return ok ? 0 : 1;
	}

}
