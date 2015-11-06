package com.iniesta.hdp.categories;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class CategoriesDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new CategoriesDriver(), args);
		System.exit(exitCode);
	}
		
	public int run(String[] args) throws Exception {

		String dataset = getClass().getClassLoader().getResource("categories/dataset").toString();
		String output = "output/out"+System.currentTimeMillis();
		
		Configuration conf = getConf();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		
		Job job = Job.getInstance(conf, "Categories example");
		job.setJarByClass(CategoriesDriver.class);
		
		FileInputFormat.addInputPath(job, new Path(dataset));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setMapperClass(CategoriesMap.class);
		job.setReducerClass(CategoriesReducer.class);
		
		job.setOutputKeyClass(CategoriesKey.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setPartitionerClass(CategoriesPartitioner.class);
		
		job.setNumReduceTasks(12);
		
		boolean ok = job.waitForCompletion(true);
		
		return ok?0:1;
	}

	
}
