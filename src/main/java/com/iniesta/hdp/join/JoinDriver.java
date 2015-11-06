package com.iniesta.hdp.join;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new JoinDriver(), args);
		System.exit(exitCode);;
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		
		Job job = Job.getInstance(conf, "Join MR Map side");

		String inputDataset = getClass().getClassLoader().getResource("join/dataset").toString();
		URL inputCustomer = getClass().getClassLoader().getResource("join/customer");
		FileInputFormat.addInputPath(job, new Path(inputDataset));
		FileOutputFormat.setOutputPath(job, new Path("output/mapsidejoin_"+System.currentTimeMillis()));
		
		job.addCacheFile(inputCustomer.toURI());
		
		job.setMapperClass(MapperJoinSide.class);
		job.setOutputKeyClass(JoinKey.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setReducerClass(DoubleSumReducer.class);
		
		job.setNumReduceTasks(1);
		
		boolean ok = job.waitForCompletion(true);
		return ok ? 0 : 1;
	}
	
	
}
