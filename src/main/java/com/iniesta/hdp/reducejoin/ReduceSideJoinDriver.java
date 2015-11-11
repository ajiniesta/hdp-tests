package com.iniesta.hdp.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceSideJoinDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new ReduceSideJoinDriver(), args);
		System.exit(exitCode);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
//		conf.set("iniesta.join.type","INNER");
//		System.out.println(">>>>>>>>>>>>"+conf.get("iniesta.join.type")+"<<<<<<<<<<<<");
		
		Job job = Job.getInstance(conf, "Reduce Side Join");
		String customerPath = getClass().getClassLoader().getResource("join/customer").toString();
		String datasetPath = getClass().getClassLoader().getResource("join/dataset").toString();
		MultipleInputs.addInputPath(job, new Path(customerPath), TextInputFormat.class, CustomerMapper.class);
		MultipleInputs.addInputPath(job, new Path(datasetPath), TextInputFormat.class, DatasetMapper.class);
		
		
		FileOutputFormat.setOutputPath(job, new Path("output/reducesidejoin_"+System.currentTimeMillis()));
		
		job.setOutputKeyClass(ReduceJoinKey.class);
		job.setOutputValueClass(ReduceJoinValue.class);
		
		job.setGroupingComparatorClass(ReduceSideJoinGroupingComparator.class);
		
		job.setReducerClass(ReducerReduceSideJoin.class);
		
		job.setNumReduceTasks(1);
		
		boolean ok = job.waitForCompletion(true);
		
		return ok ? 0 : 1;
	}
	
	

}
