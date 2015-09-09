package com.iniesta.hdp.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Task1 extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new Task1(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		
		if(args.length!=3){
			System.out.println("Two parameters are required <input dir>, <weather file> and <output dir>");
			return -1;
		}
		
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Task 1");
		job.setJarByClass(Task1.class);
		
		job.addCacheFile(new Path(args[1]).toUri());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setMapperClass(MapperTask1.class);
		job.setReducerClass(ReducerTask1.class);
		
		job.setNumReduceTasks(2);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
