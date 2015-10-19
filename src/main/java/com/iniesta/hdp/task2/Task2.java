package com.iniesta.hdp.task2;

import java.security.PrivilegedAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class Task2 extends Configured implements Tool {

	private final static Logger LOG = Logger.getLogger(Task2.class);
	
	public static void main(String[] args) throws Exception {
		initLogs();
		int exitCode = ToolRunner.run(new Configuration(), new Task2(), args);
		System.exit(exitCode);
	}

	private static void initLogs() {
		ConsoleAppender console = new ConsoleAppender(); // create appender
		// configure the appender
		String PATTERN = "%d [%p|%c|%C{1}] %m%n";
		console.setLayout(new PatternLayout(PATTERN));
		console.setThreshold(Level.INFO);
		console.activateOptions();
		// add appender to any Logger (here is root)
		Logger.getRootLogger().addAppender(console);
	}
	
	public int run(final String[] args) throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");

		if (args.length < 2) {
			System.out.println("Two parameters are required <input dir> and <output dir> and an optional boolean to says to use combiner");
			return -1;
		}
		
		Integer retCode = ugi.doAs(new PrivilegedAction<Integer>() {

			public Integer run() {
				try {
					return innerRun(args);
				} catch (Exception e) {
					LOG.error("Global exception", e);
					return -1;
				}
			}
		});
		return retCode;

	}

	protected Integer innerRun(String[] args) throws Exception {
		Configuration conf = getConf();
		boolean usingCombiner = (args.length > 2 && "true".equalsIgnoreCase(args[2]));
		
		conf.set("fs.defaultFS", "hdfs://sandbox.hortonworks.com:8020");
		conf.set("hadoop.job.ugi", "root");

		conf.set("mapred.textoutputformat.separator", ",");
		
		Job job = Job.getInstance(conf, "Task 2"+(usingCombiner?" with Combiner":""));
		job.setJarByClass(Task2.class);
		
		FileSystem fsy = FileSystem.get(conf);
		Path outputFile = new Path(args[1]);
		if(fsy.exists(outputFile)){
			fsy.delete(outputFile, true);
		}

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MapperTask2.class);
		job.setReducerClass(ReducerTask2.class);

		job.setOutputKeyClass(DateWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		if(usingCombiner){
			LOG.info(">>>>>>>>>>>Using Combiner<<<<<<<<<<<<");
			job.setCombinerClass(ReducerTask2.class);
		}
		
		job.setPartitionerClass(PartitionerTask2.class);
		
		Decades decades = new Decades(1960, 2015);
		job.setNumReduceTasks(decades.getNumDecades());
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
