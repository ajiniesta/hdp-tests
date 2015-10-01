package com.iniesta.hdp.task1;

import java.io.IOException;
import java.security.PrivilegedAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class Task1 extends Configured implements Tool {

	private final static Logger LOG = Logger.getLogger(Task1.class);

	public static void main(String[] args) throws Exception {
		initLogs();
		int exitCode = ToolRunner.run(new Configuration(), new Task1(), args);
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

		if (args.length != 3) {
			System.out.println("Two parameters are required <input dir>, <weather file> and <output dir>");
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

	private int innerRun(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = getConf();

		conf.set("fs.defaultFS", "hdfs://sandbox.hortonworks.com:8020");
		conf.set("hadoop.job.ugi", "root");
		

		Job job = Job.getInstance(conf, "Task 1");
		job.setJarByClass(Task1.class);

		job.addCacheFile(new Path(args[1]).toUri());
		LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>Added file to the cache");
		Path file1 = new Path(args[1]);
		LOG.info("Name: " + file1.getName());
		FileSystem fsy = FileSystem.get(conf);
		LOG.info("Filesystem URI : " + fsy.getUri());
		FileStatus fs = fsy.getFileStatus(file1);
		LOG.info("path: " + fs.getPath().getName());
		LOG.info("owner: " + fs.getOwner());
		LOG.info("fs permission: " + fs.getPermission().toString());

		Path outputFile = new Path(args[2]);
		if(fsy.exists(outputFile)){
			fsy.delete(outputFile, true);
		}
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapperClass(MapperTask1.class);
		job.setReducerClass(ReducerTask1.class);

		job.setOutputKeyClass(Task1Key.class);
		job.setOutputValueClass(Task1Value.class);

		job.setNumReduceTasks(2);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
