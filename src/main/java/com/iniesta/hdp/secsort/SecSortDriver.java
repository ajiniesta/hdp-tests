package com.iniesta.hdp.secsort;

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

public class SecSortDriver extends Configured implements Tool {

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

	public static void main(String[] args) throws Exception {
		initLogs();
		int exitCode = ToolRunner.run(new Configuration(), new SecSortDriver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "Secundary sort example");

		String input = getClass().getClassLoader().getResource("secsort/dataset").toString();
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path("output/secsort"+System.currentTimeMillis()));

		job.setMapperClass(MapSecSort.class);
		job.setOutputKeyClass(CompositeKey.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setGroupingComparatorClass(SecSortGroupingComparator.class);
		
		job.setReducerClass(ReduceSecsort.class);
		
		job.setPartitionerClass(SecSortPartitioner.class);
		
		job.setNumReduceTasks(2);
		
		boolean ok = job.waitForCompletion(true);

		return ok ? 0 : 1;
	}

}
