package com.iniesta.hdp.total;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class TotalDriver extends Configured implements Tool {

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
		int exitCode = ToolRunner.run(new Configuration(), new TotalDriver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Total");

		String input = getClass().getClassLoader().getResource("total/dataset").toString();
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path("output/total" + System.currentTimeMillis()));
		
		job.setMapperClass(MapTotal.class);
		job.setInputFormatClass(TotalInputFormat.class);
		
		job.setOutputKeyClass(TotalKey.class);
		job.setOutputValueClass(TotalValue.class);

		job.setReducerClass(ReducerTotal.class);

		job.setNumReduceTasks(2);

		job.setPartitionerClass(TotalOrderPartitioner.class);
		InputSampler.Sampler<TotalKey, TotalValue> sampler = new InputSampler.RandomSampler<TotalKey, TotalValue>(0.1,
				200);
		InputSampler.writePartitionFile(job, sampler);

		String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
		URI partitionUri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
		job.addCacheFile(partitionUri);

		boolean ok = job.waitForCompletion(true);

		return ok ? 0 : 1;
	}
}
