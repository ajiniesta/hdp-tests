package com.iniesta.hdp.cities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Cities extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new Cities(), args);
		System.exit(status);
	}

	
	public int run(String[] arg0) throws Exception {
		Configuration conf = this.getConf();
		
		Job job = Job.getInstance(conf, "Test with citites in US");
		job.setJarByClass(Cities.class);
		
		int code = job.waitForCompletion(true) ? 0 : 1;
		return code;
	}
}
