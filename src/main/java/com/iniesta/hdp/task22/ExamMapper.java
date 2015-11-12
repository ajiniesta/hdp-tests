package com.iniesta.hdp.task22;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class ExamMapper extends Mapper<LongWritable, Text, DateWritable, DoubleWritable> {

	private DateWritable outKey = new DateWritable();
	private DoubleWritable outValue = new DoubleWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
		try{
			String[] fields = StringUtils.split(value.toString(), StringUtils.COMMA);
			outKey.setDate(fields[0]);
			outValue.set(Double.parseDouble(fields[1]));
			ctx.write(outKey, outValue);
		}catch(Exception e){
			//Nothing need to do, the format of the key or the value wasn't OK, so we don't emit record
		}
	}

}
