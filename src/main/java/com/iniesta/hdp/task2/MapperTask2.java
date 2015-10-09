package com.iniesta.hdp.task2;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTask2 extends Mapper<LongWritable, Text, DateWritable, DoubleWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] splits = value.toString().split(",");
		if(splits.length==2){
			try{
			DateWritable keyDate = new DateWritable();
			keyDate.fillDate(splits[0]);
			BigDecimal bd = new BigDecimal(splits[1]);
			DoubleWritable valueDouble = new DoubleWritable(bd.doubleValue());
			context.write(keyDate , valueDouble);
			}catch(Exception e){
				
			}
		}
	}

	
}
