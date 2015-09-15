package com.iniesta.hdp.task1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class TestTask1 {

	MapDriver<LongWritable, Text, Task1Key, Task1Value> mapDriver;
//	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
//	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	@Before
	public void setup(){
		MapperTask1 mapper = new MapperTask1();
		String file = getClass().getClassLoader().getResource("hdp-task1/datasets/sfo_weather.csv").getFile();
		mapDriver = MapDriver.newMapDriver((Mapper<LongWritable, Text, Task1Key, Task1Value>) mapper).withCacheFile(file);
	}
	
	@Test
	public void testMapper() throws Exception{
		mapDriver.withInput(new LongWritable(), new Text("2008,1,3,4,1426,1355,1605,1530,WN,488,N615SW,99,95,75,35,31,LAS,SFO,414,3,21,0,,0,0,0,22,0,13"));
		
		Task1Key key = new Task1Key();
		key.setYear(2008);
		key.setMonth(1);
		key.setDay(3);
		key.setArrDelay(35);
		
		Task1Value val = new Task1Value();
		//2008,01,03,43,150,94
		val.setYear(2008);
		val.setMonth(1);
		val.setDay(3);
		mapDriver.withOutput(key , val );
		mapDriver.runTest();
	}
}
