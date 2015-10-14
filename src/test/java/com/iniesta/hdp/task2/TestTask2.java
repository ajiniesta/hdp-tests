package com.iniesta.hdp.task2;

import static org.junit.Assert.*;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.Before;
import org.junit.Test;

public class TestTask2 {

	MapDriver<LongWritable, Text, DateWritable, DoubleWritable> mapDriver;
	ReduceDriver<DateWritable, DoubleWritable, Text, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, DateWritable, DoubleWritable, Text, Text> mapReduceDriver;
	
	@Before
	public void setup(){
		MapperTask2 mapper = new MapperTask2();
		ReducerTask2 reducer = new ReducerTask2();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		initLogs();
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

	
	@Test
	public void testMap() throws ParseException, IOException {
		mapDriver.withInput(new LongWritable(), new Text("1960/3/14,0.25"));
		DateWritable key = new DateWritable();
		key.fillDate("1960/3/14");
		mapDriver.withOutput(key , new DoubleWritable(0.25));
		mapDriver.runTest();
	}
	
	@Test
	public void testReduce()throws Exception{
		DateWritable key = new DateWritable();
		key.fillDate("1960/3/14");
		List<DoubleWritable> values = Arrays.asList(new DoubleWritable(0.25),new DoubleWritable(0.1));
		reduceDriver.withInput(key, values);
		reduceDriver.withOutput(new Text("1960/03/14,0.35"), new Text(""));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException{
		mapReduceDriver.withInput(new LongWritable(), new Text("1960/3/14,0.25"));
		mapReduceDriver.withOutput(new Text("1960/03/14,0.25"), new Text(""));
		mapReduceDriver.runTest();
	}
}
