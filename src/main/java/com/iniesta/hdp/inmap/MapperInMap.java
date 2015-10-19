package com.iniesta.hdp.inmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperInMap extends Mapper<Object, Text, Text, LongWritable>{

	private static final LongWritable ONE = new LongWritable(1);
	private List<Word> words;
	private PriorityQueue<Word> queue;
	private int maxResults;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		words = new ArrayList<Word>();
		maxResults = context.getConfiguration().getInt("max", 10);
	}

	
//	@Override
	protected void map2(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] input = value.toString().split("\\s+");
		for (String word : input) {
			Word currentWord = new Word(word);  
			if(words.contains(currentWord)){
				words.get(words.indexOf(currentWord)).incrementFrequency();
			} else {
				words.add(currentWord);
			}
		}
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		while (tokenizer.hasMoreTokens()) {
			context.write(new Text(tokenizer.nextToken()), ONE);
		}
	}
	
//	@Override
	protected void cleanup22(Context context)
			throws IOException, InterruptedException {
		queue = new PriorityQueue<Word>(words);
		for (int i = 0; i < maxResults; i++) {
			Word polled = queue.poll();
			context.write(new Text(polled.getWord()), new LongWritable(polled.getFrequency()));
		}
	}

}
