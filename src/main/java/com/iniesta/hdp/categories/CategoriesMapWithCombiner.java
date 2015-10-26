package com.iniesta.hdp.categories;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CategoriesMapWithCombiner extends Mapper<LongWritable, Text, CategoriesKey, CategoriesValue> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split(",");
		CategoriesKey ck = new CategoriesKey();
		try{
			ck.setDate(split[0]);
			ck.setConcept(split[1]);
			context.write(ck, new CategoriesValue(new Double(split[2])));
		}catch(ParseException e){
			e.printStackTrace();
		}
	}

}
