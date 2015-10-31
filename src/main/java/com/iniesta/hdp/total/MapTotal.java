package com.iniesta.hdp.total;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapTotal extends Mapper<LongWritable, Text, TotalKey, TotalValue> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] splits = value.toString().split(",");
		TotalKey tk = new TotalKey();
		tk.setId(Integer.parseInt(splits[0]));
		tk.setDate(splits[1]);
		tk.setZip(Integer.parseInt(splits[2]));
		TotalValue tv = new TotalValue();
		tv.setValueA(Double.parseDouble(splits[3]));
		tv.setValueB(Double.parseDouble(splits[4]));
		context.write(tk, tv);
	}

}
