package com.iniesta.hdp.reducejoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class DatasetMapper extends Mapper<LongWritable, Text, ReduceJoinKey, ReduceJoinValue>{

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
					throws IOException, InterruptedException {
		String[] fields = StringUtils.split(value.toString(), StringUtils.COMMA);
		ReduceJoinKey rjKey = new ReduceJoinKey();
		rjKey.setId(Integer.parseInt(fields[0]));
		rjKey.setDiscriminator(1);
		ReduceJoinValue rjValue = new ReduceJoinValue();
		rjValue.setValue(Double.parseDouble(fields[2]));
		context.write(rjKey, rjValue);
	}

}
