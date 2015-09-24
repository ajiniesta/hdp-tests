package com.iniesta.hdp.task1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTask1 extends Mapper<LongWritable, Text, Task1Key, Task1Value> {

	private static Map<Task1Key, WeatherData> weathers = new HashMap<Task1Key, WeatherData>();

	public enum Counters{
		HITS, NUM_SFO
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		URI[] cache = context.getCacheFiles();
		if (cache.length > 0) {
			Path path = new Path(cache[0]);
			FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
			FileStatus[] status = fileSystem.listStatus(path);
			for (FileStatus fileStatus : status) {
				loadWeatherFile(fileStatus, fileSystem, context);
			}
			System.out.println("weathers size: " + weathers.size());
		}
	}

	private void loadWeatherFile(FileStatus fileStatus, FileSystem fs, Context context) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
			String line = br.readLine();
			while ((line = br.readLine()) != null) {
				String[] splits = line.split(",");
				if (splits.length > 6) {
					Task1Key key = new Task1Key();
					key.setYear(Integer.parseInt(splits[1]));
					key.setMonth(Integer.parseInt(splits[2]));
					key.setDay(Integer.parseInt(splits[3]));
					WeatherData wd = new WeatherData();
					wd.setPrcp(Integer.parseInt(splits[4]));
					wd.setTmax(Integer.parseInt(splits[5]));
					wd.setTmin(Integer.parseInt(splits[6]));
					weathers.put(key, wd);
					context.getCounter(Counters.NUM_SFO).increment(1);
				}

			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] splits = value.toString().split(",");
		//System.out.println(splits[17]);
		if ("sfo".equalsIgnoreCase(splits[17])) {
			Task1Key outKey = new Task1Key();
			outKey.setYear(extractInteger(splits[0]));
			outKey.setMonth(extractInteger(splits[1]));
			outKey.setDay(extractInteger(splits[2]));
			WeatherData wd = weathers.get(outKey);
			if (wd != null) {
				Task1Value outValue = new Task1Value();
				outValue.setYear(extractInteger(splits[0]));
				outValue.setMonth(extractInteger(splits[1]));
				outValue.setDay(extractInteger(splits[2]));
				outValue.setDepTime(extractInteger(splits[4]));
				outValue.setArrTime(extractInteger(splits[6]));
				outValue.setUniqueCarrier(new Text(splits[8]));
				outValue.setFlightNumber(extractInteger(splits[9]));
				outValue.setActualElapsedTime(extractInteger(splits[11]));
				outValue.setArrDelay(extractInteger(splits[14]));
				outValue.setDepDelay(extractInteger(splits[15]));
				outValue.setOrigin(new Text(splits[16]));
				outValue.setDestination(new Text(splits[17]));
			
				outValue.setPrcp(wd.getPrcp());
				outValue.setTmax(wd.getTmax());
				outValue.setTmin(wd.getTmin());
				outKey.setArrDelay(outValue.getArrDelay());

				context.write(outKey, outValue);
				
				context.getCounter(Counters.HITS).increment(1);
			}
		}
	}

	private int extractInteger(String chunk) {
		try {
			return Integer.parseInt(chunk);
		} catch (NumberFormatException e) {
			return 0;
		}
	}

}
