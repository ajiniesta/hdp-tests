package com.iniesta.hdp.task1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTask1 extends Mapper<LongWritable, Text, Text, Text> {

	private static List<WeatherData> weathers = new ArrayList<WeatherData>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		URI[] cache = context.getCacheFiles();
		if(cache.length>0){
			Path path = new Path(cache[0]);
			FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
			FileStatus[] status = fileSystem.listStatus(path);
			for (FileStatus fileStatus : status) {
				loadWeatherFile(fileStatus, fileSystem);
			}
			System.out.println("weathers size: " + weathers.size());
		}
	}

	private void loadWeatherFile(FileStatus fileStatus, FileSystem fs) {
		BufferedReader br = null;
		try{
			br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
			String line = br.readLine();
			while((line = br.readLine())!=null){
				String[] splits = line.split(",");
				if(splits.length>6){
					WeatherData wd = new WeatherData();
					wd.setYear(Integer.parseInt(splits[1]));
					wd.setMonth(Integer.parseInt(splits[2]));
					wd.setDay(Integer.parseInt(splits[3]));
					wd.setPrcp(Integer.parseInt(splits[4]));
					wd.setTmax(Integer.parseInt(splits[5]));
					wd.setTmin(Integer.parseInt(splits[6]));
					weathers.add(wd);
				}
				
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] splits = value.toString().split(",");
//		if("sfo".equalsIgnoreCase(splits[17])){
//			context.write(key, value);
//		}
	}

	
	
}
