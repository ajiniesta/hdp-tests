package com.iniesta.hdp.sandbox;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PlayingFiles {

	public static void main(String[] args) throws Exception {
		Path path = new Path("hdfs://sandbox.hortonworks.com/user/root/weather/sfo_weather.csv");
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		auxPrint(br);
		br.close();
		
		path = new Path("weather");
		FileStatus[] status = fs.listStatus(path);
		br = new BufferedReader(new InputStreamReader(fs.open(status[0].getPath())));
		auxPrint(br);
		br.close();
	}

	private static void auxPrint(BufferedReader br) throws IOException {
		String line;
		int count = 4;
		while((line = br.readLine())!=null){
			System.out.println(line);
			if(count--==0){
				break;
			}
		}
	}
}
