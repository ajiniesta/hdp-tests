package com.iniesta.hdp.join;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperJoinSide extends Mapper<LongWritable, Text, JoinKey, DoubleWritable> {

	private Map<Integer, JoinKey> cache = new HashMap<Integer, JoinKey>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		URI[] cacheFiles = context.getCacheFiles();
		if (cacheFiles.length > 0) {
			Path path = new Path(cacheFiles[0]);
			FileSystem fs = path.getFileSystem(context.getConfiguration());
			FileStatus[] fileStatuses = fs.listStatus(path);
			for (FileStatus fileStatus : fileStatuses) {
				loadCustomer(fs, fileStatus, context);
			}
		}
	}

	private void loadCustomer(FileSystem fs, FileStatus fileStatus, Context context) throws IOException {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())))) {
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] words = line.split(",");
				JoinKey key = new JoinKey();
				int id = Integer.parseInt(words[0]);
				key.setId(id);
				key.setName(words[1]);
				cache.put(id, key);
			}
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] words = value.toString().split(",");
		int id = Integer.parseInt(words[0]);
		JoinKey joinKey = cache.get(id);
		if(joinKey!=null){
			context.write(joinKey, new DoubleWritable(Double.parseDouble(words[2])));
		}

	}

}
