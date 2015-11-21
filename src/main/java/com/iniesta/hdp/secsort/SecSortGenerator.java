package com.iniesta.hdp.secsort;

import java.util.ArrayList;
import java.util.List;

import com.iniesta.hdp.DatasetGenerator;

public class SecSortGenerator extends DatasetGenerator{

	public static void main(String[] args) throws Exception {
		new SecSortGenerator(1).generateFiles();
	}
	
	public SecSortGenerator(int numFiles) {
		super(numFiles);
	}

	@Override
	public List<String> generateLine() {
		List<String> line = new ArrayList<String>();
		line.add(getRandomInt(100));
		line.add(getRandomDate(2014));		
		line.add(getRandomInt(30));
		line.add(getRandomDouble(1000));
		line.add(getRandomDouble(10000));
		return line;
	}

	@Override
	public int getNumLines() {
		return 20000;
	}

	
}
