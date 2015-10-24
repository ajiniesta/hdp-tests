package com.iniesta.hdp.categories;

import java.util.ArrayList;
import java.util.List;

import com.iniesta.hdp.DatasetGenerator;

/**
 * Generate data for example of categories
 * @author antonio
 *
 */
public class GeneratorData extends DatasetGenerator{

	private static String[] concepts = { "gas", "electricity", "internet", "car", "groceries", "computer", "rent", "taxes" };
	
	public GeneratorData(String[] args) {
		super(args);
	}

	public static void main(String[] args) throws Exception {		
        new GeneratorData(args).generateFiles();
	}

	@Override
	public List<String> generateLine() {
		List<String> line = new ArrayList<String>();
		line.add(getRandomDate(2010));
		line.add(getRandomString(concepts));
		line.add(getRandomDouble(1000D));
		return line;
	}

	@Override
	public int getNumLines() {
		return 100000;
	}

	
}
