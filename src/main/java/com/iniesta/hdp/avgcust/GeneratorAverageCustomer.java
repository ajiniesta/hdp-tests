package com.iniesta.hdp.avgcust;

import java.util.ArrayList;
import java.util.List;

import com.iniesta.hdp.DatasetGenerator;

public class GeneratorAverageCustomer {

	public static void main(String[] args) throws Exception {
		new GeneratorFormat1(1).generateFiles();
		new GeneratorFormat2(1).generateFiles();
		new GeneratorFormat3(1).generateFiles();
	}

	static class GeneratorFormat1 extends DatasetGenerator {

		public GeneratorFormat1(int numFiles) {
			super(numFiles, "first");
		}

		@Override
		public List<String> generateLine() {
			List<String> line = new ArrayList<>();
			line.add(getRandomDateDMY(1960));
			line.add(getRandomInt(20));
			line.add(getRandomDouble(1000));
			return line;
		}

		@Override
		public int getNumLines() {
			return 1000;
		}
		
	}

	static class GeneratorFormat2 extends DatasetGenerator {

		public GeneratorFormat2(int numFiles) {
			super(numFiles, "second");
		}

		@Override
		public List<String> generateLine() {
			List<String> line = new ArrayList<>();			
			line.add(getRandomInt(20));
			line.add(getRandomDate(1960));
			line.add(getRandomDouble(1000));
			return line;
		}

		@Override
		public int getNumLines() {
			return 1000;
		}
	}

	static class GeneratorFormat3 extends DatasetGenerator {

		public GeneratorFormat3(int numFiles) {
			super(numFiles, "third");
		}

		@Override
		public List<String> generateLine() {
			List<String> line = new ArrayList<>();
			line.add(getRandomDouble(1000));
			line.add(getRandomInt(20));
			line.add(getRandomDateDMY(1960));
			return line;
		}
		
		@Override
		public String getFieldSeparator() {
			return ";";
		}

		@Override
		public int getNumLines() {
			return 1000;
		}
	}

}
