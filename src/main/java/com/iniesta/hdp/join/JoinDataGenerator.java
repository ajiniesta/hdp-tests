package com.iniesta.hdp.join;

import java.util.ArrayList;
import java.util.List;

import com.iniesta.hdp.DatasetGenerator;

public class JoinDataGenerator {

	private final static String[] names = { "John", "Jane", "Michael", "Elisabeth", "Alex", "Alexandra", "Tom", "Kate",
			"Sammuel", "Samantha", "Paul", "Paula", "Chandler", "Rachel", "Marcus", "Lousie", "Daniel", "Rebecca" };
	private final static String[] surnames = { "Brown", "Keating", "Black", "Bellamy", "Martin", "Bahuer", "Skywalker",
			"Anderson", "Fitipaldi", "Hamilton" };

	public static void main(String[] args) throws Exception {
		new CustomerClientGenerator(args).generateFiles();
		new PurchaseDataGenerator(new String[]{"5"}).generateFiles();
	}

	static class CustomerClientGenerator extends DatasetGenerator {
		private int id;

		public CustomerClientGenerator(String[] args) {
			super(args, "customer");
			id = 0;
		}

		@Override
		public List<String> generateLine() {
			List<String> line = new ArrayList<String>();
			line.add("" + (++id));
			String fullName = super.getRandomString(names) + " " + super.getRandomString(names)
					+ " " + super.getRandomString(surnames);
			line.add(fullName);
			line.add(getRandomDate(1970));
			return line;
		}

		@Override
		public int getNumLines() {
			return 1000;
		}
	}

	static class PurchaseDataGenerator extends DatasetGenerator {

		public PurchaseDataGenerator(String[] args) {
			super(args, "purchase");
		}

		@Override
		public List<String> generateLine() {
			List<String> line = new ArrayList<String>();
			line.add(getRandomInt(1000));
			line.add(getRandomDate(2000));
			line.add(getRandomDouble(1000));
			return line;
		}

		@Override
		public int getNumLines() {
			return 5000;
		}
	}

}
