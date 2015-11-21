package com.iniesta.hdp;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.Random;

public abstract class DatasetGenerator {

	private int numFiles;
	private Random random;
	private DecimalFormat formatter;
	private String prefix;
	
	public DatasetGenerator() {
		this(1, "data");
	}
	
	public DatasetGenerator(int numFiles) {
		this(numFiles, "data");
	}
	
	public DatasetGenerator(int numFiles, String prefix) {
		this.prefix = prefix;		
		this.numFiles = numFiles;		
		random = new Random(System.currentTimeMillis());
		DecimalFormatSymbols symbols = new DecimalFormatSymbols(Locale.ENGLISH);
		formatter = new DecimalFormat("#.##", symbols);
	}

	public void generateFiles() throws Exception {
		for (int i = 0; i < numFiles; i++) {
			generateFile(i);
		}
	}

	private void generateFile(int i) throws Exception {
		BufferedWriter bw = new BufferedWriter(new FileWriter(prefix+"_" + i + ".csv"));
		for (int k = 0; k < getNumLines(); k++) {
			bw.write(getLine() + "\n");
		}
		bw.flush();
		bw.close();
	}

	public String getRandomDate() {

		return getRandomDate(1960);
	}

	public String getRandomDateDMY(int startingYear) {

		Calendar c = new GregorianCalendar();
		int currentYear = c.get(Calendar.YEAR);

		int year = random.nextInt(currentYear - startingYear) + startingYear;
		int month = random.nextInt(12) + 1;
		int day;
		if (month == 2) {
			if ((year % 4) == 0) {
				day = random.nextInt(29) + 1;
			} else {
				day = random.nextInt(28) + 1;
			}
		} else if (month == 4 || month == 6 || month == 9 || month == 11) {
			day = random.nextInt(30) + 1;
		} else {
			day = random.nextInt(31) + 1;
		}

		return fillZero(day) + "/" + fillZero(month) + "/" + year;
	}

	
	public String getRandomDate(int startingYear) {

		Calendar c = new GregorianCalendar();
		int currentYear = c.get(Calendar.YEAR);

		int year = random.nextInt(currentYear - startingYear) + startingYear;
		int month = random.nextInt(12) + 1;
		int day;
		if (month == 2) {
			if ((year % 4) == 0) {
				day = random.nextInt(29) + 1;
			} else {
				day = random.nextInt(28) + 1;
			}
		} else if (month == 4 || month == 6 || month == 9 || month == 11) {
			day = random.nextInt(30) + 1;
		} else {
			day = random.nextInt(31) + 1;
		}

		return year + "/" + fillZero(month) + "/" + fillZero(day);
	}

	public String getRandomDouble(double max) {
		double rand = random.nextDouble() * max;
		return formatter.format(rand);
	}

	public String getRandomInt(int max) {
		return new BigDecimal(random.nextInt(max)).toPlainString();
	}

	public String getRandomString(String[] options) {
		int index = random.nextInt(options.length);
		return options[index];
	}

	private String fillZero(int value) {
		String str = value + "";
		if (value <= 9) {
			str = "0" + value;
		}
		return str;
	}

	public String getLine() {
		String sLine = "";
		List<String> lLine = generateLine();
		for (int i = 0; i < lLine.size(); i++) {
			sLine += lLine.get(i);
			if (i < (lLine.size() - 1)) {
				sLine += getFieldSeparator();
			}
		}
		return sLine;
	}

	public String getFieldSeparator() {
		return ",";
	}

	public abstract List<String> generateLine();

	public abstract int getNumLines();

}
