package com.iniesta.hdp;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.util.List;
import java.util.Random;

public abstract class DatasetGenerator {

	private int numFiles;
	private Random random;

	public DatasetGenerator(String[] args) {
		if(args.length!=1) {
            numFiles = 1;
        } else{
        	numFiles = Integer.parseInt(args[0]);
        }
        random = new Random(System.currentTimeMillis());
	}

	public void generateFiles() throws Exception {
		for (int i = 0; i < numFiles; i++) {
			generateFile(i);			
		}
	}
	
	public void generateFile(int i) throws Exception {
        BufferedWriter bw = new BufferedWriter(new FileWriter("data_"+i+".csv"));
        for(int k = 0; k < getNumLines() ; k++ ) {
            bw.write(generateLine()+"\n");
        }
        bw.flush();
        bw.close();
    }

	public String getRandomDate() {

        int year = random.nextInt(55)+1960;
        int month = random.nextInt(12)+1;
        int day;
        if(month==2){
            if((year % 4)==0){
                day = random.nextInt(29) + 1;
            }else {
                day = random.nextInt(28) + 1;
            }
        } else if(month==4 || month == 6 || month==9 || month==11){
            day = random.nextInt(30)+1;
        } else {
            day = random.nextInt(31)+1;
        }

        return year + "/" + fillZero(month) + "/" + fillZero(day);
    }

	public String getRandomDouble(double max){
		double rand = random.nextDouble()*max;
		BigDecimal bd = new BigDecimal(rand);
		return bd.toString();
	}
	
	public String getRandomInt(int max){
		return new BigDecimal(random.nextInt(max)).toPlainString();
	}
	
	public String getRandomString(String[] options){
		int index = random.nextInt(options.length);
		return options[index];
	}
	
	private String fillZero(int value) {
		String str = value + "";
		if(value <= 9){
			str = "0" + value;
		}
		return str;
	}

	public String getLine(){
		String sLine = "";
		List<String> lLine = generateLine();
		for (int i = 0; i < lLine.size(); i++) {
			sLine += lLine.get(i);
			if(i<(lLine.size()-1)){
				sLine += getFieldSeparator();
			}
		}
		return sLine;
	}
	
	private String getFieldSeparator() {
		return ",";
	}

	public abstract List<String> generateLine();

	public abstract int getNumLines();
	
}
