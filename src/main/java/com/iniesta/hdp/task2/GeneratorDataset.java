package com.iniesta.hdp.task2;

import java.io.*;
import java.util.GregorianCalendar;
import java.util.Random;

/**
 * Created by MXGTEAM4 on 10/8/2015.
 */
public class GeneratorDataset {

    private static int numLines = 100000;

    private static Random random = new Random(System.currentTimeMillis());

    public static void main(String[] args) throws Exception{
        if(args.length!=1) {
            System.err.println("You need to specify the number of files");
            System.exit(1);
        }
        int numFiles = Integer.parseInt(args[0]);
        for(int i = 0; i < numFiles ; i++) {
            generateFile(i);
        }
    }

    private static void generateFile(int i) throws Exception {
        BufferedWriter bw = new BufferedWriter(new FileWriter("data_"+i+".csv"));
        for(int k = 0; k < numLines ; k++ ) {
            bw.write(generateLine()+"\n");
        }
    }

    private static String generateLine() {

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
        double value = random.nextDouble();

        return year + "/" + month + "/" + day+","+value;
    }


}
