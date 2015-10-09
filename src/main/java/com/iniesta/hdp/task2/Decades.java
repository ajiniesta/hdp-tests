package com.iniesta.hdp.task2;

public class Decades {

	private int start;
	private int end;

	public Decades(int start, int end){
		this.start = start;
		this.end = end;
		
	}
	
	public int getNumDecades(){
		int diffYears = end - start + 1;
		return (diffYears /10) + (diffYears%10!=0?1:0);
	}
	
	public int inWhichDecadeAmI(int year){
		return (year - start) / 10;
	}
}
