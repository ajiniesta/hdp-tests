package com.iniesta.hdp.task1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Task1Key implements WritableComparable<Task1Key>{

	private int year;
	private int month;
	private int day;
	private int arrDelay;
	
	public void readFields(DataInput in) throws IOException {
		year = in.readInt();
		month = in.readInt();
		day = in.readInt();
		arrDelay = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(arrDelay);
	}

	public int compareTo(Task1Key o) {
		long thisDate = Long.parseLong(year+""+addZero(month)+""+addZero(day));
		long otherDate = Long.parseLong(o.year+""+addZero(o.month)+""+addZero(o.day));
		return (int) (thisDate - otherDate);
	}

	private String addZero(int data) {
		String out = "";
		if(data<10){
			out = "0";
		}
		out += data;
		return out;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public void setArrDelay(int arrDelay) {
		this.arrDelay = arrDelay;
	}
	
	public int getArrDelay(){
		return this.arrDelay;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + arrDelay;
		result = prime * result + day;
		result = prime * result + month;
		result = prime * result + year;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Task1Key))
			return false;
		Task1Key other = (Task1Key) obj;
		if (arrDelay != other.arrDelay)
			return false;
		if (day != other.day)
			return false;
		if (month != other.month)
			return false;
		if (year != other.year)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return  + year + "," + month + "," + day + "," + arrDelay;
	}	

}
