package com.iniesta.hdp.task2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.WritableComparable;

public class DateWritable implements WritableComparable<DateWritable>{

	private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
	
	private Calendar cal;
	
	public void fillDate(String rawDate) throws ParseException{
		Date parsed = sdf.parse(rawDate);
		cal = new GregorianCalendar();
		cal.setTime(parsed);
	}
	
	public int getYearOfTheDate(){
		return cal.get(Calendar.YEAR);
	}
	
	public void readFields(DataInput di) throws IOException {
		cal = new GregorianCalendar();
		cal.set(Calendar.YEAR, di.readInt());
		cal.set(Calendar.MONTH, di.readInt());
		cal.set(Calendar.DAY_OF_MONTH, di.readInt());		
	}

	public void write(DataOutput dOut) throws IOException {
		dOut.writeInt(cal.get(Calendar.YEAR));
		dOut.writeInt(cal.get(Calendar.MONTH));
		dOut.writeInt(cal.get(Calendar.DAY_OF_MONTH));		
	}

	public int compareTo(DateWritable o) {
		return cal.compareTo(o.cal);
	}

	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cal == null) ? 0 : cal.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof DateWritable))
			return false;
		DateWritable other = (DateWritable) obj;
		if (cal == null) {
			if (other.cal != null)
				return false;
		} else if (!toString().equals(other.toString()))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return cal.get(Calendar.YEAR) + "/" + addZero(cal.get(Calendar.MONTH)+1) + "/" + addZero(cal.get(Calendar.DAY_OF_MONTH));
	}
	
	private String addZero(int data) {
		String out = "";
		if(data<10){
			out = "0";
		}
		out += data;
		return out;
	}

}
