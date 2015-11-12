package com.iniesta.hdp.task22;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DateWritable implements WritableComparable<DateWritable>{

	private String date;
		
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(date);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		date = in.readUTF();
	}

	@Override
	public int compareTo(DateWritable o) {
		return date.compareTo(o.date);
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	@Override
	public String toString() {
		return date;
	}	
	
}
