package com.iniesta.hdp.categories;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CategoriesValue implements Writable {

	private double acum;
	private long count;
	private double value;
	
	public CategoriesValue() {
		
	}
	
	public CategoriesValue(Double value) {
		this.value = value.doubleValue();
	}
	public void write(DataOutput out) throws IOException {
		out.writeDouble(acum);
		out.writeLong(count);
		out.writeDouble(value);
	}
	public void readFields(DataInput in) throws IOException {
		acum = in.readDouble();
		count = in.readLong();
		value = in.readDouble();
	}
	public double getAcum() {
		return acum;
	}
	public void setAcum(double acum) {
		this.acum = acum;
	}
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
	public double getValue() {
		return this.value;
	}
	public void setValue(double value){
		this.value = value;
	}
	
	
}
