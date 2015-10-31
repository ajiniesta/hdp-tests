package com.iniesta.hdp.total;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TotalValue implements Writable{

	private double valueA;
	private double valueB;
	
	public void write(DataOutput out) throws IOException {
		out.writeDouble(valueA);
		out.writeDouble(valueB);		
	}

	public void readFields(DataInput in) throws IOException {
		valueA = in.readDouble();
		valueB = in.readDouble();
	}

	public double getValueA() {
		return valueA;
	}

	public void setValueA(double valueA) {
		this.valueA = valueA;
	}

	public double getValueB() {
		return valueB;
	}

	public void setValueB(double valueB) {
		this.valueB = valueB;
	}

	@Override
	public String toString() {
		return valueA + "," + valueB;
	}

	
}
