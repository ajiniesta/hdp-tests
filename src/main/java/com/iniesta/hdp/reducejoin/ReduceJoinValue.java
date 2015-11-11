package com.iniesta.hdp.reducejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ReduceJoinValue implements Writable{

	private String name;
	private double value;
	
	@Override
	public void write(DataOutput out) throws IOException {
		if(name==null){
			name = "";
		}
		out.writeUTF(name);
		out.writeDouble(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		value = in.readDouble();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return  name + "," + value;
	}

	
}
