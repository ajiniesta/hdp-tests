package com.iniesta.hdp.secsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey>{

	private int id;
	private int zip;
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(zip);		
	}

	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		zip = in.readInt();
	}

	public int compareTo(CompositeKey o) {
		int res = this.id - o.id;
		if(res == 0){
			res = o.zip - this.zip;
		}
		return res;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getZip() {
		return zip;
	}

	public void setZip(int zip) {
		this.zip = zip;
	}


	public String toString(){
		return id + "," + zip;
	}
}
