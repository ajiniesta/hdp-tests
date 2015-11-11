package com.iniesta.hdp.reducejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ReduceJoinKey implements WritableComparable<ReduceJoinKey>{

	private int id;
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
	}

	@Override
	public int compareTo(ReduceJoinKey o) {
		return id - o.id;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ReduceJoinKey other = (ReduceJoinKey) obj;
		if (id != other.id)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return  String.valueOf(id);
	}
	
	

}
