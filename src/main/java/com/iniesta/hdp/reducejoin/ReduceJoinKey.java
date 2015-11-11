package com.iniesta.hdp.reducejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ReduceJoinKey implements WritableComparable<ReduceJoinKey>{

	private int id;
	private int discriminator;
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(discriminator);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		discriminator = in.readInt();
	}

	@Override
	public int compareTo(ReduceJoinKey o) {
		int res = id - o.id;
		if(res == 0) {
			res = discriminator - o.discriminator;
		}
		return res;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getDiscriminator() {
		return discriminator;
	}

	public void setDiscriminator(int discriminator) {
		this.discriminator = discriminator;
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
		return  id + "," + discriminator ;
	}
	
	

}
