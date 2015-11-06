package com.iniesta.hdp.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinKey implements WritableComparable<JoinKey>{

	private int id;
	private String name;
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(name);		
	}

	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		name = in.readUTF();
	}

	public int compareTo(JoinKey o) {
		return id - o.id;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		JoinKey other = (JoinKey) obj;
		if (id != other.id)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return id + "," + name ;
	}
	
}
