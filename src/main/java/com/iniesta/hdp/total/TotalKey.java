package com.iniesta.hdp.total;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;

public class TotalKey implements WritableComparable<TotalKey>{

	private int id;
	private Date date;
	private int zip;
	
	private final static SimpleDateFormat SDF = new SimpleDateFormat("yyyy/MM/dd"); 
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(SDF.format(date));
		out.writeInt(zip);
	}

	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		try {
			date = SDF.parse(in.readUTF());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		zip = in.readInt();
	}

	public int compareTo(TotalKey o) {
		
		return id - o.id;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(String date) {
		try {
			this.date = SDF.parse(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int getZip() {
		return zip;
	}

	public void setZip(int zip) {
		this.zip = zip;
	}

	@Override
	public String toString() {
		return id + "," + SDF.format(date) + "," + zip;
	}
	
	

}
