package com.iniesta.hdp.categories;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.WritableComparable;

public class CategoriesKey implements WritableComparable<CategoriesKey>{

	
	private final static SimpleDateFormat SDF = new SimpleDateFormat("yyyy/MM/dd");
	
	private Calendar calendar;
	private String concept;

	public CategoriesKey(){
		calendar = new GregorianCalendar();
		concept = "";
	}
	
	public void readFields(DataInput din) throws IOException {
		//Calendar
		int year = din.readInt();
		int month = din.readInt();
		int day = din.readInt();
		calendar = new GregorianCalendar();
		calendar.set(Calendar.YEAR, year);
		calendar.set(Calendar.MONTH, month-1);
		calendar.set(Calendar.DAY_OF_MONTH, day);
		fillCalendar();
		//Concept
		concept = din.readUTF();
	}

	private void fillCalendar() {
		calendar.set(Calendar.HOUR, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
	}

	public void write(DataOutput dout) throws IOException {
		//Calendar
		dout.writeInt(calendar.get(Calendar.YEAR));
		dout.writeInt(calendar.get(Calendar.MONTH)+1);
		dout.writeInt(calendar.get(Calendar.DAY_OF_MONTH));
		//concept
		dout.writeUTF(concept);
	}

	public int compareTo(CategoriesKey o) {
		int res = calendar.compareTo(o.calendar);
		if(res==0){
			res = concept.compareTo(o.concept);
		}
		return res;
	}

	public void setDate(String sDate) throws ParseException {
		calendar.setTime(SDF.parse(sDate));
		fillCalendar();
	}

	public void setConcept(String concept) {
		this.concept = concept;
	}

	public Date getDate() {
		return calendar.getTime();
	}
	
	public int getMonth(){
		return calendar.get(Calendar.MONTH);
	}

	public String getConcept() {
		return concept;
	}

	@Override
	public String toString() {
		String date = SDF.format(calendar.getTime());
		return date+","+concept;
	}
	
}
