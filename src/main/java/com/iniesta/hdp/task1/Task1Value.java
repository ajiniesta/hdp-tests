package com.iniesta.hdp.task1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Task1Value implements Writable{
	
	private int year;
	private int month;
	private int day;
	private int depTime;
	private int arrTime;
	private Text uniqueCarrier;
	private int flightNumber;
	private int actualElapsedTime;
	private int arrDelay;
	private int depDelay;
	private Text origin;
	private Text destination;
	private int prcp;
	private int tmax;
	private int tmin;
	
	public Task1Value(){
		uniqueCarrier = new Text();
		origin = new Text();
		destination = new Text();
	}
	
	public void readFields(DataInput in) throws IOException {
		year = in.readInt();
		month = in.readInt();
		day = in.readInt();
		depTime = in.readInt();
		arrTime = in.readInt();
		uniqueCarrier.readFields(in);
		flightNumber = in.readInt();
		actualElapsedTime = in.readInt();
		arrDelay = in.readInt();
		depDelay = in.readInt();
		origin.readFields(in);
		destination.readFields(in);
		prcp = in.readInt();
		tmax = in.readInt();
		tmin = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(depTime);
		out.writeInt(arrTime);
		uniqueCarrier.write(out);
		out.writeInt(flightNumber);
		out.writeInt(actualElapsedTime);
		out.writeInt(arrDelay);
		out.writeInt(depDelay);
		origin.write(out);
		destination.write(out);
		out.writeInt(prcp);
		out.writeInt(tmax);
		out.writeInt(tmin);
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getDepTime() {
		return depTime;
	}

	public void setDepTime(int depTime) {
		this.depTime = depTime;
	}

	public int getArrTime() {
		return arrTime;
	}

	public void setArrTime(int arrTime) {
		this.arrTime = arrTime;
	}

	public Text getUniqueCarrier() {
		return uniqueCarrier;
	}

	public void setUniqueCarrier(Text uniqueCarrier) {
		this.uniqueCarrier = uniqueCarrier;
	}

	public int getFlightNumber() {
		return flightNumber;
	}

	public void setFlightNumber(int flightNumber) {
		this.flightNumber = flightNumber;
	}

	public int getActualElapsedTime() {
		return actualElapsedTime;
	}

	public void setActualElapsedTime(int actualElapsedTime) {
		this.actualElapsedTime = actualElapsedTime;
	}

	public int getArrDelay() {
		return arrDelay;
	}

	public void setArrDelay(int arrDelay) {
		this.arrDelay = arrDelay;
	}

	public int getDepDelay() {
		return depDelay;
	}

	public void setDepDelay(int depDelay) {
		this.depDelay = depDelay;
	}

	public Text getOrigin() {
		return origin;
	}

	public void setOrigin(Text origin) {
		this.origin = origin;
	}

	public Text getDestination() {
		return destination;
	}

	public void setDestination(Text destination) {
		this.destination = destination;
	}

	public int getPrcp() {
		return prcp;
	}

	public void setPrcp(int prcp) {
		this.prcp = prcp;
	}

	public int getTmax() {
		return tmax;
	}

	public void setTmax(int tmax) {
		this.tmax = tmax;
	}

	public int getTmin() {
		return tmin;
	}

	public void setTmin(int tmin) {
		this.tmin = tmin;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + actualElapsedTime;
		result = prime * result + arrDelay;
		result = prime * result + arrTime;
		result = prime * result + day;
		result = prime * result + depDelay;
		result = prime * result + depTime;
		result = prime * result + ((destination == null) ? 0 : destination.hashCode());
		result = prime * result + flightNumber;
		result = prime * result + month;
		result = prime * result + ((origin == null) ? 0 : origin.hashCode());
		result = prime * result + prcp;
		result = prime * result + tmax;
		result = prime * result + tmin;
		result = prime * result + ((uniqueCarrier == null) ? 0 : uniqueCarrier.hashCode());
		result = prime * result + year;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Task1Value))
			return false;
		Task1Value other = (Task1Value) obj;
		if (actualElapsedTime != other.actualElapsedTime)
			return false;
		if (arrDelay != other.arrDelay)
			return false;
		if (arrTime != other.arrTime)
			return false;
		if (day != other.day)
			return false;
		if (depDelay != other.depDelay)
			return false;
		if (depTime != other.depTime)
			return false;
		if (destination == null) {
			if (other.destination != null)
				return false;
		} else if (!destination.equals(other.destination))
			return false;
		if (flightNumber != other.flightNumber)
			return false;
		if (month != other.month)
			return false;
		if (origin == null) {
			if (other.origin != null)
				return false;
		} else if (!origin.equals(other.origin))
			return false;
		if (prcp != other.prcp)
			return false;
		if (tmax != other.tmax)
			return false;
		if (tmin != other.tmin)
			return false;
		if (uniqueCarrier == null) {
			if (other.uniqueCarrier != null)
				return false;
		} else if (!uniqueCarrier.equals(other.uniqueCarrier))
			return false;
		if (year != other.year)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "" + year + "," + month + "," + day + "," + depTime + "," + arrTime + "," + uniqueCarrier + "," + flightNumber + "," + actualElapsedTime + "," + arrDelay + "," + depDelay + "," + origin + "," + destination + "," + prcp + "," + tmax + "," + tmin + "";
	}

	
	
}
