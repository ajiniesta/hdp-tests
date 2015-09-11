package com.iniesta.hdp.task1;

public class WeatherData {

	private int year;
	private int month;
	private int day;
	private int prcp;
	private int tmax;
	private int tmin;

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
		result = prime * result + day;
		result = prime * result + month;
		result = prime * result + prcp;
		result = prime * result + tmax;
		result = prime * result + tmin;
		result = prime * result + year;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof WeatherData))
			return false;
		WeatherData other = (WeatherData) obj;
		if (day != other.day)
			return false;
		if (month != other.month)
			return false;
		if (prcp != other.prcp)
			return false;
		if (tmax != other.tmax)
			return false;
		if (tmin != other.tmin)
			return false;
		if (year != other.year)
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "WeatherData [year=" + year + ", month=" + month + ", day=" + day + ", prcp=" + prcp + ", tmax=" + tmax + ", tmin=" + tmin + "]";
	}
	
}
