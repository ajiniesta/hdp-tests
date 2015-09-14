package com.iniesta.hdp.task1;

public class WeatherData {

	private int prcp;
	private int tmax;
	private int tmin;

	
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
		result = prime * result + prcp;
		result = prime * result + tmax;
		result = prime * result + tmin;
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
		if (prcp != other.prcp)
			return false;
		if (tmax != other.tmax)
			return false;
		if (tmin != other.tmin)
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "WeatherData [prcp=" + prcp + ", tmax=" + tmax + ", tmin=" + tmin + "]";
	}
		
}
