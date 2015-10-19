package com.iniesta.hdp.inmap;

public class Word implements Comparable<Word>{

	private String word;
	private long frequency;
	
	public Word(String word) {
		super();
		this.word = word;
		this.frequency = 1L;
	}
	
	public long getFrequency() {
		return frequency;
	}
	public void incrementFrequency() {
		this.frequency += 1L;
	}

	public String getWord() {
		return word;
	}	

	public int compareTo(Word o) {
		return (int) (o.frequency - frequency);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Word other = (Word) obj;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}
	
}
