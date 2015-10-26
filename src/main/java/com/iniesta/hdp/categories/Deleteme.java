package com.iniesta.hdp.categories;

import java.util.HashSet;
import java.util.Set;

public class Deleteme {

	public static void main(String[] args) {
		Set<Short> set = new HashSet<Short>();
		for (short i = 0; i < 10; i++) {
			set.add(i);
			set.remove((short)(i - 1));
		}
		System.out.println(set.size());
	}
	
}
