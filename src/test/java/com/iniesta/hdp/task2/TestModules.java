package com.iniesta.hdp.task2;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestModules {
	
	@Test
	public void test(){
		Decades ud = new Decades(1960, 2015);
		assertEquals(6, ud.getNumDecades());
	}
	

	@Test
	public void test2(){
		Decades ud = new Decades(1960, 2017);
		assertEquals(6, ud.getNumDecades());
	}

	@Test
	public void test3(){
		Decades ud = new Decades(1960, 2000);
		assertEquals(5, ud.getNumDecades());
	}
	
	@Test
	public void test4(){
		Decades ud = new Decades(1960, 1970);
		assertEquals(2, ud.getNumDecades());
	}

	@Test
	public void test5(){
		Decades ud = new Decades(1960, 1970);
		assertEquals(0, ud.inWhichDecadeAmI(1960));
		assertEquals(0, ud.inWhichDecadeAmI(1962));
		assertEquals(1, ud.inWhichDecadeAmI(1970));
	}

}
