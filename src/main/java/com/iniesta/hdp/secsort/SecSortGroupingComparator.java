package com.iniesta.hdp.secsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecSortGroupingComparator extends WritableComparator{

	protected SecSortGroupingComparator(){
		super(CompositeKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeKey cka = (CompositeKey)a;
		CompositeKey ckb = (CompositeKey)b;
		
		return ckb.getZip() - cka.getZip();
	}
	
	
}
