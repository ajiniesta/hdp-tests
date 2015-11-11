package com.iniesta.hdp.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ReduceSideJoinGroupingComparator extends WritableComparator{

	public ReduceSideJoinGroupingComparator(){
		super(ReduceJoinKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		ReduceJoinKey ka = (ReduceJoinKey)a;
		ReduceJoinKey kb = (ReduceJoinKey)b;
		return Integer.compare(ka.getId(), kb.getId());
	}
	
	
}

