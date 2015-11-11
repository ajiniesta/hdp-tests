package com.iniesta.hdp.reducejoin;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class ReducerReduceSideJoin extends Reducer<ReduceJoinKey, ReduceJoinValue, ReduceJoinKey, ReduceJoinValue> {
	
	public final static String LEFT = "LEFT", RIGHT = "RIGHT", INNER = "INNER", OUTER = "OUTER";
	private String joinType;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		joinType =context.getConfiguration().get("iniesta.join.type", INNER); 
	}

	@Override
	protected void reduce(ReduceJoinKey key, Iterable<ReduceJoinValue> values, Context ctx)
			throws IOException, InterruptedException {
		double acum = 0;
		String name = "";
		boolean hasLeft = false, hasRight = false;
		for (ReduceJoinValue value : values) {
			if(value.isLeft()){
				name = value.getName();
				hasLeft = true;
			}
			else if(value.isRight()){
				acum += value.getValue();
				hasRight = true;
			}
		}
		if(matchesTheJoin(hasLeft, hasRight)){
			ReduceJoinValue value = new ReduceJoinValue();
			value.setName(name);
			value.setValue(acum);		
			ctx.write(key, value);
		}
	}

	private boolean matchesTheJoin(boolean hasLeft, boolean hasRight) {
		if(isInnerJoin() && hasLeft && hasRight){
			return true;
		}
		if(isLeftJoin() && hasLeft && !hasRight) {
			return true;
		}
		if(isRightJoin() && !hasLeft && hasRight) {
			return true;
		}
		if (isOuterJoin() && !hasLeft && !hasRight) {
			return true;
		}
		return false;
	}
	
	public boolean isInnerJoin() {
		return INNER.equalsIgnoreCase(joinType);
	}

	public boolean isLeftJoin() {
		return LEFT.equalsIgnoreCase(joinType);
	}
	
	public boolean isRightJoin() {
		return RIGHT.equalsIgnoreCase(joinType);
	}
	
	public boolean isOuterJoin() {
		return OUTER.equalsIgnoreCase(joinType);
	}
	
}
