package de.bachd.bigdata.aggregation;

public class CountStrategy implements AggregationStrategy {

	private int count;

	public CountStrategy() {
		// neutrales Aggregat
		setNeutral();
	}

	@Override
	public void setNeutral() {
		count = 0;
	}

	@Override
	public void add(String obj) {
		count++;
	}

	@Override
	public Object getValue() {
		return count;
	}

	@Override
	public String getAggregateFunctionName() {
		return "count";
	}

	@Override
	public String getResultDomain() {
		return "integer";
	}

}
