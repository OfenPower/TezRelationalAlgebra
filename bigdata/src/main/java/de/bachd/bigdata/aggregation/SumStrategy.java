package de.bachd.bigdata.aggregation;

public class SumStrategy implements AggregationStrategy {

	private double sum;

	public SumStrategy() {
		sum = 0.0;
	}

	@Override
	public void add(String obj) {
		double d = Double.parseDouble(obj);
		sum += d;
	}

	@Override
	public Object getValue() {
		return sum;
	}

	@Override
	public String getAggregateFunctionName() {
		return "sum";
	}

	@Override
	public String getResultDomain() {
		return "double";
	}

	@Override
	public AggregationStrategy cloneStrategy() {
		return new SumStrategy();
	}

}
