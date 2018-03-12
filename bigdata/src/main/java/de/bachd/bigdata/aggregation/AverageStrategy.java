package de.bachd.bigdata.aggregation;

public class AverageStrategy implements AggregationStrategy {

	private double sum;
	private double count;

	public AverageStrategy() {
		// neutrales Aggregat
		setNeutral();
	}

	@Override
	public void setNeutral() {
		sum = 0.0;
		count = 0.0;
	}

	@Override
	public void add(String obj) {
		sum += Double.parseDouble(obj);
		count++;
	}

	@Override
	public Object getValue() {
		return sum / count;
	}

	@Override
	public String getAggregateFunctionName() {
		return "avg";
	}

	@Override
	public String getResultDomain() {
		return "Double";
	}

}
