package de.bachd.bigdata.predicate;

public class DoubleComparable implements PredicateComparable {

	private Double value;

	public DoubleComparable(Double val) {
		this.value = val;
	}

	@Override
	public boolean equal(String val) {
		return value.equals(Double.parseDouble(val));
	}

	@Override
	public boolean notEqual(String val) {
		return !value.equals(Double.parseDouble(val));
	}

	@Override
	public boolean lessThan(String val) {
		return value.compareTo(Double.parseDouble(val)) < 0;
	}

	@Override
	public boolean lessEqualThan(String val) {
		return value.compareTo(Double.parseDouble(val)) <= 0;
	}

	@Override
	public boolean greaterThan(String val) {
		return value.compareTo(Double.parseDouble(val)) > 0;
	}

	@Override
	public boolean greaterEqualThan(String val) {
		return value.compareTo(Double.parseDouble(val)) >= 0;
	}
}
