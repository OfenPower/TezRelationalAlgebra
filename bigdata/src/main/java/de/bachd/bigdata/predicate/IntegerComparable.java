package de.bachd.bigdata.predicate;

public class IntegerComparable implements PredicateComparable {

	private Integer value;

	public IntegerComparable(Integer val) {
		this.value = val;
	}

	@Override
	public boolean equal(String val) {
		return value.equals(Integer.parseInt(val));
	}

	@Override
	public boolean notEqual(String val) {
		return !value.equals(Integer.parseInt(val));
	}

	@Override
	public boolean lessThan(String val) {
		return value.compareTo(Integer.parseInt(val)) < 0;
	}

	@Override
	public boolean lessEqualThan(String val) {
		return value.compareTo(Integer.parseInt(val)) <= 0;
	}

	@Override
	public boolean greaterThan(String val) {
		return value.compareTo(Integer.parseInt(val)) > 0;
	}

	@Override
	public boolean greaterEqualThan(String val) {
		return value.compareTo(Integer.parseInt(val)) >= 0;
	}
}
