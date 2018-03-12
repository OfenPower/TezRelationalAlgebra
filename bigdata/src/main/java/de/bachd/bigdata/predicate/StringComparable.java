package de.bachd.bigdata.predicate;

public class StringComparable implements PredicateComparable {

	private String value;

	public StringComparable(String val) {
		this.value = val;
	}

	@Override
	public boolean equal(String val) {
		return value.equals(val);
	}

	@Override
	public boolean notEqual(String val) {
		return !value.equals(val);
	}

	/**
	 * Alle anderen Vergleichsarten ungültig für Strings, da keine
	 * lexikographische Vergleiche bei Selektion nötig sind => false!
	 */
	@Override
	public boolean lessThan(String val) {
		return false;
	}

	@Override
	public boolean lessEqualThan(String val) {
		return false;
	}

	@Override
	public boolean greaterThan(String val) {
		return false;
	}

	@Override
	public boolean greaterEqualThan(String val) {
		return false;
	}
}
