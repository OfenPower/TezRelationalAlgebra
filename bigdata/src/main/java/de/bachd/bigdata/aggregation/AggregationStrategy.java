package de.bachd.bigdata.aggregation;

public interface AggregationStrategy {

	public void setNeutral();

	public void add(String obj);

	public Object getValue();

	/**
	 * Name der Aggregatfunktion z.B "sum" oder "count". Mit diesem Namen wird
	 * die Aggregatfunktion im SELECT-Statement erkannt.
	 * 
	 * @return Name der Aggregatfunktion
	 */
	public String getAggregateFunctionName();

	/**
	 * Ergebnis-Domain der Aggregatfunktion, z.B "Integer", "Double" oder
	 * "String"
	 * 
	 * @return Rückgabe-Domain der Aggregatfunktion
	 */
	public String getResultDomain();
}
