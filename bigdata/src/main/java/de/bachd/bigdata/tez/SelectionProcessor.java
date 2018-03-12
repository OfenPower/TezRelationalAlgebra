package de.bachd.bigdata.tez;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

public class SelectionProcessor extends SimpleProcessor {

	private Predicate<Tuple> selectionPredicate;

	public SelectionProcessor(ProcessorContext context) {
		super(context);
	}

	@Override
	public void run() throws Exception {
		Preconditions.checkState(getInputs().size() == 1);
		Preconditions.checkState(getOutputs().size() == 1);

		KeyValueReader kvReader = (KeyValueReader) getInputs().values().iterator().next().getReader();
		KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();
		while (kvReader.next()) {
			Tuple tuple = (Tuple) kvReader.getCurrentKey();
			NullWritable nullValue = NullWritable.get();
			// Tuple nach Prädikat selektieren und an Projektionsprozessor
			// weiterreichen
			if (selectionPredicate.test(tuple)) {
				// DEBUG selektierte Tupel anzeigen
				tuple.printColumnValues();
				kvWriter.write(tuple, nullValue);
			}
		}
	}

	@Override
	public void initialize() throws Exception {
		System.out.println("Initialize SelectionProcessor");

		// UserPayload in Configuration konvertieren und selectionPredicate
		// bauen
		UserPayload up = getContext().getUserPayload();
		Configuration conf = TezUtils.createConfFromUserPayload(up);
		String predicate = conf.get("SelectionPredicate");
		// Wenn kein Selektionsprädikat angegeben wurde => Bedingung auf true
		// setzen!
		if (predicate.equals("true")) {
			this.selectionPredicate = t -> true;
			System.out.println("No Selection required!");
		} else {
			buildSelectionPredicateFromString(predicate);
		}

	}

	private void buildSelectionPredicateFromString(String predicate) {
		Iterable<String> predIterable = Splitter.on(CharMatcher.anyOf(" and ").and(CharMatcher.anyOf(" or ")))
				.trimResults().omitEmptyStrings().split(predicate);
		// Reihenfolge der Bool-Operatoren speichern und Parameter der Form
		// (AttributName Relation Datentyp) in predicateParameters speichern
		Deque<String> predicateParameters = new ArrayDeque<>();
		Deque<String> boolOperators = new ArrayDeque<>();
		for (String s : predIterable) {
			if (s.equals("and") || s.equals("or")) {
				boolOperators.addLast(s);
			} else {
				predicateParameters.addLast(s);
			}
		}
		// DEBUG: BoolOperators ausgeben
		/*
		 * for (String s : boolOperators) { System.out.println(s); }
		 */

		// Über predicateParameter iterieren und einzelne Predicates bauen
		Deque<Predicate<Tuple>> predQueue = new ArrayDeque<>();
		Iterator<String> predItr = predicateParameters.iterator();
		while (predItr.hasNext()) {
			String attributeName = predItr.next();
			String relation = predItr.next();
			String value = predItr.next();
			Predicate<Tuple> p = buildPredicate(attributeName, relation, value);
			predQueue.addLast(p);
			// DEBUG
			// System.out.println(attributeName + " " + relation + " " + value);
		}
		// DEBUG einzelne Seleektionsprädikate anzeigen
		System.out.println("Selektion nach: " + predicate);

		// Pädikate zu einem Gesamtprädikat (selectionPredicate) mit booleschen
		// Operatoren zusammenbauen
		this.selectionPredicate = predQueue.pollFirst();
		for (Predicate<Tuple> pred : predQueue) {
			// Anzahl boolOps identisch mit Anzahl Predicates in predQueue
			String boolOp = boolOperators.pollFirst();
			if (boolOp.equals("and")) {
				this.selectionPredicate = this.selectionPredicate.and(pred);
			} else if (boolOp.equals("or")) {
				this.selectionPredicate = this.selectionPredicate.or(pred);
			}
		}
	}

	/**
	 * Hier wird vermutlich noch eine Funktion "PredicateComparable.value()"
	 * benötigt, welche einfach den Wert des PredicateComparables liefert. Damit
	 * können join-Bedingungungen ausgewertet werden
	 */
	// Prädikat abhängig der Operation "relation" zusammenbauen und zurückgeben
	private Predicate<Tuple> buildPredicate(String attributeName, String relation, String value) {
		Predicate<Tuple> p = null;
		switch (relation) {
		case "=": {
			p = t -> t.getColumnComparableMap().get(attributeName).equal(value);
			break;
		}
		case "!=": {
			p = t -> t.getColumnComparableMap().get(attributeName).notEqual(value);
			break;
		}
		case "<": {
			p = t -> t.getColumnComparableMap().get(attributeName).lessThan(value);
			break;
		}
		case "<=": {
			p = t -> t.getColumnComparableMap().get(attributeName).lessEqualThan(value);
			break;
		}
		case ">": {
			p = t -> t.getColumnComparableMap().get(attributeName).greaterThan(value);
			break;
		}
		case ">=": {
			p = t -> t.getColumnComparableMap().get(attributeName).greaterEqualThan(value);
			break;
		}
		}
		return p;
	}

}
