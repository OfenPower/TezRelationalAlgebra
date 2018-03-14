package de.bachd.bigdata.tez;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * Der HashJoinProcessor implementiert einen Equi-Join der folgenden Form:
 * 
 * "R join S on R.A = S.A
 */

public class HashJoinProcessor extends SimpleProcessor {

	private String leftSideJoinAttribute;
	private String rightSideJoinAttribute;

	public HashJoinProcessor(ProcessorContext context) {
		super(context);
	}

	@Override
	public void run() throws Exception {
		// KeyValueReader und Writer initialisieren
		KeyValueReader kvReader1 = (KeyValueReader) getInputs().get("ArtikelTableProcessor").getReader();
		KeyValueReader kvReader2 = (KeyValueReader) getInputs().get("LiegenTableProcessor").getReader();
		KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();
		// Multimap zum Hashjoin verwenden! In einer Multimap können mehrere
		// Values mit gleichen Key gemappt werden. Die Values werden dann pro
		// Key in einer Liste gespeichert
		ListMultimap<String, Tuple> joinMultimap = ArrayListMultimap.create();

		// Alle Tupel der linken Join-Relation in die Hashmap laden
		while (kvReader1.next()) {
			Tuple tuple = (Tuple) kvReader1.getCurrentKey();
			String value = tuple.getNamesValuesMap().get(this.leftSideJoinAttribute);

			// Tupleobjekt kopieren und in Multimap laden
			// key = joinattribut, value = kopiertes Tuple, welches in Liste
			// kopiert wird
			Tuple copiedTuple = Tuple.copyTuple(tuple);
			joinMultimap.put(value, copiedTuple);
		}

		System.out.println("---------------------------------------");

		// Die Multimap ist nach der ersten while-Schleife mit Listen von Tupeln
		// der linken Relation aufgebaut. Alle Tupel des zweiten Readers werden
		// nun mit den Tupeln des ersten Readers gejoint, falls das jeweilige
		// Joinattribut in der HashMap vorkommt.
		while (kvReader2.next()) {
			Tuple rightTuple = (Tuple) kvReader2.getCurrentKey();
			// Multimap besteht aus Listen von leftSide-Tupeln =>
			// rightSideJoinAttribute zum Zugriff auf Attribute benutzen
			String joinAttribute = rightTuple.getNamesValuesMap().get(this.rightSideJoinAttribute);
			// Kommt joinAttribute in HashMap vor?
			if (joinMultimap.containsKey(joinAttribute)) {
				// => rightTuple mit allen gemappten leftTuple joinen!
				List<Tuple> leftTuples = joinMultimap.get(joinAttribute);
				// Join ausführen!
				// ---------------------------------------------------------
				for (Tuple leftTuple : leftTuples) {
					List<String> joinedNames = new ArrayList<>(leftTuple.getAttributeNames());
					joinedNames.addAll(rightTuple.getAttributeNames());
					List<String> joinedDomains = new ArrayList<>(leftTuple.getAttributeDomains());
					joinedDomains.addAll(rightTuple.getAttributeDomains());
					List<String> joinedValues = new ArrayList<>(leftTuple.getAttributeValues());
					joinedValues.addAll(rightTuple.getAttributeValues());
					Tuple joinedTuple = new Tuple();
					joinedTuple.set(joinedNames, joinedDomains, joinedValues);
					// ---------------------------------------------------------
					// DEBUG
					joinedTuple.printColumnValues();
					NullWritable nullValue = NullWritable.get();
					kvWriter.write(joinedTuple, nullValue);
				}
			}
		}
	}

	@Override
	public void initialize() throws IOException {
		System.out.println("Initialize HashJoinProcessor");
		UserPayload up = getContext().getUserPayload();
		Configuration conf = TezUtils.createConfFromUserPayload(up);
		String joinAttributes = conf.get("JoinAttribute");
		// Join-String splitten und in linkes und rechtes Joinattribut der Form
		// "Relation.Attributname" abspeichern
		Iterable<String> attrItr2 = Splitter.on('=').trimResults().omitEmptyStrings().split(joinAttributes);
		Iterator<String> attrIterator2 = attrItr2.iterator();
		while (attrIterator2.hasNext()) {
			this.leftSideJoinAttribute = attrIterator2.next();
			this.rightSideJoinAttribute = attrIterator2.next();
			// DEBUG: Joinattribute anzeigen!
			// System.out.println(this.leftSideJoinAttribute);
			// System.out.println(this.rightSideJoinAttribute);
		}
		// DEBUG
		System.out.println("Join on: " + joinAttributes);
	}
}