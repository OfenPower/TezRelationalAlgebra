package de.bachd.bigdata.tez;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

public class MergeJoinProcessor extends SimpleProcessor {

	public MergeJoinProcessor(ProcessorContext context) {
		super(context);
	}

	@Override
	public void run() throws Exception {
		// KeyValueReader und Writer initialisieren
		Iterator<LogicalInput> kvReaderItr = getInputs().values().iterator();
		KeyValuesReader kvsReader1 = null;
		KeyValuesReader kvsReader2 = null;
		while (kvReaderItr.hasNext()) {
			kvsReader1 = (KeyValuesReader) kvReaderItr.next().getReader();
			kvsReader2 = (KeyValuesReader) kvReaderItr.next().getReader();
		}
		KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();

		// Solange beide Relationen noch Tupel beinhalten...
		while (kvsReader1.next() && kvsReader2.next()) {
			// Werte der Joinattribute holen
			Text leftValue = (Text) kvsReader1.getCurrentKey();
			Text rightValue = (Text) kvsReader2.getCurrentKey();
			boolean reachedEnd = false;

			// Solange die Werte unterschiedlich sind, werden die beiden Zeiger
			// unterschiedlich weiter bewegt
			while (leftValue.compareTo(rightValue) != 0) {
				// Wenn value1 < value2 => nächsten Wert aus kevsReader1 holen!
				if (leftValue.compareTo(rightValue) < 0) {
					if (kvsReader1.next()) {
						leftValue = (Text) kvsReader1.getCurrentKey();
					} else {
						reachedEnd = true;
						break;
					}
				} else {
					// Wenn value1 > value2 => nächsten Wert aus kevsReader2
					// holen!
					if (kvsReader2.next()) {
						rightValue = (Text) kvsReader2.getCurrentKey();
					} else {
						reachedEnd = true;
						break;
					}
				}
			}

			// Wenn eine der Eingaben erschöpft ist, wird aus der Schleife
			// ausgebrochen, da kein Joinpartner mehr vorhanden ist!
			if (reachedEnd) {
				break;
			}

			// Join mit allen gruppierten Tupeln durchführen, da bei allen
			// Tupeln das Joinattribut gleich ist!
			for (Object obj1 : kvsReader1.getCurrentValues()) {
				Tuple leftTuple = (Tuple) obj1;
				for (Object obj2 : kvsReader2.getCurrentValues()) {
					Tuple rightTuple = (Tuple) obj2;
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
					kvWriter.write(NullWritable.get(), joinedTuple);
				}
			}
		}
	}

	@Override
	public void initialize() {
		System.out.println("Initialize MergeJoinProcessor");
	}

}