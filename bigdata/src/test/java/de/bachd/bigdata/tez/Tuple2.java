package de.bachd.bigdata.tez;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import de.bachd.bigdata.predicate.DoubleComparable;
import de.bachd.bigdata.predicate.IntegerComparable;
import de.bachd.bigdata.predicate.PredicateComparable;
import de.bachd.bigdata.predicate.StringComparable;
import de.bachd.bigdata.tez.Tuple;

/**
 * ToDo: Eventuell Attribut mitführen, mit welchen Relationen das Tupel gejoined
 * wurde!
 */

public class Tuple2 implements WritableComparable<Tuple> {

	// private List<String> testList;
	private ArrayWritable relationNames1;
	private ArrayWritable attributeNames1;
	private ArrayWritable attributeDomains1;
	private ArrayWritable attributeValues1;
	private List<String> relationNames;
	private List<String> attributeNames;
	private List<String> attributeDomains;
	private List<String> attributeValues;

	public Tuple2() {
		// testList = new ArrayList<>();
		// testList.add("buh");
		// testList.add("ja");
		this.relationNames1 = new ArrayWritable(Text.class);
		this.attributeNames1 = new ArrayWritable(Text.class);
		this.attributeDomains1 = new ArrayWritable(Text.class);
		this.attributeValues1 = new ArrayWritable(Text.class);
		this.relationNames = new ArrayList<>();
		this.attributeNames = new ArrayList<>();
		this.attributeDomains = new ArrayList<>();
		this.attributeValues = new ArrayList<>();
	}

	public static Tuple2 copyTuple(Tuple2 tuple) {
		List<String> relations = tuple.getRelationNames1();
		List<String> names = tuple.getAttributeNames1();
		List<String> domains = tuple.getAttributeDomains1();
		List<String> values = tuple.getAttributeValues1();
		Text[] textRelations = new Text[relations.size()];
		Text[] textNames = new Text[names.size()];
		Text[] textDomains = new Text[domains.size()];
		Text[] textValues = new Text[values.size()];
		for (int i = 0; i < relations.size(); i++) {
			textRelations[i] = new Text(relations.get(i));
		}
		for (int i = 0; i < names.size(); i++) {
			textNames[i] = new Text(names.get(i));
		}
		for (int i = 0; i < domains.size(); i++) {
			textDomains[i] = new Text(domains.get(i));
		}
		for (int i = 0; i < values.size(); i++) {
			textValues[i] = new Text(values.get(i));
		}

		Tuple2 t = new Tuple2();
		t.setRelationNames(textRelations);
		t.setElements(textNames, textDomains, textValues);

		return t;
	}

	public void set(List<String> relations, List<String> names, List<String> domains, List<String> values) {
		this.relationNames = relations;
		this.attributeNames = names;
		this.attributeDomains = domains;
		this.attributeValues = values;
	}

	public List<String> getRelationNames() {
		return this.relationNames;
	}

	public List<String> getAttributeNames() {
		return this.attributeNames;
	}

	public List<String> getAttributeDomains() {
		return this.attributeDomains;
	}

	public List<String> getAttributeValues() {
		return this.attributeValues;
	}

	public Map<String, String> getColumnStringMap() {
		List<String> columns = this.attributeNames;
		List<String> values = this.attributeValues;
		Map<String, String> map = new HashMap<>();
		for (int i = 0; i < columns.size(); i++) {
			map.put(columns.get(i), values.get(i));
		}
		return map;
	}

	public Map<String, PredicateComparable> getColumnComparableMap() {
		List<String> columns = this.attributeNames;
		List<String> domains = this.attributeDomains;
		List<String> values = this.attributeValues;
		Map<String, PredicateComparable> map = new HashMap<>();
		for (int i = 0; i < columns.size(); i++) {
			String value = values.get(i);
			if (domains.get(i).equals("Integer")) {
				IntegerComparable ic = new IntegerComparable(Integer.parseInt(value));
				map.put(columns.get(i), ic);
			} else if (domains.get(i).equals("String")) {
				StringComparable sc = new StringComparable(value);
				map.put(columns.get(i), sc);
			} else if (domains.get(i).equals("Double")) {
				DoubleComparable dc = new DoubleComparable(Double.parseDouble(value));
				map.put(columns.get(i), dc);
			}
		}
		return map;
	}

	public void setRelationNames(Text[] names) {
		relationNames1.set(names);
	}

	public void setElements(Text[] attributeNames, Text[] attributeDomains, Text[] attributeValues) {
		this.attributeNames1.set(attributeNames);
		this.attributeDomains1.set(attributeDomains);
		this.attributeValues1.set(attributeValues);
	}

	public List<String> getRelationNames1() {
		Writable[] w = this.relationNames1.get();
		ArrayList<String> l = new ArrayList<>();
		for (int i = 0; i < w.length; i++) {
			String s = ((Text) w[i]).toString();
			l.add(s);
		}
		return l;
	}

	public List<String> getAttributeNames1() {
		// Text-Elemente einzeln casten, da von Writable[] nicht auf
		// List<Text> gecastet werden kann
		Writable[] w = this.attributeNames1.get();
		ArrayList<String> l = new ArrayList<>();
		for (int i = 0; i < w.length; i++) {
			String s = ((Text) w[i]).toString();
			l.add(s);
		}
		return l;
	}

	public List<String> getAttributeDomains1() {
		// Text-Elemente einzeln casten, da von Writable[] nicht auf
		// List<Text> gecastet werden kann
		Writable[] w = this.attributeDomains1.get();
		ArrayList<String> l = new ArrayList<>();
		for (int i = 0; i < w.length; i++) {
			String s = ((Text) w[i]).toString();
			l.add(s);
		}
		return l;
	}

	public List<String> getAttributeValues1() {
		// Text-Elemente einzeln casten, da von Writable[] nicht auf
		// List<Text> gecastet werden kann
		Writable[] w = this.attributeValues1.get();
		ArrayList<String> l = new ArrayList<>();
		for (int i = 0; i < w.length; i++) {
			String s = ((Text) w[i]).toString();
			l.add(s);
		}
		return l;
	}

	public Map<String, String> getColumnValueStringMap1() {
		List<String> columns = getAttributeNames1();
		List<String> values = getAttributeValues1();
		Map<String, String> map = new HashMap<>();
		for (int i = 0; i < columns.size(); i++) {
			map.put(columns.get(i), values.get(i));
		}
		return map;
	}

	public Map<String, PredicateComparable> getColumnValueCompMap1() {
		List<String> columns = getAttributeNames1();
		List<String> domains = getAttributeDomains1();
		List<String> values = getAttributeValues1();
		Map<String, PredicateComparable> map = new HashMap<>();
		for (int i = 0; i < columns.size(); i++) {
			String value = values.get(i);
			if (domains.get(i).equals("Integer")) {
				IntegerComparable ic = new IntegerComparable(Integer.parseInt(value));
				map.put(columns.get(i), ic);
			} else if (domains.get(i).equals("String")) {
				StringComparable sc = new StringComparable(value);
				map.put(columns.get(i), sc);
			} else if (domains.get(i).equals("Double")) {
				DoubleComparable dc = new DoubleComparable(Double.parseDouble(value));
				map.put(columns.get(i), dc);
			}
		}
		return map;
	}

	/*
	 * public List<String> getTestList() { return this.testList; }
	 */

	@Override
	public void readFields(DataInput in) throws IOException {

		this.relationNames1.readFields(in);
		this.attributeNames1.readFields(in);
		this.attributeDomains1.readFields(in);
		this.attributeValues1.readFields(in);
		// Listen deserialisieren
		// ----------------------
		// RelationNames
		int relationSize = in.readInt();
		this.relationNames = new ArrayList<>();
		for (int i = 0; i < relationSize; i++) {
			String s = in.readUTF();
			this.relationNames.add(s);
		}
		// AttributeNames
		int nameSize = in.readInt();
		this.attributeNames = new ArrayList<>();
		for (int i = 0; i < nameSize; i++) {
			String s = in.readUTF();
			this.attributeNames.add(s);
		}
		// AttributeDomains
		int domainSize = in.readInt();
		this.attributeDomains = new ArrayList<>();
		for (int i = 0; i < domainSize; i++) {
			String s = in.readUTF();
			this.attributeDomains.add(s);
		}
		// AttributeValues
		int valueSize = in.readInt();
		this.attributeValues = new ArrayList<>();
		for (int i = 0; i < valueSize; i++) {
			String s = in.readUTF();
			this.attributeValues.add(s);
		}

	}

	@Override
	public void write(DataOutput out) throws IOException {

		this.relationNames1.write(out);
		this.attributeNames1.write(out);
		this.attributeDomains1.write(out);
		this.attributeValues1.write(out);
		// Listen serialisieren
		// --------------------
		// RelationNames
		out.writeInt(this.relationNames.size());
		for (int i = 0; i < this.relationNames.size(); i++) {
			out.writeUTF(this.relationNames.get(i));
		}
		// AttributeNames
		out.writeInt(this.attributeNames.size());
		for (int i = 0; i < this.attributeNames.size(); i++) {
			out.writeUTF(this.attributeNames.get(i));
		}
		// AttributeDomains
		out.writeInt(this.attributeDomains.size());
		for (int i = 0; i < this.attributeDomains.size(); i++) {
			out.writeUTF(this.attributeDomains.get(i));
		}
		// AttributeValues
		out.writeInt(this.attributeValues.size());
		for (int i = 0; i < this.attributeValues.size(); i++) {
			out.writeUTF(this.attributeValues.get(i));
		}
	}

	@Override
	public int compareTo(Tuple obj) {
		if (this.hashCode() < obj.hashCode()) {
			return -1;
		} else {
			return 1;
		}
	}

	@Override
	public int hashCode() {
		return this.attributeValues.hashCode();
	}

	/*
	 * Text[] attributeNames = new Text[attributeNameList.size()]; Text[]
	 * attributeDomains = new Text[attributeDomainList.size()]; Text[]
	 * attributeValues = new Text[attributeValueList.size()]; attributeNames =
	 * attributeNameList.toArray(attributeNames); attributeDomains =
	 * attributeDomainList.toArray(attributeDomains); attributeValues =
	 * attributeValueList.toArray(attributeValues);
	 */
}
