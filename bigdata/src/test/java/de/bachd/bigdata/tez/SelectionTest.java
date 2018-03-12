package de.bachd.bigdata.tez;

public class SelectionTest {
	public static void main(String[] args) {
		/*
		 * Predicate<Tuple> selection = t -> { System.out.println("Map");
		 * Map<String, String> m = t.getColumnValueStringMap1();
		 * System.out.println("Get Key"); String text = m.get("ArtNr");
		 * System.out.println(text + "equals 1001?"); boolean b =
		 * text.equals("1001"); return b; }; Tuple tuple1 = new Tuple(); Text[]
		 * names = new Text[] { new Text("ArtNr"), new Text("ArtName"), new
		 * Text("Preis") }; Text[] domains = new Text[] { new Text("Integer"),
		 * new Text("String"), new Text("Double") }; Text[] values = new Text[]
		 * { new Text("1001"), new Text("Hose"), new Text("19.99") };
		 * tuple1.setElements(names, domains, values); Tuple tuple2 = new
		 * Tuple(); Text[] names2 = new Text[] { new Text("ArtNr"), new
		 * Text("ArtName"), new Text("Preis") }; Text[] domains2 = new Text[] {
		 * new Text("Integer"), new Text("String"), new Text("Double") }; Text[]
		 * values2 = new Text[] { new Text("1002"), new Text("Hut"), new
		 * Text("39.99") }; tuple2.setElements(names2, domains2, values2); Tuple
		 * tuple3 = new Tuple(); Text[] names3 = new Text[] { new Text("ArtNr"),
		 * new Text("ArtName"), new Text("Preis") }; Text[] domains3 = new
		 * Text[] { new Text("Integer"), new Text("String"), new Text("Double")
		 * }; Text[] values3 = new Text[] { new Text("1003"), new
		 * Text("Schuhe"), new Text("9.99") }; tuple3.setElements(names3,
		 * domains3, values3); List<Tuple> tupleList = new ArrayList<>();
		 * tupleList.add(tuple1); tupleList.add(tuple2); tupleList.add(tuple3);
		 * for (Tuple t : tupleList) { if (selection.test(t)) {
		 * System.out.println("Erfolg"); } else {
		 * System.out.println("Fehlschlag"); }
		 * 
		 * }
		 */
	}
}
