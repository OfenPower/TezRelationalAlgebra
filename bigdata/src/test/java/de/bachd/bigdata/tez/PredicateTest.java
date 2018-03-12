package de.bachd.bigdata.tez;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class PredicateTest {

	public static Predicate<String> predicate;

	public static void setPredicate(Predicate<String> p) {
		predicate = p;
	}

	public static void testPredicate(List<String> list) {
		for (String s : list) {
			if (predicate.test(s)) {
				System.out.println("erfolgreich");
			} else {
				System.out.println("fehlgeschlagen");
			}
		}
	}

	public static void main(String[] args) throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException {

		// Method Invocation um Predicate zu setzen
		Method method1 = PredicateTest.class.getDeclaredMethod("setPredicate", Predicate.class);
		method1.invoke(null, new Predicate<String>() {
			@Override
			public boolean test(String t) {
				if (t.length() > 3) {
					return true;
				} else {
					return false;
				}
			}
		});

		List<String> list = new ArrayList<>();
		String testString1 = "hi";
		String testString2 = "hallo";
		String testString3 = "buh";
		list.add(testString1);
		list.add(testString2);
		list.add(testString3);

		// Method Invocation zum Testen des Predicates
		Method method2 = PredicateTest.class.getDeclaredMethod("testPredicate", List.class);
		// method2.invoke(null, list);

		Predicate<String> p1 = s -> s.length() <= 2;
		Predicate<String> p2 = p1.or(s -> s.length() == 5);
		Predicate<String> p3 = p2.negate();
		for (String s : list) {
			if (p3.test(s)) {
				System.out.println(s);
			}
		}

	}

}
