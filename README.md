# TezRelationalAlgebra
Implementierung der relationalen Algebra in Apache Tez
# --------------------------------------------------------

Start des Programms:
java -jar tez.jar <queryfile.sql>

- Die Querys werden aus dem Ordner "querys" gelesen. (Beispielsweise ./querys/query01.sql) 
- Alle benötigten Tabellen mit Schema befinden sich im "tables"-Ordner
- Ergebnisse landen in den Ordnern "output_scheme" bzw. "output_table"

# --------------------------------------------------------

Beispielquery mit allen Operationen:

SELECT artikel.name, sum(liegen.bestand)
FROM artikel
JOIN liegen ON artikel.anr = liegen.anr
WHERE artikel.anr = 1003 or artikel.name = Hose	
GROUP BY artikel.name

# --------------------------------------------------------

Anmerkungen zur Queryformulierung:
- Alle Projektionsattribute müssen vollqualifiziert sein (relation.attributname!)
- Alle Klauseln sind zeilenweise formuliert
- Strings in der WHERE-Klausel müssen ohne "" formuliert sein (siehe obige Bedingung)
- Alle Joins müssen aufeinander aufbauen
- Die Projektions- und Gruppierungsattribute sollten übereinstimmen

# --------------------------------------------------------

Anmerkungen zum Programm:
- Join: Momentan wird stets der Hash-Join verwendet

- Self-Joins sind aktuell noch nicht möglich, da die Möglichkeit zur Umbenennung von 
  Relationen noch nicht ganz eingebaut ist

- Selektion: Die einzelnen Bedingungen in der WHERE-Klausel dürfen kein "and" oder "or" enthalten, da
	     aktuell noch explizit nach "and" und "or" geparst wird. (ja, das ist sehr unschön, aber erfüllt vorerst den Zweck)

- Aggregation: Unterstützt werden zurzeit die Aggregatfunktionen sum(), count() und avg()
