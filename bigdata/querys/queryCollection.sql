// Alle Artikel mit anr > 1003
SELECT *
FROM artikel
WHERE artikel.anr > 1003

// Alle Attribute der Relation "lager"
SELECT *
FROM lager

// Welche artikel liegen mindestens 7x in welchem lager?
SELECT artikel.name, lager.name, liegen.bestand
FROM artikel
JOIN liegen ON artikel.anr = liegen.anr
JOIN lager ON liegen.lnr = lager.lnr
WHERE liegen.bestand >= 7

// Welcher Professor liest die Vorlesung Mäeutik?
SELECT professoren.name, vorlesungen.titel
FROM professoren
JOIN vorlesungen ON professoren.persnr = vorlesungen.gelesenvon
WHERE vorlesungen.titel = Mäeutik



