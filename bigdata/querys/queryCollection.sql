SELECT artikel.anr, artikel.preis
FROM artikel
WHERE artikel.anr > 100

SELECT liegen.anr, liegen.lnr
FROM liegen
WHERE liegen.bestand >= 5

SELECT *
FROM lager