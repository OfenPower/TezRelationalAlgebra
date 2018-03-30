SELECT artikel.anr, artikel.name, count(liegen.lnr), sum(liegen.bestand)
FROM artikel 
JOIN liegen ON artikel.anr = liegen.anr
WHERE artikel.anr != 1003
GROUP BY artikel.anr, artikel.name