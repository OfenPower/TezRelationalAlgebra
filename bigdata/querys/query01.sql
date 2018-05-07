SELECT artikel.name, sum(liegen.bestand)
FROM artikel
JOIN liegen ON artikel.anr = liegen.anr
WHERE artikel.anr = 1003 or artikel.name = Hose
GROUP BY artikel.name
