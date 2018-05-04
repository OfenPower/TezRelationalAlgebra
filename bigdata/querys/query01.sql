SELECT artikel.name, lager.name, liegen.bestand
FROM artikel
JOIN liegen ON artikel.anr = liegen.anr
JOIN lager ON liegen.lnr = lager.lnr
WHERE artikel.name = Hose