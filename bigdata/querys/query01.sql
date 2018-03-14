SELECT artikel.ArtNr, artikel.ArtName, count(liegen.LagerNr), sum(liegen.Bestand)
FROM artikel 
JOIN liegen ON artikel.ArtNr = liegen.ArtNr
WHERE artikel.ArtNr != 1003
GROUP BY artikel.ArtNr, artikel.ArtName