SELECT professoren, *
FROM professoren
JOIN vorlesungen ON professoren.persnr = vorlesungen.gelesenvon
WHERE vorlesungen.titel = Mäeutik