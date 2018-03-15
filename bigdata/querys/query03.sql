SELECT professoren.name, vorlesungen.titel
FROM professoren
JOIN vorlesungen ON professoren.persnr = vorlesungen.gelesenvon
WHERE vorlesungen.titel = Mäeutik