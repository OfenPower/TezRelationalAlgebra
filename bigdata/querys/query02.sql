SELECT v1.Vorgänger
FROM voraussetzen v1
JOIN voraussetzen v2 ON v1.Nachfolger = v2.Vorgänger
WHERE v2.Nachfolger = 5216
