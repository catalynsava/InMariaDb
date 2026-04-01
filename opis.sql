SELECT cfg_localitati.localitate
	, adrese_roluri.tip
	, adrese_roluri.volum
	, adrese_roluri.pozitie
	, persoane_fizice.nume
	, persoane_fizice.initiala
	, persoane_fizice.prenume
	, persoane_fizice.cnp
FROM adrese_roluri
	INNER JOIN cfg_localitati
	ON adrese_roluri.cod_cfg_localitati = cfg_localitati.cod
	INNER JOIN persoane_fizice 
	ON adrese_roluri.id_persoana = persoane_fizice.id
WHERE adrese_roluri.tip IN (1,2) ORDER BY cfg_localitati.localitate, adrese_roluri.tip, adrese_roluri.volum, adrese_roluri.pozitie;