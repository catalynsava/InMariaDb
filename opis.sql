SELECT 
    ar.id,
    cl.localitate,
    ar.tip,
    ar.volum,
    ar.pozitie,
    CASE 
    	  WHEN ar.tip IN (1,2) THEN CONCAT_WS(' ', pf.nume, pf.initiala, pf.prenume)
        WHEN ar.tip IN (3,4) THEN CONCAT_WS(' ', pj.denumire, fo.abreviere)   
    END AS titular
FROM adrese_roluri ar
INNER JOIN cfg_localitati cl 
    ON ar.cod_cfg_localitati = cl.cod
LEFT JOIN persoane_fizice pf 
    ON ar.id_persoana = pf.id AND ar.tip IN (1,2)
LEFT JOIN persoane_juridice pj 
    ON ar.id_persoana = pj.id AND ar.tip IN (3,4)
LEFT JOIN cfg_forme_organizare AS fo
	 ON pj.cod_forma_organizare = fo.cod
ORDER BY 
    cl.localitate,
    ar.tip,
    ar.volum,
    ar.pozitie;