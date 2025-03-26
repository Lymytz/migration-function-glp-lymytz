-- DROP FUNCTION public.import_depot(int8, int8, varchar, int4, varchar, varchar, varchar);

CREATE OR REPLACE FUNCTION public.import_depot(societe_ bigint, agence_ bigint, host character varying, port integer, database character varying, users character varying, password character varying)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    depots_ RECORD;

    depot_ BIGINT;

    query_ CHARACTER VARYING;

BEGIN
	-- BEGIN DEPOT
    query_ = 'SELECT codedepot, adresse, nomresponsable, tel, type, actif, agence, mode_validation_stock, periodicite_inventaire, visible_synthese, vente_online FROM depots ';
	FOR depots_  IN SELECT * FROM dblink('host='||host||' dbname='||database||' user='||users||' password='||password, query_)
		AS t(codedepot character varying, adresse character varying, nomresponsable character varying, tel character varying, type character varying, actif boolean, agence character varying, mode_validation_stock character, periodicite_inventaire integer, visible_synthese boolean, vente_online boolean)
	LOOP
		RAISE NOTICE 'depots_ : %',depots_;
		SELECT INTO depot_ y.id FROM yvs_base_depots y WHERE y.code = depots_.codedepot;
		RAISE NOTICE 'depot_ : %',depot_;
		IF(COALESCE(depot_, 0) = 0)THEN
			INSERT INTO yvs_base_depots
			(abbreviation, code, designation, agence, actif) 
			VALUES 
			(depots_.codedepot, depots_.codedepot, depots_.codedepot, agence_, depots_.actif);
			depot_ = currval('yvs_base_depots_id_seq');
		END IF;
	END LOOP;
	-- END DEPOT 
	return true;
END;
$function$
;
