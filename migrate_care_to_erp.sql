-- DROP FUNCTION public.migrate_care_to_erp(varchar, int4, varchar, varchar, varchar);

CREATE OR REPLACE FUNCTION public.migrate_care_to_erp(host character varying, port integer, database character varying, users character varying, password character varying)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    societes_ RECORD;
    agences_ RECORD;

    societe_ BIGINT;
    agence_ BIGINT;

    query_ CHARACTER VARYING;

BEGIN
	-- BEGIN SOCIETE
    query_ = 'SELECT adress_siege, capital, code_postal, devise, email, forme_juridique, designation, code_abreviation, siege FROM societes ';
	FOR societes_  IN SELECT * FROM dblink('host='||host||' dbname='||database||' user='||users||' password='||password, query_)
		AS t(adress_siege character varying, capital double precision, code_postal character varying, devise character varying, email character varying
			, forme_juridique character varying, designation character varying, code_abreviation character varying, siege character varying)
	LOOP
		RAISE NOTICE 'societes_ : %',societes_;
		SELECT INTO societe_ y.id FROM yvs_societes y WHERE y.code_abreviation = societes_.code_abreviation AND y."name" = societes_.designation;
		RAISE NOTICE 'societe_ : %',societe_;
		IF(COALESCE(societe_, 0) = 0)THEN
			INSERT INTO yvs_societes
			(adress_siege, capital, code_abreviation, "name", devise, email, forme_juridique, actif) 
			VALUES 
			(societes_.adress_siege, societes_.capital, societes_.code_abreviation, societes_.designation, societes_.devise, societes_.email, societes_.forme_juridique, true);
			societe_ = currval('yvs_societes_id_seq');
		END IF;
	END LOOP;
	-- END SOCIETE	

	-- BEGIN AGENCE
    query_ = 'SELECT codeagence, intitule, abbreviation, actif FROM agences ';
	FOR agences_  IN SELECT * FROM dblink('host='||host||' dbname='||database||' user='||users||' password='||password, query_)
		AS t(codeagence character varying, intitule character varying, abbreviation character varying, actif boolean)
	LOOP
		RAISE NOTICE 'agences_ : %',agences_;
		SELECT INTO agence_ y.id FROM yvs_agences y WHERE y.codeagence = agences_.codeagence AND y.societe = societe_;
		RAISE NOTICE 'agence_ : %',agence_;
		IF(COALESCE(agence_, 0) = 0)THEN
			INSERT INTO yvs_agences
			(abbreviation, codeagence, designation, societe,  actif) 
			VALUES 
			(agences_.abbreviation, agences_.codeagence, agences_.intitule, societe_, agences_.actif);
			agence_ = currval('yvs_agences_id_seq');
		END IF;
	END LOOP;
	-- END AGENCE

	-- BEGIN UTILISATEUR
	PERFORM public.import_user(societe_, agence_, "host", port, "database", users, "password");
	-- END UTILISATEUR

	-- BEGIN DEPOT
	PERFORM public.import_depot(societe_, agence_, "host", port, "database", users, "password");
	-- END DEPOT

	-- BEGIN COMPTE
	PERFORM public.import_compte(societe_, agence_, "host", port, "database", users, "password");
	-- END COMPTE

	-- BEGIN ARTICLE
	PERFORM public.import_article(societe_, agence_, "host", port, "database", users, "password");
	-- END ARTIDLE

	-- BEGIN RISTOURNE
	PERFORM public.import_ristourne(societe_, agence_, "host", port, "database", users, "password");
	-- END RISTOURNE
	return true;
END;
$function$
;
