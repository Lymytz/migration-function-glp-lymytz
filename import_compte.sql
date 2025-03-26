 DROP FUNCTION public.import_compte(int8, int8, varchar, int4, varchar, varchar, varchar);

CREATE OR REPLACE FUNCTION public.import_compte(societe_ bigint, agence_ bigint, host character varying, port integer, database character varying, users character varying, password character varying)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    comptes_ RECORD;

    compte_ BIGINT;
    nature_ BIGINT;

    query_ CHARACTER VARYING;

BEGIN

	-- BEGIN COMPTE
	query_ = 'SELECT numcompte, actif, comptegeneral, intitule, letrable, nature, reportanouveau, saisianal, saisicomptetiers, saisiecheance, type, vente_online FROM comptes ';
	FOR comptes_  IN SELECT * FROM dblink('host='||host||' dbname='||database||' user='||users||' password='||password, query_)
		AS t(numcompte character varying, actif boolean, comptegeneral character varying, intitule character varying, letrable boolean, nature character varying, reportanouveau character varying, saisianal boolean, saisicomptetiers boolean, saisiecheance boolean, type character varying, vente_online boolean)
	LOOP
		RAISE NOTICE 'comptes_ : %',comptes_;
		SELECT INTO nature_ y.id FROM yvs_base_nature_compte y WHERE y.designation = comptes_.nature;
		RAISE NOTICE 'nature_ : %',nature_;
		IF(COALESCE(nature_, 0) = 0)THEN
			INSERT INTO yvs_base_nature_compte
			(designation, type_report, nature, lettrable, saisie_echeance, saisie_compte_tier, saisie_anal, societe, agence, actif) 
			VALUES 
			(comptes_.nature, 'SOLDE', 'AUTRE', comptes_.letrable, comptes_.saisiecheance, comptes_.saisicomptetiers, comptes_.saisianal, societe_, agence_, true);
			nature_ = currval('yvs_nature_compte_id_seq');
		END IF;
		SELECT INTO compte_ y.id FROM yvs_base_plan_comptable y WHERE y.num_compte = comptes_.numcompte;
		RAISE NOTICE 'compte_ : %',compte_;
		IF(COALESCE(compte_, 0) = 0)THEN
			INSERT INTO yvs_base_plan_comptable
			(num_compte, intitule, lettrable, saisie_echeance, saisie_compte_tiers, saisie_analytique, nature_compte, actif) 
			VALUES 
			(comptes_.numcompte, comptes_.intitule, comptes_.letrable, comptes_.saisiecheance, comptes_.saisicomptetiers, comptes_.saisianal, nature_, comptes_.actif);
			compte_ = currval('yvs_compta_plan_de_compte_id_seq');
		END IF;
	END LOOP;
	-- END COMPTE
	return true;
END;
$function$
;
