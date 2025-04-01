-- DROP FUNCTION public.import_compte(int8, int8, varchar, int4, varchar, varchar, varchar);

CREATE OR REPLACE FUNCTION public.migration_from_glp_plan_comptable(
    societe_ bigint,
    agence_ bigint,
    host character varying,
    database character varying,
    users character varying,
    password character varying,
    author bigint
)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    comptes_ RECORD;
    categories_ RECORD;
    taxes_ RECORD;

    compte_ BIGINT;
    compte_general_ BIGINT;
    nature_ BIGINT;
    categorie_ BIGINT;
    taxe_ BIGINT;

    query_ CHARACTER VARYING;
    type_compte_ CHARACTER VARYING;
    type_de_report_ CHARACTER VARYING;

BEGIN
	-- BEGIN COMPTE
	query_ = 'SELECT numcompte, actif, comptegeneral, intitule, letrable, nature, reportanouveau, saisianal, saisicomptetiers, saisiecheance, type, vente_online FROM comptes ';
	FOR comptes_  IN SELECT * FROM dblink('host='||host||' dbname='||database||' user='||users||' password='||password, query_)
		AS t(numcompte character varying, actif boolean, comptegeneral character varying, intitule character varying, letrable boolean,
		    nature character varying, reportanouveau character varying, saisianal boolean, saisicomptetiers boolean,
		    saisiecheance boolean, "type" character varying, vente_online boolean)
	LOOP
		RAISE NOTICE 'comptes_ : %',comptes_;
		SELECT INTO nature_ y.id FROM yvs_base_nature_compte y WHERE y.designation = comptes_.nature;
		RAISE NOTICE 'nature_ : %',nature_;
		IF(comptes_."type"='AUXILLIAIRE') THEN
            type_compte_='DETAIL';
        ELSEIF(comptes_."type"='COLLECTIFS') THEN
		    type_compte_='CO';
        end if;
		--mapping du type de repport
		IF(comptes_.reportanouveau='DETAILS') THEN
            type_de_report_='DETAIL';
        ELSEIF(comptes_.reportanouveau='SOLDE') THEN
            type_de_report_='SOLDE';
        ELSEIF(comptes_.reportanouveau IS NULL OR comptes_.reportanouveau='') THEN
            type_de_report_='AU';
        end if;
		IF(COALESCE(nature_, 0) = 0)THEN
			INSERT INTO yvs_base_nature_compte
			    (designation, type_report, nature, lettrable, saisie_echeance,
			     saisie_compte_tier, saisie_anal, societe, agence, actif,author, date_save, date_update)
			VALUES (comptes_.nature, 'SOLDE', 'AUTRE', comptes_.letrable,
			        comptes_.saisiecheance, comptes_.saisicomptetiers, comptes_.saisianal,
			        societe_, agence_, true, author, current_timestamp, current_timestamp) RETURNING id INTO nature_;
		END IF;
		SELECT INTO compte_ y.id FROM yvs_base_plan_comptable y WHERE y.num_compte = comptes_.numcompte;
		RAISE NOTICE 'compte_ : %',compte_;
		IF(COALESCE(compte_, 0) = 0)THEN
			INSERT INTO yvs_base_plan_comptable
			(num_compte, intitule, lettrable, saisie_echeance, saisie_compte_tiers, saisie_analytique, nature_compte,  type_report,
			 actif, type_compte,author, date_update,date_save, compte_general)
			VALUES 
			(comptes_.numcompte, comptes_.intitule, comptes_.letrable, comptes_.saisiecheance,
			 comptes_.saisicomptetiers, comptes_.saisianal, nature_,type_de_report_, comptes_.actif,type_compte_,
                author, current_timestamp, current_timestamp);
			compte_ = currval('yvs_compta_plan_de_compte_id_seq');
		END IF;
	END LOOP;
	-- END COMPTE
    --Deuxième parcours pour les compte généreaux
    query_ = 'SELECT numcompte, comptegeneral FROM comptes ';
    FOR comptes_  IN SELECT * FROM dblink('host='||host||' dbname='||database||' user='||users||' password='||password, query_)
                                       AS t(numcompte character varying, comptegeneral character varying)
        LOOP
            SELECT INTO compte_ y.id FROM yvs_base_plan_comptable y WHERE y.num_compte = comptes_.numcompte;
            SELECT INTO compte_general_ y.id FROM yvs_base_plan_comptable y WHERE y.num_compte = comptes_.comptegeneral;
            UPDATE yvs_base_plan_comptable SET compte_general=compte_general_ WHERE id=compte_;
        END LOOP;
    --End du deuxième parcours

	-- BEGIN CATEGORIE COMPTABLE
	query_ = 'SELECT categorie, type, vente_online FROM categoriec ';
	FOR categories_  IN SELECT * FROM dblink('host='||host||' dbname='||database||' user='||users||' password='||password, query_)
		AS t(categorie character varying, type character varying, vente_online boolean)
	LOOP
		RAISE NOTICE 'categories_ : %',categories_;
		SELECT INTO categorie_ y.id FROM yvs_base_categorie_comptable y WHERE y.code = categories_.categorie;
		RAISE NOTICE 'categorie_ : %',categorie_;
		IF(COALESCE(categorie_, 0) = 0)THEN
			INSERT INTO yvs_base_categorie_comptable
			(code_appel, code, nature, designation, societe, actif) 
			VALUES 
			(categories_.categorie, categories_.categorie, UPPER(categories_.type), categories_.categorie, societe_, true);
			categorie_ = currval('yvs_catcompta_id_seq');
		END IF;
	END LOOP;
	-- END CATEGORIE COMPTABLE

	-- BEGIN TAXES
	query_ = 'SELECT codetaxe, sens, taux, numcompte FROM taxes ';
	FOR taxes_  IN SELECT * FROM dblink('host='||host||' dbname='||database||' user='||users||' password='||password, query_)
		AS t(codetaxe character varying, sens character varying, taux double precision, numcompte character varying)
	LOOP
		RAISE NOTICE 'taxes_ : %',taxes_;
		--SELECT INTO compte_ y.id FROM yvs_base_nature_compte y WHERE y.num_compte = taxes_.numcompte;
		SELECT INTO taxe_ y.id FROM yvs_base_taxes y WHERE y.code_taxe = taxes_.codetaxe AND y.societe = societe_;
		RAISE NOTICE 'taxe_ : % %', taxe_, compte_;
		IF(COALESCE(taxe_, 0) = 0)THEN
			INSERT INTO yvs_base_taxes
			(code_appel, code_taxe, taux, designation, societe, compte, actif) 
			VALUES 
			(taxes_.codetaxe, taxes_.codetaxe, taxes_.taux, taxes_.codetaxe, societe_, compte_, true);
			taxe_ = currval('yvs_taxes_id_seq');
		END IF;
	END LOOP;
	-- END TAXES
	return true;
END;
$function$
;
