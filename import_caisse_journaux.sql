
CREATE OR REPLACE FUNCTION public.migration_from_glp_caisses_journaux(
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
    caisses_ RECORD;
    journaux_ RECORD;

    caisse_ BIGINT;
    journal_ BIGINT;

    _compte_ BIGINT;
    _journal_ BIGINT;

    query_ CHARACTER VARYING;

BEGIN
	-- BEGIN JOURNAL
	query_ = 'SELECT codejournal, actif, centralisationparligne, comptetresorerie, libele, "type" FROM journaux';
	FOR journaux_  IN SELECT * FROM dblink('host='||host||' dbname='||database||' user='||users||' password='||password, query_)
		AS t(codejournal character varying,
		    actif boolean,
		    centralisationparligne boolean,
		    comptetresorerie character varying,
		    libele character varying,
            "type" character varying)
	LOOP
		RAISE NOTICE 'journaux_ : %',journaux_;
		SELECT INTO journal_ y.id FROM yvs_compta_journaux y WHERE y.code_journal = journaux_.codejournal;
		IF(COALESCE(journal_, 0) = 0)THEN
			INSERT INTO yvs_compta_journaux (code_journal, intitule, agence, actif, author)
			VALUES  (journaux_.codejournal, journaux_.libele, agence_, true, author)
			RETURNING id INTO journal_;
		END IF;
		SELECT INTO caisse_ id FROM yvs_base_caisse WHERE code=journaux_.codejournal;
        SELECT INTO _compte_ y.id FROM yvs_base_plan_comptable y WHERE y.num_compte = journaux_.comptetresorerie;
        IF(COALESCE(caisse_, 0) = 0)THEN
            INSERT INTO yvs_base_caisse (intitule, code, compte, journal, actif, author)
            VALUES  (journaux_.libele, journaux_.codejournal, _compte_, journal_, true, author);
        END IF;
	END LOOP;
	-- END CAISSE
	return true;
END;
$function$
;
