CREATE OR REPLACE FUNCTION migration_from_glp_article_familles(
    societe_ bigint,
    serveur character varying,
    database character varying,
    users character varying,
    password character varying,
    author bigint,
    simulate boolean DEFAULT true
)
    RETURNS void
    LANGUAGE plpgsql
AS
$$
DECLARE
    familles_ RECORD;
    famille_  BIGINT;
    query_    CHARACTER VARYING;
BEGIN
    query_ = 'SELECT reffamille, categoriefam, designation, remise, sommeil FROM famillearticles';
    FOR familles_ IN SELECT *
        FROM dblink('host=' || serveur || ' dbname=' || database || ' user=' || users || ' password=' || password,query_)
            AS t(reffamille character varying,
                    categoriefam character varying,
                    designation character varying,
                    remise double precision,
                    sommeil boolean)
        LOOP
            SELECT INTO famille_ y.id
            FROM yvs_base_famille_article y
            WHERE y.reference_famille = familles_.reffamille
              AND y.societe = societe_;

            IF COALESCE(famille_, 0) = 0 THEN
                IF simulate THEN
                    RAISE NOTICE 'SIMULATION - INSERT Famille: % (% - sommeil: %)', familles_.reffamille, familles_.designation, familles_.sommeil;
                ELSE
                    INSERT INTO yvs_base_famille_article(reference_famille, designation, societe, author, actif)
                    VALUES (familles_.reffamille, familles_.designation, societe_, author, true)
                    RETURNING id INTO famille_;
                END IF;
            END IF;
        END LOOP;
END;
$$;
