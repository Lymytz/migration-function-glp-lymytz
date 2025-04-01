/*todo: insérer une unité par defaut*/
CREATE OR REPLACE FUNCTION migration_from_glp_article_unites(
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
    unites_ RECORD;
    unite_  BIGINT;
    query_  CHARACTER VARYING;
BEGIN
    query_ = 'SELECT id, libelle, reference, type FROM yvs_unite_mesure';
    FOR unites_ IN SELECT *
        FROM dblink('host=' || serveur || ' dbname=' || database || ' user=' || users || ' password=' || password, query_ )
            AS t(id integer, libelle character varying, reference character varying, type character varying)
        LOOP
            SELECT INTO unite_ y.id
            FROM yvs_base_unite_mesure y
            WHERE y.reference = unites_.reference
              AND y.societe = societe_;

            IF COALESCE(unite_, 0) = 0 THEN
                IF simulate THEN
                    RAISE NOTICE 'SIMULATION - INSERT Unité: % (% - type: %)', unites_.reference, unites_.libelle, unites_.type;
                ELSE
                    INSERT INTO yvs_base_unite_mesure(reference, libelle, societe, author, description, type)
                    VALUES (unites_.reference, unites_.libelle, societe_, author, '', 'Q')
                    RETURNING id INTO unite_;
                END IF;
            END IF;
        END LOOP;
END;
$$;
