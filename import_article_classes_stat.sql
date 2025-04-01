CREATE OR REPLACE FUNCTION migration_from_glp_classes_stat(
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
    classes_ RECORD;
    classe_  BIGINT;
    query_   CHARACTER VARYING;
BEGIN
    query_ = 'SELECT id, intitule, visibleensynthese FROM classestat';
    FOR classes_ IN
        SELECT *
        FROM dblink('host=' || serveur || ' dbname=' || database || ' user=' || users || ' password=' || password,query_) AS t(id integer, intitule character varying, visibleensynthese boolean)
        LOOP
            SELECT INTO classe_ y.id FROM yvs_base_classes_stat y WHERE y.code_ref = classes_.intitule AND y.societe = societe_;
            IF COALESCE(classe_, 0) = 0 THEN
                IF simulate THEN
                    RAISE NOTICE 'SIMULATION - INSERT ClasseStat: % (visible: %)', classes_.intitule, classes_.visibleensynthese;
                ELSE
                    INSERT INTO yvs_base_classes_stat(code_ref, designation, societe, author, actif, visible_synthese)
                    VALUES (classes_.intitule, classes_.intitule, societe_, author, true, classes_.visibleensynthese)
                    RETURNING id INTO classe_;
                END IF;
            END IF;
        END LOOP;
END;
$$;
