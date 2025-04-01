CREATE OR REPLACE FUNCTION migration_from_glp_article_codebarres(
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
    articles_        RECORD;
    article_         BIGINT;
    unite_           BIGINT;
    conditionnement_ BIGINT;
    code_            BIGINT;
    query_           CHARACTER VARYING;
BEGIN
    query_ = 'SELECT refart, conditionnement, codebarre FROM articles WHERE codebarre IS NOT NULL';
    FOR articles_ IN SELECT * FROM dblink('host=' || serveur || ' dbname=' || database || ' user=' || users || ' password=' || password, query_)
                 AS t(refart character varying, conditionnement character varying, codebarre character varying)
        LOOP
            -- Récupère l’article
            SELECT INTO article_ a.id FROM yvs_base_articles a inner join yvs_base_famille_article f ON f.id = a.famille WHERE ref_art = articles_.refart AND f.societe = societe_;
            IF COALESCE(article_, 0) = 0 THEN
                RAISE NOTICE 'Article % non trouvé (code-barre ignoré)', articles_.refart;
                CONTINUE;
            END IF;

            -- Récupère l’unité
            SELECT INTO unite_ id FROM yvs_base_unite_mesure  WHERE reference = articles_.conditionnement AND societe = societe_;
            IF COALESCE(unite_, 0) = 0 THEN
                --Récupère l'unté par dafaut
                SELECT INTO unite_ id FROM yvs_base_unite_mesure u WHERE u.defaut IS TRUE AND societe = societe_ limit 1;
                IF COALESCE(unite_, 0) = 0 THEN
                    RAISE NOTICE 'Unité % non trouvée pour article %', articles_.conditionnement, articles_.refart;
                    CONTINUE;
                END IF;
            END IF;

            -- Récupère le conditionnement
            SELECT INTO conditionnement_ id FROM yvs_base_conditionnement WHERE article = article_ AND unite = unite_;
            IF COALESCE(conditionnement_, 0) = 0 THEN
                RAISE NOTICE 'Conditionnement introuvable pour article %', articles_.refart;
                CONTINUE;
            END IF;

            -- Vérifie si le code-barre existe déjà
            SELECT INTO code_ id FROM yvs_base_article_code_barre WHERE code_barre = articles_.codebarre AND conditionnement = conditionnement_;
            IF COALESCE(code_, 0) = 0 THEN
                IF simulate THEN
                    RAISE NOTICE 'SIMULATION - INSERT Code-barre [%] pour article % (cond: %)', articles_.codebarre, articles_.refart, articles_.conditionnement;
                ELSE
                    INSERT INTO yvs_base_article_code_barre(code_barre, conditionnement, date_save, author)
                    VALUES (articles_.codebarre, conditionnement_, current_timestamp, author)
                    RETURNING id INTO code_;
                END IF;
            END IF;
        END LOOP;
END;
$$;
