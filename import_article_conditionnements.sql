CREATE OR REPLACE FUNCTION migration_from_glp_article_conditionnements(
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
    query_           CHARACTER VARYING;
BEGIN
    query_ = 'SELECT refart, conditionnement, puv, pua, remise FROM articles';
    FOR articles_ IN SELECT * FROM dblink('host=' || serveur || ' dbname=' || database || ' user=' || users || ' password=' || password,query_)
                 AS t(refart character varying, conditionnement character varying, puv double precision,
                      pua double precision, remise double precision)
        LOOP
            -- Récupère l’article
            SELECT INTO article_ a.id FROM yvs_base_articles a inner join yvs_base_famille_article f ON f.id = a.famille WHERE ref_art = articles_.refart AND f.societe = societe_;
            IF COALESCE(article_, 0) = 0 THEN
                RAISE NOTICE 'Article % non trouvé. (conditionnement ignoré)', articles_.refart;
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
            -- Vérifie si conditionnement existe déjà
            SELECT INTO conditionnement_ id FROM yvs_base_conditionnement WHERE article = article_ AND unite = unite_;
            IF COALESCE(conditionnement_, 0) = 0 THEN
                IF simulate THEN
                    RAISE NOTICE 'SIMULATION - INSERT Conditionnement pour article % (unité: %, puv: %, pua: %, remise: %)',
                        articles_.refart, articles_.conditionnement, articles_.puv, articles_.pua, articles_.remise;
                ELSE
                    INSERT INTO yvs_base_conditionnement(article, unite, author, prix, prix_min, nature_prix_min,
                                                         remise, cond_vente, prix_achat, photo, code_barre)
                    VALUES (article_, unite_, author, articles_.puv, articles_.puv, 'MONTANT',
                            articles_.remise, true, articles_.pua, NULL, NULL)
                    RETURNING id INTO conditionnement_;
                END IF;
            END IF;
        END LOOP;
END;
$$;
