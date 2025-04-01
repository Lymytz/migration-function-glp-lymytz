CREATE OR REPLACE FUNCTION migration_from_glp_categories_comptables(
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
    articles_   RECORD;
    categories_ RECORD;
    article_    BIGINT;
    _categorie_ BIGINT;
    _compte_    BIGINT;
    categorie_  BIGINT;
    query_      CHARACTER VARYING;
BEGIN
    -- Récupère tous les articles de la base externe
    query_ = 'SELECT DISTINCT refart FROM articles';
    FOR articles_ IN
        SELECT *
        FROM dblink('host=' || serveur || ' dbname=' || database || ' user=' || users || ' password=' || password,
                    query_)
                 AS t(refart character varying)
        LOOP
            -- Récupère l'article dans la base locale
            SELECT INTO article_ a.id FROM yvs_base_articles a INNER JOIN yvs_base_famille_article f ON f.id=a.famille WHERE ref_art = articles_.refart AND societe = societe_;
            IF COALESCE(article_, 0) = 0 THEN
                RAISE NOTICE 'Article % non trouvé. (catégorie comptable ignorée)', articles_.refart;
                CONTINUE;
            END IF;

            -- Récupère les lignes de catégorie/comptes liées à l'article
            query_ = 'SELECT refart, numcompte, categorie FROM art_catc_compte WHERE refart = ' ||quote_literal(articles_.refart);
            FOR categories_ IN
                SELECT *
                FROM dblink('host=' || serveur || ' dbname=' || database || ' user=' || users || ' password=' ||
                            password, query_)
                         AS t(refart character varying, numcompte character varying, categorie character varying)
                LOOP
                    -- Trouver la catégorie comptable
                    SELECT INTO _categorie_ y.id FROM yvs_base_categorie_comptable y WHERE y.code = categories_.categorie AND y.societe = societe_;

                    -- Trouver le compte général
                    SELECT INTO _compte_ y.id FROM yvs_base_plan_comptable y INNER JOIN yvs_base_nature_compte n ON y.nature_compte = n.id
                    WHERE y.num_compte = categories_.numcompte AND n.societe = societe_;

                    IF COALESCE(_categorie_, 0) != 0 AND COALESCE(_compte_, 0) != 0 THEN
                        SELECT INTO categorie_ id FROM yvs_base_article_categorie_comptable
                        WHERE article = article_
                          AND categorie = _categorie_
                          AND compte = _compte_;

                        IF COALESCE(categorie_, 0) = 0 THEN
                            IF simulate THEN
                                RAISE NOTICE 'SIMULATION - Catégorie comptable [%] / Compte [%] pour article %',
                                    categories_.categorie, categories_.numcompte, articles_.refart;
                            ELSE
                                INSERT INTO yvs_base_article_categorie_comptable(article, categorie, compte, actif, author)
                                VALUES (article_, _categorie_, _compte_, true, author)
                                RETURNING id INTO categorie_;
                            END IF;
                        END IF;
                    ELSE
                        RAISE NOTICE 'Catégorie ou compte introuvable pour article %', articles_.refart;
                    END IF;
                END LOOP;
        END LOOP;
END;
$$;
