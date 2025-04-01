CREATE OR REPLACE FUNCTION migration_from_glp_article_depots(
    societe_ bigint,
    agence_ bigint,
    serveur character varying,
    database character varying,
    users character varying,
    password character varying,
    author bigint,
    simulate boolean DEFAULT true)
    RETURNS void
    LANGUAGE plpgsql
AS $$
DECLARE
    depots_ RECORD;
    article_ RECORD;
    _depot_ BIGINT;
    articledepot_ BIGINT;

    query_ CHARACTER VARYING;
BEGIN
    -- Requête principale
    query_ = 'SELECT refart, codedepot, stockmax, stockmin, pua, puv, remise, marge_minimale, conditionnement, quantite_vendu, prix_revient, controle_stock FROM articledepots';

    FOR depots_ IN SELECT * FROM dblink('host=' || serveur || ' dbname=' || database || ' user=' || users || ' password=' || password,query_)
                AS t(
                    refart character varying,
                    codedepot character varying,
                    stockmax double precision,
                    stockmin double precision,
                    pua double precision,
                    puv double precision,
                    remise double precision,
                    marge_minimale double precision,
                    conditionnement integer,
                    quantite_vendu double precision,
                    prix_revient double precision,
                    controle_stock boolean
            )
        LOOP
            -- Article
            SELECT INTO article_ a.id, a.categorie FROM yvs_base_articles a inner join yvs_base_famille_article f ON f.id = a.famille WHERE ref_art = depots_.refart AND f.societe = societe_;
            IF COALESCE(article_.id, 0) = 0 THEN
                RAISE NOTICE 'Article % non trouvé (dépot ignoré)', depots_.refart;
                CONTINUE;
            END IF;

            -- Dépôt
            SELECT INTO _depot_ id FROM yvs_base_depots WHERE code = depots_.codedepot AND agence = agence_;
            IF COALESCE(_depot_, 0) = 0 THEN
                RAISE NOTICE 'Dépôt % non trouvé pour agence %', depots_.codedepot, agence_;
                CONTINUE;
            END IF;

            -- Conditionnement (optionnel)
            --SELECT INTO conditionnement_ id FROM yvs_base_conditionnement  WHERE article = article_ LIMIT 1;

            -- Déjà existant ?
            SELECT INTO articledepot_ id FROM yvs_base_article_depot WHERE article = article_.id AND depot = _depot_;
            IF COALESCE(articledepot_, 0) = 0 THEN
                IF simulate THEN
                    RAISE NOTICE 'SIMULATION - LIER Article % au dépôt % (Stock max: %, suivi: %)',
                        depots_.refart, depots_.codedepot, depots_.stockmax, depots_.controle_stock;
                ELSE
                    INSERT INTO yvs_base_article_depot(
                        article, depot, stock_max, stock_min, mode_appro, mode_reappro, interval_approv,
                        quantite_stock, actif, stock_alert, stock_initial, marg_stock_moyen,
                        stock_net, suivi_stock, default_cond, depot_pr, default_pr, categorie, author
                    ) VALUES (
                                 article_.id, _depot_, depots_.stockmax, depots_.stockmin, NULL, NULL, 0,
                                 0, true, 0, 0, 0, 0, true,
                                 null, _depot_, true, article_.categorie,author
                             )
                    RETURNING id INTO articledepot_;
                END IF;
            END IF;
        END LOOP;
END;
$$;
