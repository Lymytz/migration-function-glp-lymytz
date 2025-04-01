CREATE OR REPLACE FUNCTION migration_from_glp_article_articles(
    societe_ bigint,
    serveur character varying,
    database character varying,
    users character varying,
    password character varying,
    author_ bigint,
    simulate boolean DEFAULT true
)
    RETURNS void
    LANGUAGE plpgsql
AS $$
DECLARE
    familles_ RECORD;
    articles_ RECORD;
    article_ BIGINT;
    famille_ BIGINT;
    classe_ BIGINT;
    classe2_ BIGINT;
    query_ CHARACTER VARYING;
    categorie_ CHARACTER VARYING;
BEGIN
    -- Récupère toutes les familles depuis la base externe
    query_ = 'SELECT reffamille, categoriefam, designation FROM famillearticles';
    FOR familles_ IN
        SELECT * FROM dblink('host=' || serveur || ' dbname=' || database || ' user=' || users || ' password=' || password, query_)
                          AS t(reffamille character varying, categoriefam character varying, designation character varying)
        LOOP
            SELECT INTO famille_ y.id FROM yvs_base_famille_article y WHERE y.reference_famille = familles_.reffamille AND y.societe = societe_;
            IF COALESCE(famille_, 0) = 0 THEN
                RAISE NOTICE 'Famille non trouvée (devrait être importée avant) : %', familles_.reffamille;
                CONTINUE;
            END IF;
            -- Récupère les articles liés à cette famille
            query_ = 'SELECT refart, categorie, changeprix, classe, codebarre, commentaire, conditionnement, designation, modeconso, norme, photos, poidnet, pua, puv, remise, sommeil, ' ||
                     'suivienstock, typepv, unite, unitepoids, reffamille, defnorme, visibleensynthese, datesave, taill_du_lot, new_ref, code_acces, code_sortie, classe2, appliquer_remise, groupe ' ||
                     'FROM articles WHERE reffamille = ' || quote_literal(familles_.reffamille);

            FOR articles_ IN
                SELECT * FROM dblink('host=' || serveur || ' dbname=' || database || ' user=' || users || ' password=' || password, query_)
                                  AS t(
                                       refart character varying,
                                       categorie character varying,
                                       changeprix boolean,
                                       classe character varying,
                                       codebarre character varying,
                                       commentaire text,
                                       conditionnement character varying,
                                       designation character varying,
                                       modeconso character,
                                       norme character varying,
                                       photos character varying,
                                       poidnet double precision,
                                       pua double precision,
                                       puv double precision,
                                       remise double precision,
                                       sommeil boolean,
                                       suivienstock boolean,
                                       typepv character varying,
                                       unite character varying,
                                       unitepoids character varying,
                                       reffamille character varying,
                                       defnorme boolean,
                                       visibleensynthese boolean,
                                       datesave timestamp without time zone,
                                       taill_du_lot double precision,
                                       new_ref character varying,
                                       code_acces bigint,
                                       code_sortie bigint,
                                       classe2 character varying,
                                       appliquer_remise boolean,
                                       groupe integer
                        )
                LOOP
                    SELECT INTO article_ y.id FROM yvs_base_articles y WHERE y.ref_art = articles_.refart AND y.famille = famille_;
                    IF COALESCE(article_, 0) = 0 THEN
                        -- Récupère les classes
                        SELECT INTO classe_ y.id FROM yvs_base_classes_stat y WHERE y.code_ref = articles_.classe AND y.societe = societe_;
                        SELECT INTO classe2_ y.id FROM yvs_base_classes_stat y WHERE y.code_ref = articles_.classe2 AND y.societe = societe_;

                        IF simulate THEN
                            RAISE NOTICE 'SIMULATION - INSERT Article: % (%), classe1: %, classe2: %', articles_.refart, articles_.designation, articles_.classe, articles_.classe2;
                        ELSE
                            if(articles_.categorie='PRODUIT SEMI FINI') then
                                categorie_='PSF';
                            elseif(articles_.categorie='MATIERE PREMIERE') then
                                categorie_='MP';
                            elseif(articles_.categorie='PRODUIT FINI')then
                                categorie_='PF';
                            elseif(articles_.categorie='REVENTE')then
                                categorie_='MARCHANDISE';
                            else
                                categorie_=articles_.categorie;
                            end if;
                            INSERT INTO yvs_base_articles(
                                change_prix, description, def_norme, designation, mode_conso, norme, photo_1, masse_net, prix_min,
                                pua, puv, ref_art, remise, suivi_en_stock, visible_en_synthese, groupe, class_stat, coefficient,
                                service, methode_val, actif, fabriquant, photo_2, photo_3, categorie, famille, duree_vie,
                                duree_garantie, fichier, template, unite_de_masse, unite_volume, lot_fabrication, author,
                                nature_prix_min, puv_ttc, pua_ttc, unite_stockage, unite_vente, date_update, date_save,
                                classe1, classe2, type_service
                            ) VALUES (
                                         articles_.changeprix,
                                         COALESCE(articles_.commentaire, ''),
                                         articles_.defnorme,
                                         articles_.designation,
                                         articles_.modeconso,
                                         articles_.defnorme,
                                         articles_.photos,
                                         articles_.poidnet,
                                         articles_.puv,
                                         articles_.pua,
                                         articles_.puv,
                                         articles_.refart,
                                         articles_.remise,
                                         articles_.suivienstock,
                                         articles_.visibleensynthese,
                                         articles_.groupe,
                                         articles_.classe,
                                         0,
                                         NULL,
                                         'CMPI',
                                         true,
                                         NULL,
                                         NULL,
                                         NULL,
                                         categorie_,
                                         famille_,
                                         1,
                                         1,
                                         NULL,
                                         NULL,
                                         NULL,
                                         NULL,
                                         NULL,
                                         author_,
                                         'MONTANT',
                                         true,
                                         true,
                                         NULL,
                                         NULL,
                                         current_timestamp,
                                         COALESCE(articles_.datesave, current_date),
                                         classe_,
                                         classe2_,
                                         'C'
                                     )
                            RETURNING id INTO article_;
                        END IF;
                    END IF;
                END LOOP;
        END LOOP;
END;
$$;
