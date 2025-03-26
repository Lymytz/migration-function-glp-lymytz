-- DROP FUNCTION public.import_article(int8, int8, varchar, int4, varchar, varchar, varchar);

CREATE OR REPLACE FUNCTION public.import_article(societe_ bigint, agence_ bigint, serveur character varying, port integer, database character varying, users character varying, password character varying)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    articles_ RECORD;
    familles_ RECORD;
    classes_ RECORD;
    conditionnements_ RECORD;
    unites_ RECORD;

    article_ BIGINT;
    famille_ BIGINT;
    classe_ BIGINT;
    classe2_ BIGINT;
    conditionnement_ BIGINT;
    unite_ BIGINT;

    query_ CHARACTER VARYING;

BEGIN
	-- BEGIN CLASSE STAT
    query_ = 'SELECT id, intitule, visibleensynthese FROM classestat ';
	FOR classes_  IN SELECT * FROM dblink('host='||serveur||' dbname='||database||' user='||users||' password='||password, query_)
		AS t(id integer, intitule character varying, visibleensynthese boolean)
	LOOP
		SELECT INTO classe_ y.id FROM yvs_base_classes_stat y WHERE y.code_ref = classes_.intitule AND y.societe = societe_;
		IF(COALESCE(classe_, 0) = 0)THEN
			INSERT INTO yvs_base_classes_stat(code_ref, designation, societe, author, actif, visible_synthese) VALUES (classes_.intitule, classes_.intitule, societe_, 16, true, classes_.visibleensynthese);
			classe_ = currval('yvs_base_classes_stat_id_seq');
		END IF;
	END LOOP;
	-- END CLASSE STAT

	-- BEGIN UNITE MESURE
    query_ = 'SELECT id, libelle, reference, type FROM yvs_unite_mesure ';
	FOR unites_  IN SELECT * FROM dblink('host='||serveur||' dbname='||database||' user='||users||' password='||password, query_)
		AS t(id integer, libelle character varying, reference character varying, type character varying)
	LOOP
		SELECT INTO unite_ y.id FROM yvs_base_unite_mesure y WHERE y.reference = unites_.reference AND y.societe = societe_;
		IF(COALESCE(unite_, 0) = 0)THEN
			INSERT INTO yvs_base_unite_mesure(reference, libelle, societe, author, description, type) VALUES (unites_.reference, unites_.libelle, societe_, 16, '', unites_.type);
			unite_ = currval('yvs_prod_unite_masse_id_seq');
		END IF;
	END LOOP;
	-- END UNITE MESURE

	-- BEGIN ARTICLE
    query_ = 'SELECT reffamille, categoriefam, designation, remise, sommeil FROM famillearticles ';
	FOR familles_  IN SELECT * FROM dblink('host='||serveur||' dbname='||database||' user='||users||' password='||password, query_)
		AS t(reffamille character varying, categoriefam character varying, designation character varying, remise double precision, sommeil boolean)
	LOOP
		SELECT INTO famille_ y.id FROM yvs_base_famille_article y WHERE y.reference_famille = familles_.reffamille AND y.societe = societe_;
		IF(COALESCE(famille_, 0) = 0)THEN
			INSERT INTO yvs_base_famille_article(reference_famille, designation, societe, author, actif) VALUES (familles_.reffamille, familles_.designation, societe_, 16, true);
			famille_ = currval('yvs_prod_famille_article_id_seq');
		END IF;
    	query_ = 'SELECT refart, categorie, changeprix, classe, codebarre, commentaire, conditionnement, designation, modeconso, norme, photos, poidnet, pua, puv, remise, sommeil, 
					suivienstock, typepv, unite, unitepoids, reffamille, defnorme, visibleensynthese, datesave, taill_du_lot, new_ref, code_acces, code_sortie, 
					classe2, appliquer_remise, groupe FROM articles WHERE reffamille = '||QUOTE_LITERAL(familles_.reffamille);
		FOR articles_  IN SELECT * FROM dblink('host='||serveur||' dbname='||database||' user='||users||' password='||password, query_)
			AS t(refart character varying, categorie character varying, changeprix boolean, classe character varying, codebarre character varying, commentaire text, 
				conditionnement character varying, designation character varying, modeconso character, norme character varying, photos character varying, 
				poidnet double precision, pua double precision, puv double precision, remise double precision, sommeil boolean, suivienstock boolean, 
				typepv character varying, unite character varying, unitepoids character varying, reffamille character varying, defnorme boolean, 
				visibleensynthese boolean, datesave timestamp without time zone, taill_du_lot double precision, new_ref character varying, code_acces bigint, 
				code_sortie bigint, classe2 character varying, appliquer_remise boolean, groupe integer)
		LOOP
			SELECT INTO article_ y.id FROM yvs_base_articles y WHERE y.ref_art = articles_.refart AND y.famille = famille_;
			IF(COALESCE(article_, 0) = 0)THEN
				SELECT INTO classe_ y.id FROM yvs_base_classes_stat y WHERE y.code_ref = articles_.classe AND y.societe = societe_;
				SELECT INTO classe2_ y.id FROM yvs_base_classes_stat y WHERE y.code_ref = articles_.classe2 AND y.societe = societe_;
			
				INSERT INTO yvs_base_articles(change_prix, description, def_norme, designation, mode_conso, norme, photo_1, masse_net, prix_min, pua, puv, ref_art, remise, 
					    suivi_en_stock, visible_en_synthese, groupe, class_stat, coefficient, service, methode_val, actif, fabriquant, photo_2, photo_3, categorie, 
					    famille, duree_vie, duree_garantie, fichier, template, unite_de_masse, unite_volume, lot_fabrication, author, nature_prix_min, puv_ttc, 
					    pua_ttc, unite_stockage, unite_vente, date_update, date_save, classe1, classe2, type_service)
				    VALUES (articles_.changeprix, COALESCE(articles_.commentaire, ''), articles_.defnorme, articles_.designation, articles_.modeconso, articles_.defnorme, 
						articles_.photos, articles_.poidnet, articles_.puv, articles_.pua, articles_.puv, articles_.refart, articles_.remise,
					    articles_.suivienstock, articles_.visibleensynthese, null, articles_.classe, 0, null, 'CMPI', true, null, null, null, articles_.categorie, 
					    famille_, 1, 1, null, null, null, null, null, 16, 'MONTANT', true,
					    true, null, null, current_date, COALESCE(articles_.datesave, current_date), classe_, classe2_, 'C');
				article_ = currval('yvs_articles_id_seq');
			END IF;
			query_ = 'SELECT c.id, c.conditionnement, c.by_achat, c.by_prod, c.by_vente, c.pua, c.puv, c.remise, c.article, c.unite, u.reference FROM conditionnement c INNER JOIN yvs_unite_mesure u ON c.unite = u.id WHERE article = '||QUOTE_LITERAL(articles_.refart);
			FOR conditionnements_  IN SELECT * FROM dblink('host='||serveur||' dbname='||database||' user='||users||' password='||password, query_)
				AS t(id bigint, conditionnement character varying, by_achat boolean, by_prod boolean, by_vente boolean, pua double precision, puv double precision, remise double precision, article character varying, unite integer, reference character varying)
			LOOP
				SELECT INTO unite_ id FROM yvs_base_unite_mesure y WHERE y.reference = conditionnements_.reference AND y.societe = societe_;
				SELECT INTO conditionnement_ y.id FROM yvs_base_conditionnement y WHERE y.article = article_ AND y.unite = unite_;
				IF(COALESCE(conditionnement_, 0) = 0)THEN
					INSERT INTO yvs_base_conditionnement(article, unite, author, prix, prix_min, nature_prix_min, remise, cond_vente, prix_achat, photo, code_barre)
						VALUES (article_, unite_, 16, conditionnements_.puv, conditionnements_.puv, 'MONTANT', conditionnements_.remise, conditionnements_.by_vente, conditionnements_.pua, null, null);
					conditionnement_ = currval('yvs_base_conditionnement_id_seq');
				END IF;
			END LOOP;
		END LOOP;
	END LOOP;
	-- END ARTICLE
	return true;
END;
$function$
;
