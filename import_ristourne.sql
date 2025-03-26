-- DROP FUNCTION public.import_ristourne(int8, int8, varchar, int4, varchar, varchar, varchar);

CREATE OR REPLACE FUNCTION public.import_ristourne(societe_ bigint, agence_ bigint, serveur character varying, port integer, database character varying, users character varying, password character varying)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    plans_ RECORD;
    ristournes_ RECORD;
    article_ RECORD;
    
    plan_ BIGINT DEFAULT 0;
    ristourne_ BIGINT DEFAULT 0;
    grille_ BIGINT DEFAULT 0;

    permanent_ BOOLEAN DEFAULT FALSE;

    base_ CHARACTER VARYING DEFAULT 'QTE';
    nature_montant_ CHARACTER VARYING DEFAULT 'TAUX';

    max_ DOUBLE PRECISION DEFAULT 10000000;
    montant_ DOUBLE PRECISION DEFAULT 0;

    query_ CHARACTER VARYING;

BEGIN
    query_ = 'SELECT id, base, calcul, debut, domaine, fin, intitule, modeapplication, objectif, code_acces, vente_online FROM planderistourne ';
	FOR plans_  IN SELECT * FROM dblink('host='||serveur||' dbname='||database||' user='||users||' password='||password,'select * FROM planderistourne ')
		AS t(id integer, base character varying, calcul character varying, debut date, domaine character varying, fin date, intitule character varying, modeapplication character varying, objectif character varying, code_acces bigint, vente_online boolean)
	LOOP
		IF(UPPER(COALESCE(plans_.modeapplication, '')) = 'PERMANENCE')THEN
			permanent_ = TRUE;
		ELSE
			permanent_ = FALSE;
		END IF;
		IF(UPPER(COALESCE(plans_.base, '')) != '')THEN
			base_ = UPPER(COALESCE(plans_.base, ''));
		ELSE
			base_ = 'QTE';
		END IF;
		SELECT INTO plan_ id FROM yvs_com_plan_ristourne WHERE reference = plans_.intitule;
		IF(COALESCE(plan_, 0) = 0)THEN
			INSERT INTO yvs_com_plan_ristourne(actif, reference, societe, author) VALUES (true, plans_.intitule, societe_, 16);
			plan_ = currval('yvs_com_plan_ristourne_id_seq');
		END IF;
    	query_ = 'SELECT id, borne, istranche, taux, tranche, valeur, idristourne, refart FROM articlesristourne where idristourne = '||plans_.id;
		FOR ristournes_  IN SELECT * FROM dblink('host='||serveur||' dbname='||database||' user='||users||' password='||password,'select * FROM articlesristourne where idristourne = '||plans_.id)
			AS t(id integer, borne double precision, istranche integer, taux double precision, tranche integer, valeur double precision, idristourne integer, refart character varying)
		LOOP
			SELECT INTO article_ c.id as unite, a.id FROM yvs_base_articles a LEFT JOIN yvs_base_conditionnement c ON c.article = a.id WHERE c.cond_vente AND a.ref_art = ristournes_.refart LIMIT 1;
			IF(COALESCE(article_.id, 0) > 0)THEN
				SELECT INTO ristourne_ id FROM yvs_com_ristourne WHERE article = article_.id AND plan = plan_;
				IF(COALESCE(ristourne_, 0) = 0)THEN
					INSERT INTO yvs_com_ristourne(date_debut, date_fin, permanent, actif, author, article, plan, nature, date_update, date_save, conditionnement)
						VALUES (COALESCE(plans_.debut, current_date), COALESCE(plans_.fin, current_date), permanent_, true, 16, article_.id, plan_, 'R', current_timestamp, current_timestamp, article_.unite);
					ristourne_ = currval('yvs_com_ristourne_id_seq');
				END IF;
				IF(COALESCE(ristournes_.taux, 0) != 0)THEN
					nature_montant_ = 'TAUX';
					montant_ = COALESCE(ristournes_.taux, 0);
				ELSE
					nature_montant_ = 'MONTANT';
					montant_ = COALESCE(ristournes_.valeur, 0);
				END IF;
				SELECT INTO grille_ id FROM yvs_com_grille_ristourne WHERE ((nature_montant = 'TAUX' AND montant_ristourne = ristournes_.taux) OR (nature_montant = 'MONTANT' AND montant_ristourne = ristournes_.valeur)) AND ristourne = ristourne_;
				IF(COALESCE(grille_, 0) = 0)THEN
					INSERT INTO yvs_com_grille_ristourne(montant_minimal, montant_maximal, montant_ristourne, nature_montant, ristourne, author, base, date_update, date_save, article, conditionnement)
						VALUES (0, max_, montant_, nature_montant_, ristourne_, 16, base_, current_timestamp, current_timestamp, article_.id, article_.unite);
					grille_ = currval('yvs_com_grille_ristourne_id_seq');
				END IF;
			END IF;
		END LOOP;
	END LOOP;
	return true;
END;
$function$
;
