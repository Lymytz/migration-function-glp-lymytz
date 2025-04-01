CREATE OR REPLACE FUNCTION public.migration_from_glp_ristourne(
    societe_ bigint,
    agence_ bigint,
    serveur character varying,
    database character varying,
    users character varying,
    author bigint,
    password character varying
)
    RETURNS boolean
    LANGUAGE plpgsql
AS $$
DECLARE
    plans_ RECORD;
    ristournes_ RECORD;
    article_ RECORD;

    plan_ BIGINT := 0;
    ristourne_ BIGINT := 0;
    grille_ BIGINT := 0;

    permanent_ BOOLEAN := FALSE;

    base_ CHARACTER VARYING := 'QTE';
    nature_montant_ CHARACTER VARYING := 'TAUX';

    max_ DOUBLE PRECISION := 10000000;
    montant_ DOUBLE PRECISION := 0;

    query_ CHARACTER VARYING;
BEGIN
    -- Import des plans de ristourne
    query_ := 'SELECT id, base, calcul, debut, domaine, fin, intitule, modeapplication, objectif, code_acces, vente_online
               FROM planderistourne';

    FOR plans_ IN
        SELECT * FROM dblink('host=' || serveur || ' dbname=' || database || ' user=' || users || ' password=' || password, query_)
            AS t(id integer, base character varying, calcul character varying, debut date, domaine character varying,
                 fin date, intitule character varying, modeapplication character varying, objectif character varying,
                 code_acces bigint, vente_online boolean
            )
        LOOP
            -- Détermination du mode permanent
            permanent_ := UPPER(COALESCE(plans_.modeapplication, '')) = 'PERMANENCE';

            -- Définir la base
            base_ := COALESCE(UPPER(plans_.base), 'QTE');

            -- Vérifie si le plan existe déjà
            SELECT INTO plan_ id FROM yvs_com_plan_ristourne WHERE reference = plans_.intitule;

            IF COALESCE(plan_, 0) = 0 THEN
                INSERT INTO yvs_com_plan_ristourne(actif, reference, societe, author)
                VALUES (true, plans_.intitule, societe_, author)
                RETURNING id INTO plan_;
            END IF;
            -- Récupère les grilles de ristourne pour ce plan
            query_ := 'SELECT id, borne, istranche, taux, tranche, valeur, idristourne, refart FROM articlesristourne WHERE idristourne = ' || plans_.id;
            FOR ristournes_ IN
                SELECT * FROM dblink('host=' || serveur || ' dbname=' || database || ' user=' || users || ' password=' || password, query_)
                    AS t(id integer, borne double precision, istranche integer, taux double precision,
                         tranche integer, valeur double precision, idristourne integer, refart character varying)
                LOOP
                    -- Trouve l’article + conditionnement (vente uniquement)
                    SELECT INTO article_ c.id AS unite, a.id FROM yvs_base_articles a
                        INNER JOIN yvs_base_conditionnement c ON c.article = a.id
                        WHERE a.ref_art = ristournes_.refart LIMIT 1;
                    IF COALESCE(article_.id, 0) > 0 THEN
                        -- Vérifie si la ristourne existe déjà
                        SELECT INTO ristourne_ id FROM yvs_com_ristourne  WHERE article = article_.id AND plan = plan_;
                        IF COALESCE(ristourne_, 0) = 0 THEN
                            INSERT INTO yvs_com_ristourne(
                                date_debut, date_fin, permanent, actif, author,
                                article, plan, nature, conditionnement
                            )
                            VALUES (
                                       COALESCE(plans_.debut, current_date),
                                       COALESCE(plans_.fin, current_date),
                                       permanent_, true, author,
                                       article_.id, plan_, 'R', article_.unite
                                   )
                            RETURNING id INTO ristourne_;
                        END IF;

                        -- Déterminer le montant et la nature
                        IF COALESCE(ristournes_.taux, 0) != 0 THEN
                            nature_montant_ := 'TAUX';
                            montant_ := ristournes_.taux;
                        ELSE
                            nature_montant_ := 'MONTANT';
                            montant_ := COALESCE(ristournes_.valeur, 0);
                        END IF;

                        -- Vérifie si la grille existe
                        SELECT INTO grille_ id  FROM yvs_com_grille_ristourne WHERE ristourne = ristourne_
                          AND (
                            (nature_montant = 'TAUX' AND montant_ristourne = ristournes_.taux)
                                OR
                            (nature_montant = 'MONTANT' AND montant_ristourne = ristournes_.valeur)
                            );

                        IF COALESCE(grille_, 0) = 0 THEN
                            INSERT INTO yvs_com_grille_ristourne(
                                montant_minimal, montant_maximal, montant_ristourne,
                                nature_montant, ristourne, author, base, date_update,
                                date_save, article, conditionnement
                            )
                            VALUES (0, max_, montant_, nature_montant_, ristourne_,
                                       author, base_, current_timestamp, current_timestamp,
                                       article_.id, article_.unite
                                   )
                            RETURNING id INTO grille_;
                        END IF;
                    END IF;
                END LOOP;
        END LOOP;

    RETURN true;
END;
$$;
