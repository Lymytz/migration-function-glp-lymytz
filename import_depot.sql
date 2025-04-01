CREATE OR REPLACE FUNCTION migration_from_glp_depot(
    societe_ bigint,
    agence_ bigint,
    host character varying,
    database character varying,
    users character varying,
    password character varying,
    author bigint,
    simulate boolean DEFAULT true
)
    RETURNS boolean
    LANGUAGE plpgsql
AS $$
DECLARE
    depots_ RECORD;
    tranche_ RECORD;
    liason_depot_ RECORD;

    depot_ BIGINT;
    query_ CHARACTER VARYING;
    types_operation_ TEXT[] := ARRAY['TRANSFERT', 'ENTREE', 'SORTIE', 'AUTRE'];
    operations_ TEXT[] := ARRAY['DONS', 'TRANSFERT', 'INITIALISATION', 'PRODUCTION', 'RETOUR', 'ACHAT', 'AJUSTEMENT STOCK', 'DEPRECIATION', 'VENTE'];
    type_op_ CHARACTER VARYING;
    op_ CHARACTER VARYING;
    operations_transfert_ CHARACTER VARYING := 'TRANSFERT';
    creneau_depot_query_ CHARACTER VARYING;
    tranche_id_ bigint;
BEGIN
    -- Tranches horaires
    query_ = 'SELECT ordre, service, titre, typedejrnee, actif, heure_debut, heure_fin FROM public.tranchehoraire';
    FOR tranche_ IN
        SELECT * FROM dblink('host=' || host || ' dbname=' || database || ' user=' || users || ' password=' || password, query_)
                          AS t(ordre int, service character varying, titre character varying, typedejrnee character varying, actif boolean, heure_debut time, heure_fin time)
        LOOP
            IF simulate THEN
                RAISE NOTICE '[TRANCHE] % - % (% - %)', tranche_.titre, tranche_.typedejrnee, tranche_.heure_debut, tranche_.heure_fin;
            ELSE
                SELECT INTO tranche_id_ y.id FROM yvs_grh_tranche_horaire y
                WHERE y.heure_debut = tranche_.heure_debut AND y.heure_fin = tranche_.heure_fin AND y.societe = societe_;
                IF tranche_id_ IS NULL THEN
                    INSERT INTO yvs_grh_tranche_horaire
                    (titre, heure_debut, heure_fin, type_journee, actif, duree_pause, date_update, date_save, societe)
                    VALUES
                        (tranche_.titre, tranche_.heure_debut, tranche_.heure_fin, tranche_.typedejrnee, tranche_.actif, '01:00:00', current_timestamp, current_timestamp, societe_);
                END IF;
            END IF;
        END LOOP;

    -- Dépôts
    query_ = 'SELECT codedepot, adresse, nomresponsable, tel, type, actif, agence, mode_validation_stock, periodicite_inventaire, visible_synthese, vente_online FROM depots WHERE actif IS TRUE';
    FOR depots_ IN
        SELECT * FROM dblink('host=' || host || ' dbname=' || database || ' user=' || users || ' password=' || password, query_)
                          AS t(codedepot character varying, adresse character varying, nomresponsable character varying, tel character varying, type character varying, actif boolean, agence character varying, mode_validation_stock character, periodicite_inventaire integer, visible_synthese boolean, vente_online boolean)
        LOOP
            IF simulate THEN
                RAISE NOTICE '[DEPOT] % (actif: %, type: %)', depots_.codedepot, depots_.actif, depots_.type;
            ELSE
                SELECT INTO depot_ y.id FROM yvs_base_depots y WHERE y.code = depots_.codedepot;
                IF COALESCE(depot_, 0) = 0 THEN
                    INSERT INTO yvs_base_depots (abbreviation, code, designation, agence, actif, date_save, date_update, author)
                    VALUES (depots_.codedepot, depots_.codedepot, depots_.codedepot, agence_, depots_.actif, current_timestamp, current_timestamp,author)
                    RETURNING id INTO depot_;
                END IF;
            END IF;

            -- Opérations du dépôt
            FOREACH type_op_ IN ARRAY types_operation_
                LOOP
                    IF type_op_ != 'TRANSFERT' THEN
                        FOREACH op_ IN ARRAY operations_
                            LOOP
                                IF simulate THEN
                                    RAISE NOTICE '   - OP [% - %] pour %', op_, type_op_, depots_.codedepot;
                                ELSE
                                    INSERT INTO yvs_base_depot_operation (operation, type, depot, author, date_update, date_save)
                                    VALUES (op_, type_op_, depot_, null, current_timestamp, current_timestamp);
                                END IF;
                            END LOOP;
                    ELSE
                        IF simulate THEN
                            RAISE NOTICE '   - OP [TRANSFERT - TRANSFERT] pour %', depots_.codedepot;
                        ELSE
                            INSERT INTO yvs_base_depot_operation (operation, type, depot, author, date_update, date_save)
                            VALUES (operations_transfert_, type_op_, depot_, null, current_timestamp, current_timestamp);
                        END IF;
                    END IF;
                END LOOP;

            -- Tranches liées au dépôt
            creneau_depot_query_ = 'SELECT t.typedejrnee, t.heure_debut, t.heure_fin FROM tranchehoraire t INNER JOIN creneaudepot cd ON cd.tranche=t.id WHERE cd.depot=' || quote_literal(depots_.codedepot);
            FOR tranche_ IN
                SELECT * FROM dblink('host=' || host || ' dbname=' || database || ' user=' || users || ' password=' || password, creneau_depot_query_)
                                  AS t(typedejrnee character varying, heure_debut time, heure_fin time)
                LOOP
                    IF simulate THEN
                        RAISE NOTICE '   - Lier [% - %] à %', tranche_.heure_debut, tranche_.heure_fin, depots_.codedepot;
                    ELSE
                        SELECT INTO tranche_id_ t.id FROM yvs_grh_tranche_horaire t
                        WHERE t.type_journee = tranche_.typedejrnee AND t.heure_debut = tranche_.heure_debut AND t.heure_fin = tranche_.heure_fin;
                        IF tranche_id_ IS NOT NULL THEN
                            INSERT INTO yvs_com_creneau_depot (tranche, depot, actif, permanent, date_update, date_save)
                            VALUES (tranche_id_, depot_, true, true, current_timestamp, current_timestamp);
                        END IF;
                    END IF;
                END LOOP;
        END LOOP;

    -- Liaison dépôts
    FOR depots_ IN SELECT d.* FROM yvs_base_depots d WHERE d.agence = agence_
        LOOP
            query_ = 'SELECT depot_lie FROM liaison_depot WHERE depot=' || quote_literal(depots_.code);
            FOR liason_depot_ IN
                SELECT * FROM dblink('host=' || host || ' dbname=' || database || ' user=' || users || ' password=' || password, query_)
                                  AS t(depot_lie character varying)
                LOOP
                    IF simulate THEN
                        RAISE NOTICE '-> Lier % à %', depots_.code, liason_depot_.depot_lie;
                    ELSE
                        SELECT INTO depot_ d.id FROM yvs_base_depots d WHERE d.code = liason_depot_.depot_lie;
                        IF depot_ IS NOT NULL THEN
                            INSERT INTO yvs_com_liaison_depot (depot, depot_lier, date_update, date_save)
                            VALUES (depots_.id, depot_, current_timestamp, current_timestamp);
                        END IF;
                    END IF;
                END LOOP;
        END LOOP;

    RETURN true;

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Erreur : %', SQLERRM;
    RETURN false;
END;
$$;
