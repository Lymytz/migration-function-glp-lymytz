CREATE OR REPLACE FUNCTION public.debug_migration_from_glp_depot_test(
    societe_ bigint,
    agence_ bigint,
    host character varying,
    port integer,
    database character varying,
    users character varying,
    password character varying
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
            RAISE NOTICE '[TRANCHE] % - % (% % - %)', tranche_.titre, tranche_.typedejrnee, tranche_.heure_debut, tranche_.heure_fin, tranche_.actif;
        END LOOP;

    -- Dépôts
    query_ = 'SELECT codedepot, adresse, nomresponsable, tel, type, actif, agence, mode_validation_stock, periodicite_inventaire, visible_synthese, vente_online FROM depots';
    FOR depots_ IN
        SELECT * FROM dblink('host=' || host || ' dbname=' || database || ' user=' || users || ' password=' || password, query_)
                          AS t(codedepot character varying, adresse character varying, nomresponsable character varying, tel character varying, type character varying, actif boolean, agence character varying, mode_validation_stock character, periodicite_inventaire integer, visible_synthese boolean, vente_online boolean)
        LOOP
            RAISE NOTICE '[DEPOT] Code: %, Actif: %, Type: %', depots_.codedepot, depots_.actif, depots_.type;

            RAISE NOTICE '-> Générer opérations pour %', depots_.codedepot;

            FOREACH type_op_ IN ARRAY types_operation_
                LOOP
                    IF type_op_ != 'TRANSFERT' THEN
                        FOREACH op_ IN ARRAY operations_
                            LOOP
                                RAISE NOTICE '   - Insert OPERATION [% - %] pour DEPOT % (simulation)', op_, type_op_, depots_.codedepot;
                            END LOOP;
                    ELSE
                        RAISE NOTICE '   - Insert OPERATION [TRANSFERT - TRANSFERT] pour DEPOT % (simulation)', depots_.codedepot;
                    END IF;
                END LOOP;

            -- Tranches horaires liées
            creneau_depot_query_ = 'SELECT t.typedejrnee, t.heure_debut, t.heure_fin FROM tranchehoraire t INNER JOIN creneaudepot cd ON cd.tranche=t.id WHERE cd.depot=' || quote_literal(depots_.codedepot);
            FOR tranche_ IN
                SELECT * FROM dblink('host=' || host || ' dbname=' || database || ' user=' || users || ' password=' || password, creneau_depot_query_)
                                  AS t(typedejrnee character varying, heure_debut time, heure_fin time)
                LOOP
                    RAISE NOTICE '   - Lier tranche [% - %] au dépôt % (simulation)', tranche_.heure_debut, tranche_.heure_fin, depots_.codedepot;
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
                    RAISE NOTICE '-> Lier dépôt % à % (simulation)', depots_.code, liason_depot_.depot_lie;
                END LOOP;
        END LOOP;

    RETURN true;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Erreur dans la fonction debug : %', SQLERRM;
        RETURN false;
END;
$$;
