CREATE OR REPLACE FUNCTION migration_from_glp_depot_point_de_vente(
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
    point_ BIGINT;
    depot_ BIGINT;
    query_ CHARACTER VARYING;
BEGIN
    query_ = 'SELECT codedepot, adresse, nomresponsable, tel, type, actif, agence, mode_validation_stock, periodicite_inventaire, visible_synthese, vente_online FROM depots d WHERE actif IS TRUE AND d.type IN (''GUICHETS'',''LIVREURS'')';
    FOR depots_ IN
        SELECT * FROM dblink('host=' || host || ' dbname=' || database || ' user=' || users || ' password=' || password, query_)
                          AS t(codedepot character varying, adresse character varying, nomresponsable character varying, tel character varying, type character varying, actif boolean, agence character varying, mode_validation_stock character, periodicite_inventaire integer, visible_synthese boolean, vente_online boolean)
        LOOP
            IF simulate THEN
                RAISE NOTICE '[POINT DE VENTE] % (actif: %, type: %)', depots_.codedepot, depots_.actif, depots_.type;
            ELSE
                SELECT INTO point_ y.id FROM yvs_base_point_vente y WHERE y.code = depots_.codedepot;
                IF COALESCE(point_, 0) = 0 THEN
                    insert into yvs_base_point_vente (code, libelle, adresse, agence, author, reglement_auto, actif,
                                                             livraison_on, date_update, date_save, commission_for, prix_min_strict,
                                                             accept_client_no_name, validation_reglement,telephone,
                                                             type,saisie_phone_obligatoire, comptabilisation_auto)
                    values (depots_.codedepot, depots_.codedepot, depots_.adresse, agence_,author, true, true,
                            'V', current_timestamp, current_timestamp, 'C', true, true,
                            true, depots_.tel,'C',false, true)
                    RETURNING id INTO point_;
                END IF;
                SELECT INTO depot_ d.id from yvs_base_depots d where d.code=depots_.codedepot AND d.agence=agence_;
                IF(depot_ IS NOT NULL) THEN
                    insert into yvs_base_point_vente_depot (depot, point_vente, actif, author, principal, date_update, date_save)
                    values (depot_, point_, true, author, true, current_timestamp,
                            current_timestamp);
                END IF;
            END IF;
        END LOOP;

    RETURN true;

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Erreur : %', SQLERRM;
    RETURN false;
END;
$$;
