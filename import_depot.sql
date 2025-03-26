-- DROP FUNCTION public.import_depot(int8, int8, varchar, int4, varchar, varchar, varchar);

CREATE OR REPLACE FUNCTION public.import_depot(societe_ bigint, agence_ bigint, host character varying, port integer, database character varying, users character varying, password character varying)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    depots_ RECORD;
    tranche_ RECORD;
    liason_depot_ RECORD;

    depot_ BIGINT;

    query_ CHARACTER VARYING;
    types_operation_ TEXT:=['TRANSFERT', 'ENTREE', 'SORTIE', 'AUTRE'];
    operations_ TEXT:=['DONS', 'TRANSFERT', 'INITIALISATION', 'PRODUCTION', 'RETOUR', 'ACHAT', 'AJUSTEMENT STOCK', 'DEPRECIATION', 'VENTE'];
    operations_transfert_ CHARACTER VARYING:='TRANSFERT';
    creneau_depot_query_ CHARACTER VARYING;
    tranche_id_ bigint;
BEGIN
    --Tranches horaire
        query_= 'SELECT ordre, service, titre, typedejrnee, actif, heure_debut, heure_fin FROM public.tranchehoraire';
	    FOR tranche_ IN SELECT * FROM dblink('host='||host||' dbname='||database||' user='||users||' password='||password, query_)
            AS t(ordre int, service character varying, titre character varying, typedejrnee character varying, actif boolean, heure_debut time,heure_fin time )
            LOOP
                insert into yvs_grh_tranche_horaire (titre, heure_debut, heure_fin, type_journee, actif, duree_pause, date_update, date_save, societe)
                values (tranche_.titre, tranche_.heure_debut, tranche_.heure_fin, tranche_.typedejrnee, tranche_.actif,'01:00:00', current_timestamp, current_timestamp, societe_);
            END loop;
    commit ;
    -- BEGIN DEPOT
    query_ = 'SELECT codedepot, adresse, nomresponsable, tel, type, actif, agence, mode_validation_stock, periodicite_inventaire, visible_synthese, vente_online FROM depots ';
	FOR depots_  IN SELECT * FROM dblink('host='||host||' dbname='||database||' user='||users||' password='||password, query_)
		AS t(codedepot character varying, adresse character varying, nomresponsable character varying, tel character varying, type character varying, actif boolean, agence character varying, mode_validation_stock character, periodicite_inventaire integer, visible_synthese boolean, vente_online boolean)
	LOOP
		RAISE NOTICE 'depots_ : %',depots_;
		SELECT INTO depot_ y.id FROM yvs_base_depots y WHERE y.code = depots_.codedepot;
		RAISE NOTICE 'depot_ : %',depot_;
		IF(COALESCE(depot_, 0) = 0)THEN
			INSERT INTO yvs_base_depots
			(abbreviation, code, designation, agence, actif, date_save, date_update)
			VALUES
			(depots_.codedepot, depots_.codedepot, depots_.codedepot, agence_, depots_.actif, current_timestamp, current_timestamp);
			--Les dépôts de type 'GUICHETS' et 'LIVREURS' sont aussi des points de ventes
            insert into yvs_base_point_vente (code, libelle, adresse, agence, reglement_auto, responsable, actif, livraison_on, date_update, date_save, commission_for, prix_min_strict, vente_online, accept_client_no_name, validation_reglement, telephone, type, saisie_phone_obligatoire, comptabilisation_auto)
            values (depots_.codedepot, depots_.codedepot, null, agence_, true,null, depots_.actif, 'V',current_timestamp, current_timestamp, 'C', true, false, true, true, null, 'C', false, true);
			depot_ = currval('yvs_base_depots_id_seq');
		-- générer les opérations
			FOR type_op_ IN types_operation_
			LOOP
			    IF(type_op_!='TRANSFERT') THEN
			        FOR op_ IN operations_
			        LOOP
                        INSERT INTO yvs_base_depot_operation (operation, type, depot, author, date_update, date_save)
                        values (op_, type_op_, depot_, null, current_timestamp, current_timestamp);
                    END LOOP ;
                ELSE
                    INSERT INTO yvs_base_depot_operation (operation, type, depot, author, date_update, date_save)
                    values (operations_transfert_, type_op_, depot_, null, current_timestamp, current_timestamp);
                END IF;
            END LOOP;
		-- gère les tranches horaire
            creneau_depot_query_= 'SELECT t.typedejrnee, t.heure_debut, t.heure_fin FROM tranchehoraire t INNER JOIN creneaudepot cd ON cd.tranche=t.id WHERE cd.depot='||depots_.codedepot;
            FOR tranche_ IN SELECT * FROM dblink('host='||host||' dbname='||database||' user='||users||' password='||password, creneau_depot_query_)
                                              AS t(typedejrnee character varying, heure_debut time, heure_fin time )
                LOOP
                    --trouve l'id dans notre bdd qui correspond
                    SELECT INTO tranche_id_ t.id from yvs_grh_tranche_horaire t WHERE t.type_journee=tranche_.typedejrnee AND t.heure_debut=tranche_.heure_debut AND t.heure_fin=tranche_.heure_fin;
                    IF(tranche_id_ IS NOT NULL) THEN
                        insert into yvs_com_creneau_depot (tranche, depot, actif, permanent, date_update, date_save)
                        values (tranche_id_, depot_, true, true, current_timestamp, current_timestamp);
                    END IF;
                END LOOP;
		END IF;
	END LOOP;
	-- END DEPOT
    --Liaison Dépot
    FOR depots_ IN SELECT d.* FROM yvs_base_depots d where d.agence=agence_
        LOOP
            -- trouve les dépôts rattaché
            query_='SELECT depot_lie FROM liaison_depot WHERE depot='||depots_.code;
            FOR liason_depot_ IN SELECT * FROM dblink('host='||host||' dbname='||database||' user='||users||' password='||password, query_)
                                              AS t(depot_lie character varying)
                LOOP
                    --trouve l'id dépot qui corespond dans notre bdd
                    SELECT INTO depot_ d.id from yvs_base_depots d WHERE d.code=liason_depot_.depot_lie;
                    IF(depot_ IS NOT NULL) THEN
                        insert into yvs_com_liaison_depot (depot, depot_lier, date_update, date_save)
                        values (depots_.id, depot_, current_timestamp, current_timestamp);
                    END IF;
                END LOOP;
        END LOOP;
	return true;
END;
$function$
;
