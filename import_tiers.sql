CREATE OR REPLACE FUNCTION public.migration_from_glp_tiers_client_fseur(
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
    clients_ RECORD;
    fournisseurs_ RECORD;

    client_ BIGINT;
    fournisseur_ BIGINT;
    tier_ BIGINT;

    _compte_ BIGINT;
    _categorie_comptable_ BIGINT := NULL;
    _plan_ristourne_ BIGINT := NULL;

    query_ CHARACTER VARYING;
BEGIN
    -- === CLIENTS ===
    query_ := 'SELECT codeclient, adresse, contact, defaultclt, email, fonction, name, prenom, raisons, sommeil, telephone, categoriec, ' ||
              'categoriet, compte, pointdevente, tristounre, datesave, confirmer, p.intitule as plan_rist FROM clients c LEFT JOIN planderistourne p ON p.id=c.tristounre';

    FOR clients_ IN
        SELECT * FROM dblink('host=' || host || ' dbname=' || database || ' user=' || users || ' password=' || password, query_)
                          AS t(codeclient character varying, adresse character varying, contact character varying, defaultclt boolean, email character varying,
                               fonction character varying, name character varying, prenom character varying, raisons character varying, sommeil boolean,
                               telephone character varying, categoriec character varying, categoriet character varying, compte character varying, pointdevente character varying,
                               tristounre integer, datesave timestamp, confirmer boolean, plan_rist character varying)
        LOOP
            SELECT INTO _compte_ id FROM yvs_base_plan_comptable WHERE num_compte = clients_.compte;
            SELECT INTO tier_ id FROM yvs_base_tiers WHERE code_tiers = clients_.codeclient;
            SELECT INTO _categorie_comptable_ FROM yvs_base_categorie_comptable WHERE code=clients_.categoriec;
            IF COALESCE(tier_, 0) = 0 THEN
                IF simulate THEN
                    RAISE NOTICE '[SIMULATION] Insertion Tiers (client) : %', clients_.codeclient;
                ELSE
                    INSERT INTO yvs_base_tiers(adresse, client, code_tiers, nom, prenom, tel, societe, compte_collectif, agence, actif)
                    VALUES (clients_.adresse, true, clients_.codeclient, clients_.name, clients_.prenom,
                            clients_.contact, societe_, _compte_, agence_, true)
                    RETURNING id INTO tier_;
                END IF;
            END IF;

            SELECT INTO client_ id FROM yvs_com_client WHERE code_client = clients_.codeclient;
            SELECT INTO _plan_ristourne_ id FROM yvs_com_plan_ristourne p WHERE p.reference=clients_.plan_rist;
            IF COALESCE(client_, 0) = 0 THEN
                IF simulate THEN
                    RAISE NOTICE '[SIMULATION] Insertion Client: % with ristourne %', clients_.codeclient, _plan_ristourne_;
                ELSE
                    INSERT INTO yvs_com_client(tiers, code_client, nom, prenom, compte, categorie_comptable,
                                               plan_ristourne, actif, defaut, suivi_comptable)
                    VALUES (tier_, clients_.codeclient, clients_.name, clients_.prenom, _compte_,
                            _categorie_comptable_, _plan_ristourne_,
                            not clients_.sommeil, clients_.defaultclt, true)
                    RETURNING id INTO client_;
                END IF;
            END IF;
        END LOOP;

    -- === FOURNISSEURS ===
    query_ := 'SELECT codefseur, adresse, contact, email, fonction, name, prenom, sommeil, telephone, categoriec, compte, modelr, datesave FROM fournisseurs';

    FOR fournisseurs_ IN
        SELECT * FROM dblink('host=' || host || ' dbname=' || database || ' user=' || users || ' password=' || password, query_)
                          AS t(codefseur character varying, adresse character varying, contact character varying, email character varying, fonction character varying,
                               name character varying, prenom character varying, sommeil boolean, telephone character varying, categoriec character varying,
                               compte character varying, modelr character varying, datesave timestamp)
        LOOP
            SELECT INTO _compte_ id FROM yvs_base_plan_comptable WHERE num_compte = fournisseurs_.compte;
            SELECT INTO _categorie_comptable_ FROM yvs_base_categorie_comptable WHERE code=clients_.categoriec;
            SELECT INTO tier_ id FROM yvs_base_tiers WHERE code_tiers = fournisseurs_.codefseur;

            IF COALESCE(tier_, 0) = 0 THEN
                IF simulate THEN
                    RAISE NOTICE '[SIMULATION] Insertion Tiers (fournisseur) : %', fournisseurs_.codefseur;
                ELSE
                    INSERT INTO yvs_base_tiers(adresse, fournisseur, code_tiers, nom, prenom, tel, societe, compte_collectif, agence, actif)
                    VALUES (fournisseurs_.adresse, true, fournisseurs_.codefseur, fournisseurs_.name, fournisseurs_.prenom, fournisseurs_.contact, societe_, _compte_, agence_, true)
                    RETURNING id INTO tier_;
                END IF;
            END IF;

            SELECT INTO fournisseur_ id FROM yvs_base_fournisseur WHERE code_fsseur = fournisseurs_.codefseur;

            IF COALESCE(fournisseur_, 0) = 0 THEN
                IF simulate THEN
                    RAISE NOTICE '[SIMULATION] Insertion Fournisseur : %', fournisseurs_.codefseur;
                ELSE
                    INSERT INTO yvs_base_fournisseur(tiers, code_fsseur, nom, prenom, compte, categorie_comptable, actif)
                    VALUES (tier_, fournisseurs_.codefseur, fournisseurs_.name,
                            fournisseurs_.prenom, _compte_, _categorie_comptable_, not fournisseurs_.sommeil)
                    RETURNING id INTO fournisseur_;
                END IF;
            END IF;
        END LOOP;

    RETURN true;
END;
$$;
