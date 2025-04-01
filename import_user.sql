CREATE OR REPLACE FUNCTION public.migration_from_glp_user(
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
    users_ RECORD;
    niveaux_ RECORD;

    user_ BIGINT;
    auteur_ BIGINT;
    niveau_ BIGINT;
    acces_ BIGINT;

    password_user_ CHARACTER VARYING DEFAULT '-1281017-57787275-95503875-30125-71-8-99';
    alea_mdp_ CHARACTER VARYING DEFAULT 'xKIl75qNCuiD3zqfpxTs*4ajNT062Tcc@/ZT(BJc@13yglLPXFh)zrSX-6KQUjTU0FmPJ3';

    query_ CHARACTER VARYING;
BEGIN
    -- === ADMIN ===
    SELECT INTO user_ id FROM yvs_users WHERE code_users = 'ADMINGLP';

    IF COALESCE(user_, 0) = 0 THEN
        IF simulate THEN
            RAISE NOTICE '[SIMULATION] Création utilisateur ADMINISTRATEUR';
        ELSE
            INSERT INTO yvs_users(nom_users, code_users, password_user, alea_mdp, acces_multi_agence, agence, super_admin, actif)
            VALUES ('ADMINISTRATEUR', 'ADMINGLP', password_user_, alea_mdp_, true, agence_, true, true)
            RETURNING id INTO user_;
        END IF;
    END IF;

    SELECT INTO auteur_ id FROM yvs_users_agence a WHERE a.users = user_ AND agence = agence_;

    IF COALESCE(auteur_, 0) = 0 THEN
        IF simulate THEN
            RAISE NOTICE '[SIMULATION] Liaison utilisateur ADMINGLP à l''agence';
        ELSE
            INSERT INTO yvs_users_agence(id, users, agence, actif)
            VALUES (16, user_, agence_, true);
            auteur_ := 16;
        END IF;
    END IF;

    -- === NIVEAUX D'ACCÈS ===
    query_ := 'SELECT id, designation, description, supp, actif, grade, societe, super_admin FROM niveau_acces';
    FOR niveaux_ IN
        SELECT * FROM dblink('host=' || host || ' dbname=' || database || ' user=' || users || ' password=' || password, query_)
            AS t(id integer, designation character varying, description character varying, supp boolean, actif boolean, grade character varying, societe integer, super_admin boolean)
        LOOP
            SELECT INTO niveau_ id FROM yvs_niveau_acces WHERE designation = niveaux_.designation;

            IF COALESCE(niveau_, 0) = 0 THEN
                IF simulate THEN
                    RAISE NOTICE '[SIMULATION] Création niveau d''accès : %', niveaux_.designation;
                ELSE
                    INSERT INTO yvs_niveau_acces(designation, description, actif, societe, super_admin, author)
                    VALUES (niveaux_.designation, niveaux_.description, niveaux_.actif, societe_, false, author)
                    RETURNING id INTO niveau_;
                END IF;
            END IF;
        END LOOP;

    -- === UTILISATEURS ===
    query_ := 'SELECT u.coderep, u.adresse, u.fonction, u.grade, u.groupe, u.journal, u.nom, u.password, u.prenom, u.tel, u.comission, u.compte, u.depot, u.equipe, ' ||
              'u.datesave, u.actif, u.niveau, u.agence, u.connect_only_crenau, u.vente_online, u.externe, n.designation ' ||
              'FROM users u LEFT JOIN niveau_acces n ON u.niveau = n.id';

    FOR users_ IN
        SELECT * FROM dblink('host=' || host || ' dbname=' || database || ' user=' || users || ' password=' || password,query_)
                 AS t(coderep character varying, adresse character varying, fonction character varying, grade integer, groupe character varying, journal character varying,
                      nom character varying, password character varying, prenom character varying, tel character varying, comission integer, compte character varying,
                      depot character varying, equipe character varying, datesave timestamp, actif boolean, niveau integer, agence character varying,
                      connect_only_crenau boolean, vente_online boolean, externe bigint, designation character varying)
        LOOP
            SELECT INTO niveau_ id FROM yvs_niveau_acces WHERE designation = users_.designation;

            SELECT INTO user_ id FROM yvs_users WHERE code_users = users_.coderep;

            IF COALESCE(user_, 0) = 0 THEN
                IF simulate THEN
                    RAISE NOTICE '[SIMULATION] Création utilisateur : %', users_.coderep;
                ELSE
                    INSERT INTO yvs_users(nom_users, code_users, password_user, alea_mdp, agence, actif, author)
                    VALUES (users_.nom || ' ' || users_.prenom, users_.coderep,
                            password_user_, alea_mdp_, agence_, users_.actif, author)
                    RETURNING id INTO user_;
                    INSERT INTO public.yvs_users_agence (users, agence, actif, can_action)
                    VALUES (user_, agence_, true, true);
                END IF;
            END IF;
            IF COALESCE(niveau_, 0) != 0 THEN
                SELECT INTO acces_ id FROM yvs_niveau_users WHERE id_user = user_ AND id_niveau = niveau_;
                IF COALESCE(acces_, 0) = 0 THEN
                    IF simulate THEN
                        RAISE NOTICE '[SIMULATION] Attribution niveau % à l''utilisateur %', users_.designation, users_.coderep;
                    ELSE
                        INSERT INTO yvs_niveau_users(id_user, id_niveau)
                        VALUES (user_, niveau_)
                        RETURNING id INTO acces_;
                    END IF;
                END IF;
            END IF;
        END LOOP;

    RETURN true;
END;
$$;
