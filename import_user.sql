-- DROP FUNCTION public.import_user(int8, int8, varchar, int4, varchar, varchar, varchar);

CREATE OR REPLACE FUNCTION public.import_user(societe_ bigint, agence_ bigint, host character varying, port integer, database character varying, users character varying, password character varying)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
DECLARE
    users_ RECORD;

    user_ BIGINT;
    auteur_ BIGINT;

	password_user_ CHARACTER VARYING DEFAULT '-1281017-57787275-95503875-30125-71-8-99';
	alea_mdp_ CHARACTER VARYING DEFAULT 'xKIl75qNCuiD3zqfpxTs*4ajNT062Tcc@/ZT(BJc@13yglLPXFh)zrSX-6KQUjTU0FmPJ3';

    query_ CHARACTER VARYING;

BEGIN

	-- BEGIN ADMIN
	SELECT INTO user_ y.id FROM yvs_users y WHERE y.code_users = 'ADMINGLP';
	RAISE NOTICE 'user_ : % %',user_,agence_;
	IF(COALESCE(user_, 0) = 0)THEN
		INSERT INTO yvs_users
		(nom_users, code_users, password_user, alea_mdp, acces_multi_agence, agence, super_admin, actif) 
		VALUES 
		('ADMINISTRATEUR', 'ADMINGLP', password_user_, alea_mdp_, true, agence_, true, true);
		user_ = currval('yvs_users_id_seq');
	END IF;
	SELECT INTO auteur_ y.id FROM yvs_users_agence y WHERE y.users = user_ AND y.agence = agence_;
	RAISE NOTICE 'auteur_ : %',auteur_;
	IF(COALESCE(auteur_, 0) = 0)THEN
		INSERT INTO yvs_users_agence
		(id, users, agence, actif) 
		VALUES 
		(16, user_, agence_, true);
		auteur_ = 16;
	END IF;
	-- END ADMIN

	-- BEGIN UTILISATEUR 
    query_ = 'SELECT coderep, adresse, fonction, grade, groupe, journal, nom, password, prenom, tel, comission, compte, depot, equipe, 
			datesave, actif, niveau, agence, connect_only_crenau, vente_online, externe FROM users ';
	FOR users_  IN SELECT * FROM dblink('host='||host||' dbname='||database||' user='||users||' password='||password, query_)
		AS t(coderep character varying, adresse character varying, fonction character varying, grade integer, groupe character varying, journal character varying, 
			nom character varying, password character varying, prenom character varying, tel character varying, comission integer, compte character varying, 
			depot character varying, equipe character varying, datesave timestamp without time zone, actif boolean, niveau integer, agence character varying, 
			connect_only_crenau boolean, vente_online boolean, externe bigint)
	LOOP
		RAISE NOTICE 'users_ : %',users_;
		SELECT INTO user_ y.id FROM yvs_users y WHERE y.code_users = users_.coderep;
		RAISE NOTICE 'user_ : %',user_;
		IF(COALESCE(user_, 0) = 0)THEN
			INSERT INTO yvs_users
			(nom_users, code_users, password_user, alea_mdp, agence, actif) 
			VALUES 
			(users_.nom || ' ' ||users_.prenom, users_.coderep, password_user_, alea_mdp_, agence_, users_.actif);
			user_ = currval('yvs_users_id_seq');
		END IF;
	END LOOP;
	-- END UTILISATEUR 
	return true;
END;
$function$
;
