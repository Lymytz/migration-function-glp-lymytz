-- DROP FUNCTION public.import_user(int8, int8, varchar, int4, varchar, varchar, varchar);

CREATE OR REPLACE FUNCTION public.migration_from_glp_nomenclature(
    societe_ bigint,
    agence_ bigint,
    host character varying,
    port integer,
    database character varying,
    users character varying,
    password character varying,
    simulate boolean DEFAULT true
)
    RETURNS boolean
    LANGUAGE plpgsql
AS
$function$
DECLARE
    query_composant_   CHARACTER VARYING;
    query_compose_     CHARACTER VARYING;
    nomenclatures_     RECORD;
    composants_     RECORD;
    article_     RECORD;
    composant_     RECORD;
    id_nomenclature    bigint;
    id_article         bigint;
    id_comosant_exist bigint;

BEGIN
    query_compose_ =
            'SELECT DISTINCT compose, qtecompose  FROM structurations s INNER JOIN articles a ON a.refart=s.compose ' ||
            'WHERE a.sommeil IS FALSE';
    -- BEGIN Nomenclature
    FOR nomenclatures_
        IN SELECT *
           FROM dblink('host=' || host || ' dbname=' || database || ' user=' || users || ' password=' || password,
                       query_compose_)
                    AS t(compose character varying, qtecompose double precision)
        LOOP
            query_composant_ =
                    'SELECT composant, compose, ordre, qtecomposant, qtecompose, umasse FROM public.structurations s ' ||
                    'WHERE s.compose='||nomenclatures_.compose;
            -- on récupère l'article
            SELECT INTO article_ a.*,c.id id_conditionnement FROM yvs_base_articles a INNER JOIN yvs_base_famille_article f ON f.id=a.famille
                                     INNER JOIN yvs_base_conditionnement c ON c.article=a.id
                                   WHERE f.societe=societe_ AND a.ref_art=nomenclatures_.compose LIMIT 1;
            IF(article_.id IS NOT NULL AND article_.id_conditionnement IS NOT NULL)THEN
                -- Vérifie s'il y a déjà une nomenclature
                SELECT INTO id_nomenclature id FROM yvs_prod_nomenclature n WHERE n.article=article_.id;
                IF(id_nomenclature IS NULL) THEN
                    --insert new Nomenclature
                    INSERT INTO yvs_prod_nomenclature (reference, niveau, debut_validite, fin_validite, quantite, article, actif,
                                                       quantite_lie_aux_composants, principal, alway_valide, author,date_update, date_save, unite_mesure)
                    VALUES ('NOME-'||article_.ref_art||'/01', 1, current_date, current_date, nomenclatures_.qtecompose, article_.id,
                           true, false,true, true, null, current_timestamp, current_timestamp,article_.id_conditionnement)
                    RETURNING id INTO id_nomenclature;
                END IF;
                FOR composants_
                    IN SELECT *
                       FROM dblink('host=' || host || ' dbname=' || database || ' user=' || users || ' password=' || password,
                                   query_composant_)
                                AS t(composant character varying, compose character varying, ordre int,
                                     qtecomposant double precision, qtecompose double precision, umasse character varying)
                    LOOP
                        -- Gère les composants
                        SELECT INTO composant_ a.*,c.id id_conditionnement FROM yvs_base_articles a INNER JOIN yvs_base_famille_article f ON f.id=a.famille
                                                                                                 INNER JOIN yvs_base_conditionnement c ON c.article=a.id
                        WHERE f.societe=societe_ AND a.ref_art=composants_.composant LIMIT 1;
                        -- vérifie que l'article composant existe
                        IF(composant_.id IS NOT NULL AND composant_.id_conditionnement IS NOT NULL) THEN
                            SELECT INTO id_comosant_exist c.id FROM yvs_prod_composant_nomenclature c WHERE c.nomenclature=id_nomenclature AND c.article=composant_.id;
                            IF(id_comosant_exist IS NULL) THEN
                                --insert le nouveau composant
                                insert into yvs_prod_composant_nomenclature (quantite, coefficient, type, mode_arrondi, actif, article,
                                                                             nomenclature, unite, date_update, date_save, stockable, ordre,inside_cout)
                                values (composants_.qtecomposant, 0,'N', 'E', true, composant_.id, id_nomenclature,
                                        composant_.id_conditionnement, current_timestamp,current_timestamp, false, composants_.ordre, true);
                            ELSE
                                RAISE NOTICE 'Composant % est déjà inséré', composants_.composant;
                            END IF;
                        ELSE
                            RAISE NOTICE 'Le composant %s ou son conditionnement n\''a pas encore été inséré\''', composants_.composant;
                        END IF;
                    END LOOP;
            ELSE
                RAISE NOTICE 'L\''article %s ou son conditionnement n\''a pas encore été inséré\''', nomenclatures_.compose;
            END IF;
        END LOOP;
    -- END Nomenclature
    return true;
END;
$function$
;
