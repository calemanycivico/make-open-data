{{ 
    config(
        materialized='table',
        schema='intermediaires'
    ) 
}}

WITH source_combined AS (
    {% for millesime in ['2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024'] %}
        SELECT *, '{{ millesime }}' as source_year 
        FROM {{ source('sources', 'dvf_' ~ millesime ~ '_dev') }}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),
ventes_immobiliers_filtrees AS (
    SELECT 
        "id_mutation",
        CAST("valeur_fonciere" AS DOUBLE) as "valeur_fonciere",
        CAST("longitude" AS DOUBLE) as "longitude",
        CAST("latitude" AS DOUBLE) as "latitude",
        CAST("nombre_pieces_principales" AS NUMBER) as "nombre_pieces_principales",
        CAST("surface_reelle_bati" AS NUMBER) as "surface_reelle_bati",
        "type_local",
        LPAD(CAST("code_postal" AS VARCHAR), 5, '0') as "code_postal",
        "code_commune",
        source_year
    FROM 
        source_combined 
    WHERE 
        EXISTS (
            SELECT 1
            FROM source_combined d1
            WHERE d1."id_mutation" = source_combined."id_mutation" 
            AND d1."type_local" IN ('Appartement', 'Maison')
        ) 
        AND NOT EXISTS (
            SELECT 1
            FROM source_combined d2
            WHERE d2."id_mutation" = source_combined."id_mutation" 
            AND d2."nature_mutation" != 'Vente'
        )
),
ventes_immobiliers_aggregees_au_bien AS (
    SELECT 
        "id_mutation",
        SUM(CAST("surface_reelle_bati" AS NUMBER)) AS "total_surface",
        SUM(CAST("nombre_pieces_principales" AS NUMBER)) AS "total_pieces",
        source_year
    FROM 
        ventes_immobiliers_filtrees
    GROUP BY 
        "id_mutation",
        source_year
),
bien_principal_de_la_vente AS (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY "id_mutation"
                ORDER BY 
                    CASE 
                        WHEN "type_local" = 'Maison' THEN 1
                        WHEN "type_local" = 'Appartement' THEN 2
                        ELSE 3
                    END,
                    "surface_reelle_bati" DESC
            ) AS rang
        FROM ventes_immobiliers_filtrees
    ) subquery
    WHERE
        rang = 1
) 
SELECT 
    bien_principal_de_la_vente."id_mutation",
    bien_principal_de_la_vente."valeur_fonciere",
    bien_principal_de_la_vente."longitude",
    bien_principal_de_la_vente."latitude",
    ventes_immobiliers_aggregees_au_bien."total_pieces",
    ventes_immobiliers_aggregees_au_bien."total_surface",
    bien_principal_de_la_vente."type_local",
    bien_principal_de_la_vente."code_postal",
    bien_principal_de_la_vente."code_commune",
    ST_POINT(bien_principal_de_la_vente."longitude", bien_principal_de_la_vente."latitude") AS "geopoint",
    bien_principal_de_la_vente."valeur_fonciere" / NULLIF(ventes_immobiliers_aggregees_au_bien."total_surface", 0) AS "prix_m2",
    infos_communes.nom_commune,
    infos_communes.code_arrondissement,
    infos_communes.code_departement,
    infos_communes.code_region,
    infos_communes.nom_arrondissement,
    infos_communes.nom_departement,
    infos_communes.nom_region,
    TO_DATE(bien_principal_de_la_vente.source_year, 'YYYY') AS "millesime"
FROM 
    bien_principal_de_la_vente
JOIN 
    ventes_immobiliers_aggregees_au_bien 
        ON ventes_immobiliers_aggregees_au_bien."id_mutation" = bien_principal_de_la_vente."id_mutation"
        AND ventes_immobiliers_aggregees_au_bien.source_year = bien_principal_de_la_vente.source_year
LEFT JOIN
    {{ ref('infos_communes') }} AS infos_communes 
        ON infos_communes.code_commune = bien_principal_de_la_vente."code_commune"