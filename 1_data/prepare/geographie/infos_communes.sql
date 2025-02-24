{{ config(materialized='table') }}

WITH filtre_cog_communes AS (
    -- Filter out non-commune rows to avoid confusion in filtering
    SELECT * 
    FROM {{ source('sources', 'cog_communes') }} AS cog_communes
    WHERE cog_communes."type" IN ('commune-actuelle', 'arrondissement-municipal')
),
denomalise_cog AS (
    SELECT
        LPAD(CAST(filtre_cog_communes."code" AS TEXT), 5, '0') AS code_commune,
        filtre_cog_communes."nom" AS nom_commune,
        filtre_cog_communes."arrondissement" AS code_arrondissement,
        filtre_cog_communes."departement" AS code_departement,
        filtre_cog_communes."region" AS code_region,
        "codesPostaux" AS codes_postaux,
        filtre_cog_communes."population" AS population,
        filtre_cog_communes."zone" AS code_zone,
        cog_arrondissements."nom" AS nom_arrondissement,
        cog_departements."nom" AS nom_departement,
        cog_regions."nom" AS nom_region
    FROM filtre_cog_communes
    LEFT JOIN {{ source('sources', 'cog_arrondissements') }} AS cog_arrondissements
        ON cog_arrondissements."code" = filtre_cog_communes."arrondissement"
    LEFT JOIN {{ source('sources', 'cog_departements') }} AS cog_departements
        ON cog_departements."code" = filtre_cog_communes."departement"
    LEFT JOIN {{ source('sources', 'cog_regions') }} AS cog_regions
        ON cog_regions."code" = filtre_cog_communes."region"
),
laposte_gps AS (
    SELECT DISTINCT
        LPAD(CAST(cog_poste."code_commune_insee" AS TEXT), 5, '0') AS code_commune,
        CAST(SPLIT_PART(cog_poste."_geopoint", ',', 1) AS FLOAT) AS commune_latitude,
        CAST(SPLIT_PART(cog_poste."_geopoint", ',', 2) AS FLOAT) AS commune_longitude
    FROM {{ source('sources', 'cog_poste') }} AS cog_poste
),
ign_shapes AS (
    SELECT "INSEE_COM" AS code_commune,
           "geometry" AS commune_contour 
    FROM {{ source('sources', 'shape_commune_2024') }}
    UNION
    SELECT "INSEE_ARM" AS code_commune,
           "geometry" AS commune_contour
    FROM {{ source('sources', 'shape_arrondissement_municipal_2024') }}
)

SELECT
    denomalise_cog.*,
    laposte_gps.commune_latitude,
    laposte_gps.commune_longitude,
    TO_GEOMETRY(
        'POINT(' || laposte_gps.commune_longitude || ' ' || laposte_gps.commune_latitude || ')',
        4326
    ) AS commune_centre_geopoint,
    ign_shapes.commune_contour
FROM denomalise_cog
LEFT JOIN laposte_gps
    ON denomalise_cog.code_commune = laposte_gps.code_commune
LEFT JOIN ign_shapes
    ON denomalise_cog.code_commune = ign_shapes.code_commune 