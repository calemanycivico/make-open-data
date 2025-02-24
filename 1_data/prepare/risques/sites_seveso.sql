{{ 
    config(
        materialized='table',
    ) 
}}

WITH seveso_sites AS (
    SELECT * FROM {{ source('sources', 'seveso_2024') }}
),
renamed_columns AS (
    SELECT
        "longitude"::numeric AS longitude,
        "latitude"::numeric AS latitude,
        TO_GEOMETRY(
            'POINT(' || TO_NUMBER(seveso_sites."longitude") || ' ' || TO_NUMBER(seveso_sites."latitude") || ')',
            4326
        ) AS point,
        "identifier" AS id_site,
        "name" AS nom_site,
        "localid" AS id_national_incremental,
        "streetname" AS nom_de_rue,
        "postalcode" AS code_postal,
        "city" AS ville,
        "status" AS statut_exploitation,
        "activity" AS type_activite,
        "id_lex_aiot_seveso"::integer AS id_seuil,
        "type" AS nom_seuil
    FROM seveso_sites
)
SELECT * FROM renamed_columns
