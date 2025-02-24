{{ config(materialized='table') }}

WITH base AS (
    SELECT 
         dp."Nom_du_POI" AS nom,
         dp."Longitude",
         dp."Latitude",
         dp."Adresse_postale" AS adresse_postale,
         SPLIT(dp."Code_postal_et_commune", '#')[0] AS code_postal,
         SPLIT(dp."Code_postal_et_commune", '#')[1] AS commune,
         dp."Createur_de_la_donnee" AS createur_de_la_donnee,
         dp."SIT_diffuseur" AS sit_diffuseur,
         dp."Date_de_mise_a_jour"::date AS date_de_mise_a_jour,
         dp."Contacts_du_POI" AS contacts_du_poi,
         dp."Classements_du_POI" AS classements_du_poi,
         dp."Description" AS description,
         TO_GEOMETRY(
            'POINT(' || dp."Longitude"::string || ' ' || dp."Latitude"::string || ')',
            4326
         ) AS geopoint,
         dp."Categories_de_POI" AS categories_de_poi
    FROM {{ source('sources', 'datatourisme_place') }} AS dp
),
cat_extracted AS (
    SELECT 
         nom,
         ARRAY_AGG(
             DISTINCT regexp_replace(value::string, '.+[\/#]', '')
         ) AS categories
    FROM base,
         LATERAL FLATTEN(INPUT => SPLIT(categories_de_poi, '|'))
    GROUP BY nom
)
SELECT 
    b.nom,
    ce.categories,
    b.geopoint,
    b.adresse_postale,
    b.code_postal,
    b.commune,
    b.createur_de_la_donnee,
    b.sit_diffuseur,
    b.date_de_mise_a_jour,
    b.contacts_du_poi,
    b.classements_du_poi,
    b.description
FROM base AS b
LEFT JOIN cat_extracted AS ce
  ON b.nom = ce.nom