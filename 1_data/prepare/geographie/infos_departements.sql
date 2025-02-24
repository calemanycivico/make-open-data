{{ config(materialized='table') }}

select
    code_departement,
    nom_departement,
    code_region,
    nom_region,
    sum(cast(population as number)) as population_departement,
    -- Snowflake geographic aggregation
    ST_COLLECT(TO_GEOGRAPHY(commune_contour)) AS contour_departement
from {{ ref('infos_communes') }}
group by code_departement, nom_departement, code_region, nom_region