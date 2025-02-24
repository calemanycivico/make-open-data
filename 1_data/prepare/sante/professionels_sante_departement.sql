{{ config(materialized='table') }}

{% set profession_query %}
    select distinct "profession_sante" 
    from {{ source('sources', 'professionels_sante') }}
    order by "profession_sante"
{% endset %}

{% set results = run_query(profession_query) %}

{% if execute %}
    {% set professions = results.columns[0].values() %}
{% else %}
    {% set professions = [] %}
{% endif %}

with source_data as (
    select
        "departement"::varchar as departement,
        "profession_sante"::varchar as profession_sante,
        "annee"::varchar as annee,
        try_cast("effectif" as number) as effectif
    from {{ source('sources', 'professionels_sante') }}
),

aggreger_effectif_sante_unpivot as (
    SELECT
        departement,
        profession_sante,
        annee,
        sum(effectif) as effectif
    FROM source_data
    GROUP BY departement, profession_sante, annee
),

aggreger_effectif_sante_departements as (
    select
        departement,
        annee,
        {% for profession in professions %}
        sum(case when profession_sante = '{{ profession }}' then effectif else 0 end) as "{{ profession | replace(' ', '_') | replace('-', '_') }}"
        {%- if not loop.last -%},{% endif %}
        {% endfor %}
    FROM aggreger_effectif_sante_unpivot
    group by departement, annee
)

select 
    agg.*,
    info.*
from aggreger_effectif_sante_departements agg
left join {{ ref('infos_departements') }} info
    on agg.departement = info.code_departement