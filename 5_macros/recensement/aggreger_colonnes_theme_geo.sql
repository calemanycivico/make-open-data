{% macro aggreger_colonnes_theme_geo(theme, table_renomee, champs_geo_arrivee) %}

{% set colonnes_a_aggreger_liste = lister_colonnes_par_theme(theme) %}

{% set colonnes_cles_liste = ['POIDS_DU_LOGEMENT', 'CODE_COMMUNE_INSEE', 'CODE_IRIS'] %}

with poids_par_geo as (
    SELECT
      {{ champs_geo_arrivee }},
      SUM(CASE WHEN "CATL" = '1' THEN POIDS_DU_LOGEMENT ELSE 0 END) AS nombre_de_menage_base_ou_logements_occupee,
      SUM(CASE WHEN "CATL" = '2' THEN POIDS_DU_LOGEMENT ELSE 0 END) AS nombre_de_logements_occasionnels,
      SUM(CASE WHEN "CATL" = '3' THEN POIDS_DU_LOGEMENT ELSE 0 END) AS nombre_de_logements_residences_secondaires,
      SUM(CASE WHEN "CATL" = '4' THEN POIDS_DU_LOGEMENT ELSE 0 END) AS nombre_de_logements_vacants,
      SUM(POIDS_DU_LOGEMENT) AS nombre_de_logements_total_tous_status_occupation
    FROM
      {{ ref(table_renomee) }}
    GROUP BY
      {{ champs_geo_arrivee }}
), 
poids_par_geo_clean as (
    SELECT  
      {{ champs_geo_arrivee }},
      CAST(COALESCE(nombre_de_menage_base_ou_logements_occupee, 0) AS INT) as nombre_de_menage_base_ou_logements_occupee,
      CAST(COALESCE(nombre_de_logements_occasionnels, 0) AS INT) as nombre_de_logements_occasionnels,
      CAST(COALESCE(nombre_de_logements_residences_secondaires, 0) AS INT) as nombre_de_logements_residences_secondaires,
      CAST(COALESCE(nombre_de_logements_vacants, 0) AS INT) as nombre_de_logements_vacants,
      CAST(COALESCE(nombre_de_logements_total_tous_status_occupation, 0) AS INT) as nombre_de_logements_total_tous_status_occupation
    FROM
      poids_par_geo
),
final_aggregated as (

    SELECT * 
    FROM poids_par_geo_clean

    {% for colonne_a_aggreger in colonnes_a_aggreger_liste %}

      LEFT JOIN ( 
          {{ aggreger_logement_par_colonne(table_renomee, colonnes_a_aggreger_liste, colonne_a_aggreger, colonnes_cles_liste, champs_geo_arrivee) }}
      ) as alias_{{ colonne_a_aggreger }}_par_geo
      USING ({{ champs_geo_arrivee }})

    {% endfor %}

)

select * from final_aggregated
{% endmacro %}
