{{ config(materialized='view', schema='simulations') }}
WITH fake_knn_table AS (
    SELECT 1 AS id,
           TO_GEOMETRY('POINT(3.832 43.7)', 4326) AS geopoint,
           100 AS "valeur",
           '2024' AS millesime,
           '123' AS code_arrondissement
    UNION ALL
    SELECT 3,
           TO_GEOMETRY('POINT(3.830 43.7)', 4326),
           200,
           '2024',
           '123'
    UNION ALL
    SELECT 2,
           TO_GEOMETRY('POINT(3.831 43.7)', 4326),
           300,
           '2024',
           '123'
    UNION ALL
    SELECT 4,
           TO_GEOMETRY('POINT(3.839 43.7)', 4326),
           400,
           '2024',
           '123'
    UNION ALL
    SELECT 6,
           TO_GEOMETRY('POINT(3.838 43.7)', 4326),
           500,
           '2024',
           '123'
)
SELECT * FROM fake_knn_table 
