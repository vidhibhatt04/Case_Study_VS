{{ config(materialized='view') }}

SELECT *
FROM {{ source('deals', 'deals') }}