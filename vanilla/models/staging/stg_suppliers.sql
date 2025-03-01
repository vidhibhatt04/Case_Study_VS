{{ config(materialized='view') }}

SELECT
  supplier_id,
  material,
  grade,
  quality_choice,
  finish,
  thickness_mm,
  width_mm,
  weight_kg,
  quantity,
  article_id,
  description
FROM {{ source('buyers_sellers', 'suppliers') }}