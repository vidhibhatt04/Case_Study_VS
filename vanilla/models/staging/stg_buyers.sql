{{ config(materialized='view') }}

SELECT
  buyer_id,
  preferred_grade AS preferred_grade,
  preferred_finish AS preferred_finish,
  preferred_thickness_mm AS preferred_thickness_mm,
  preferred_width_mm AS preferred_width_mm,
  max_weight_kg,
  min_quantity
FROM {{ source('buyers_sellers', 'buyers') }}