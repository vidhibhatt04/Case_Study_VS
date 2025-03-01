{{ config(materialized='table') }}

SELECT
  b.buyer_id,
  s.supplier_id,
  s.article_id,
  s.grade,
  s.finish,
  s.thickness_mm,
  s.width_mm,
  s.weight_kg,
  s.quantity,
  b.max_weight_kg AS buyer_max_weight,
  b.min_quantity AS buyer_min_order
FROM {{ source('buyers_sellers', 'buyers') }} b
INNER JOIN {{ source('buyers_sellers', 'suppliers') }} s
  ON b.preferred_grade = s.grade
  OR b.preferred_finish = s.finish
WHERE
  s.weight_kg <= b.max_weight_kg
  AND s.quantity >= b.min_quantity