{{config(materialized='table')}}

WITH base_matches AS (
  SELECT
    b.buyer_id,
    b.preferred_grade,
    b.preferred_finish,
    b.preferred_thickness_mm,
    b.preferred_width_mm,
    s.*
  FROM {{  source('buyers_sellers', 'buyers') }} b
  CROSS JOIN {{  source('buyers_sellers', 'suppliers')}} s
  WHERE s.weight_kg <= b.max_weight_kg  
    AND s.quantity >= b.min_quantity 
),

calculated_scores AS (
  SELECT
    *,
    -- Grade and finish exact matches
    CASE WHEN grade = preferred_grade THEN 30 ELSE 0 END AS grade_score,
    CASE WHEN finish = preferred_finish THEN 30 ELSE 0 END AS finish_score,
    -- Thickness similarity score (only for non-null technical specs)
    CASE WHEN thickness_mm IS NOT NULL THEN 40 * (1 - (ABS(thickness_mm - preferred_thickness_mm) / 
        (MAX(ABS(thickness_mm - preferred_thickness_mm)) OVER (PARTITION BY buyer_id)))
    )
    ELSE 0 END AS thickness_score
  FROM base_matches
),

final_scoring AS (
  SELECT
    *,
    (grade_score + finish_score + thickness_score) AS match_score,
    ROW_NUMBER() OVER (
      PARTITION BY buyer_id 
      ORDER BY (grade_score + finish_score + thickness_score) DESC, 
        supplier_id  -- Prefer supplier1 for ties
    ) AS match_rank
  FROM calculated_scores
)

SELECT
  buyer_id,
  supplier_id,
  ROUND(match_score::numeric, 2) AS match_score,
  article_id,
  grade,
  material,
  finish,
  thickness_mm,
  width_mm,
  weight_kg,
  quantity,
  quality_choice,
  reserved_status
FROM final_scoring
WHERE match_rank <= {{ var('number_of_matches', 3) }} 