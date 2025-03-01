-- Identify coverage gaps by material type
WITH buyer_needs AS (
    SELECT 
        preferred_grade AS material,
        COUNT(DISTINCT buyer_id) AS total_buyers
    FROM {{ ref('stg_buyers') }}
    GROUP BY 1
),

supply_capacity AS (
    SELECT
        grade AS material,
        COUNT(DISTINCT supplier_id) AS total_suppliers,
        SUM(quantity) AS total_quantity
    FROM {{ ref('stg_suppliers') }}
    GROUP BY 1
)

SELECT 
    COALESCE(b.material, s.material) AS material,
    b.total_buyers,
    COALESCE(s.total_suppliers, 0) AS suppliers_available,
    COALESCE(s.total_quantity, 0) AS quantity_available,
    CASE 
        WHEN s.total_quantity IS NULL THEN 'Unmet Demand'
        WHEN s.total_quantity < b.total_buyers * 1000 THEN 'Partial Coverage' 
        ELSE 'Full Coverage'
    END AS coverage_status
FROM buyer_needs b
FULL OUTER JOIN supply_capacity s
    ON b.material = s.material