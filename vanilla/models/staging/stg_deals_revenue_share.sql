{{ config(materialized='view') }}

SELECT 
    deal_id,
    opportunity_id,
    deal_type,
    buyer_company,
    supplier_company,
    buyer_name,
    buyer_am,
    supplier_name,
    supplier_am,
    buyer_country,
    supplier_country,
    buyer_region,
    supplier_region,
    deal_created_at,
    booked_gross_revenue,
    confirmed_gross_revenue,
    buyer_am_id,
    supplier_am_id,

    CASE
        WHEN buyer_am_id = supplier_am_id THEN booked_gross_revenue
        ELSE booked_gross_revenue * 0.5
    END AS buyer_am_booked_revenue_share,

    CASE
        WHEN buyer_am_id = supplier_am_id THEN confirmed_gross_revenue
        ELSE confirmed_gross_revenue * 0.5
    END AS buyer_am_confirmed_revenue_share,

    CASE
        WHEN buyer_am_id = supplier_am_id THEN confirmed_gross_revenue
        ELSE confirmed_gross_revenue * 0.5
    END AS supplier_am_confirmed_revenue_share

FROM {{ source('deals', 'deals') }}