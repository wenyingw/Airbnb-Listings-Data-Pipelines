-- Part 3 Ad-hoc analysis
-- a. What are the main differences from a population point of view (i.g. higher population of under 30s) between the best performing "neighbourhood_cleansed"
-- and the worst (in terms of estimated revenue per active listings) over the last 12 months? 

-- Get the best performing "neighbourhood_cleansed"
WITH CTE AS
(
SELECT 
neighbourhood_lga
, neighbourhood_lga_code
, estimated_revenue_per_active_listings
FROM (
    SELECT 
    neighbourhood_lga
    , neighbourhood_lga_code
    , AVG((30 - availability_30) * price)::decimal(10,2) AS estimated_revenue_per_active_listings
    , ROW_NUMBER() over(order by estimated_revenue_per_active_listings DESC) AS RK_BEST
    , ROW_NUMBER() over(order by estimated_revenue_per_active_listings) AS RK_WORST
    FROM datawarehouse.fact_listing fl
    WHERE has_availability = 't'
    GROUP BY neighbourhood_lga, neighbourhood_lga_code
)
WHERE RK_BEST =1 OR RK_WORST = 1
)
SELECT DISTINCT
neighbourhood_lga
, estimated_revenue_per_active_listings
, median_age_persons
, tot_p_p
, indigenous_p_tot_p
, (100 * indigenous_p_tot_p / tot_p_p)::decimal(10, 2) AS percent_indigenous_p_tot_p
, australian_citizen_p
, (100 * australian_citizen_p / tot_p_p)::decimal(10, 2) AS percent_australian_citizen_p
, (age_15_19_yr_p + age_20_24_yr_p + age_25_34_yr_p) AS age_under_35
, (100 * (age_15_19_yr_p + age_20_24_yr_p + age_25_34_yr_p) / tot_p_p)::decimal(10, 2) AS percent_age_under_35
, (age_35_44_yr_p + age_55_64_yr_p) AS age_35_64
, (100 * (age_35_44_yr_p + age_55_64_yr_p) / tot_p_p)::decimal(10, 2) AS percent_age_35_64
, (age_65_74_yr_p + age_75_84_yr_p) AS age_above_65
, (100 * (age_65_74_yr_p + age_75_84_yr_p) / tot_p_p)::decimal(10, 2) AS percent_age_above_65
, (age_35_44_yr_p + age_55_64_yr_p + age_65_74_yr_p + age_75_84_yr_p) AS age_above_35
, (100 * (age_35_44_yr_p + age_55_64_yr_p + age_65_74_yr_p + age_75_84_yr_p) / tot_p_p)::decimal(10, 2) AS percent_over_35
FROM CTE fl
LEFT JOIN datawarehouse.dim_census dc
ON fl.neighbourhood_lga_code = dc.lga_code
ORDER BY estimated_revenue_per_active_listings DESC
;


-- b. What will be the best type of listing (property type, room type and accommodates for) for the top 5 “neighbourhood_cleansed” (in terms of estimated revenue per active listing) to have the highest number of stays?
WITH CTE AS
(
SELECT 
neighbourhood_lga
, AVG((30 - availability_30) * price)::decimal(10,2) AS estimated_revenue_per_active_listings
FROM datawarehouse.fact_listing fl
WHERE has_availability = 't'
GROUP BY neighbourhood_lga
ORDER BY estimated_revenue_per_active_listings DESC
LIMIT 5
)
SELECT 
l2.*
FROM CTE l1
LEFT JOIN (
    SELECT
    neighbourhood_lga
    , property_type
    , room_type
    , accommodates
    -- Number of stays: (only for active listings) = 30 - availability_30 
    , AVG(30 - availability_30)::decimal(10,0) AS avg_number_stays 
    FROM datawarehouse.fact_listing
    WHERE has_availability = 't'
    GROUP BY neighbourhood_lga, property_type, room_type, accommodates
) l2
ON l1.neighbourhood_lga = l2.neighbourhood_lga
QUALIFY RANK() OVER(PARTITION BY l1.neighbourhood_lga ORDER BY avg_number_stays DESC) = 1
ORDER BY estimated_revenue_per_active_listings DESC, avg_number_stays DESC, property_type, room_type, accommodates;




-- c. Do hosts with multiple listings are more inclined to have their listings in the same “neighbourhood” AS where they live?
WITH CTE AS 
(
-- get percentage and percent_range of listing in same location, including all host now
SELECT DISTINCT
host_id
, same_neighbourhood
, COUNT(id) OVER(PARTITION BY host_id, same_neighbourhood) AS ct_same
, COUNT(id) OVER(PARTITION BY host_id) AS ct_total
, (100 * COUNT(id) OVER(PARTITION BY host_id, same_neighbourhood) / COUNT(id) OVER(PARTITION BY host_id))::decimal(10, 0) AS percent
, CASE WHEN percent = 100 THEN '100%'
    WHEN percent >= 50 AND percent < 100 THEN '50% - 99%'
    WHEN percent < 50 THEN '<50%'
END AS percent_range
FROM (
    SELECT DISTINCT
    host_id
    , id
    , same_neighbourhood
    , COUNT(id) OVER(PARTITION BY host_id, same_neighbourhood) AS ct_same
    , COUNT(id) OVER(PARTITION BY host_id) AS ct_total
    , (100 * COUNT(id) OVER(PARTITION BY host_id, same_neighbourhood) / COUNT(id) OVER(PARTITION BY host_id))::decimal(10, 0) AS percent
    , CASE WHEN percent = 100 THEN '100%'
        WHEN percent >= 50 AND percent < 100 THEN '50% - 99%'
        WHEN percent < 50 THEN '<50%'
    END AS percent_range
    FROM (
        -- the table with unique host_id, id
        SELECT
        host_id
        , id
        , neighbourhood_lga
        , host_lga
        , CASE WHEN neighbourhood_lga != 'MISSING' AND host_lga != 'MISSING' AND neighbourhood_lga != 'OTHER' AND host_lga != 'OTHER' THEN
            CASE 
                WHEN neighbourhood_lga = host_lga  THEN 'TRUE'
                WHEN neighbourhood_lga != host_lga THEN 'FALSE'
                END
            ELSE 'NOT_SURE'
        END AS same_neighbourhood
        FROM datawarehouse.fact_listing
        QUALIFY row_number() OVER(partition BY host_id, id ORDER BY id DESC) = 1
        )
    )
WHERE ct_total > 1   
)
SELECT DISTINCT
percent_range AS percentage_in_same_lga
, number_of_host_same_lga_per_range
, total_number_of_host_same_lga
, (100 * number_of_host_same_lga_per_range / total_number_of_host_same_lga)::decimal(10,2) AS percentage_of_host_with_same_lga_mutiple_listings
, total_number_of_host_with_mutiple_listings
, (100 * number_of_host_same_lga_per_range / total_number_of_host_with_mutiple_listings)::decimal(10,2) AS percentage_of_host_with_mutiple_listings

FROM (
    SELECT
    *
    , (SELECT COUNT(DISTINCT host_id) FROM CTE) AS total_number_of_host_with_mutiple_listings
    , COUNT(*) OVER(PARTITION BY same_neighbourhood) AS total_number_of_host_same_lga
    , COUNT(*) OVER(PARTITION BY percent_range) AS number_of_host_same_lga_per_range
    , SUM(ct_same) OVER(PARTITION BY same_neighbourhood) AS total_number_of_listing_same_lga
    , SUM(ct_total) OVER(PARTITION BY same_neighbourhood) AS total_number_of_listing 
    FROM CTE
    WHERE same_neighbourhood = 'TRUE'
    ORDER BY host_id, same_neighbourhood
)
ORDER BY percentage_of_host_with_mutiple_listings DESC
;



-- d. For hosts with a unique listing, does their estimated revenue over the last 12 months can cover the annualised median mortgage repayment of their listing’s “neighbourhood_cleansed”?
WITH CTE AS 
(
SELECT DISTINCT 
f.host_id
, neighbourhood_lga
, total_revenue
, dc.median_mortgage_repay_monthly * 12 AS total_median_mortgage
FROM (
    
    SELECT 
    host_id AS host_id
    , neighbourhood_lga
    , neighbourhood_lga_code
    , SUM((30 - availability_30) * price) AS total_revenue
    FROM datawarehouse.fact_listing
    WHERE host_listings_count = 1
    GROUP BY host_id, neighbourhood_lga, neighbourhood_lga_code
) f
LEFT JOIN datawarehouse.dim_census dc
ON f.neighbourhood_lga_code = dc.lga_code
)
SELECT *
, (100 * total_number_of_host_can_cover_all / total_number_of_host)::decimal(10,2) AS percentage_of_host_can_cover_all
, (100 * total_number_of_host_can_cover_half / total_number_of_host)::decimal(10,2) AS percentage_of_host_can_cover_half
, (100 * total_number_of_host_can_cover_20per / total_number_of_host)::decimal(10,2) AS percentage_of_host_can_cover_20per
, (100 * total_number_of_host_cannot_cover / total_number_of_host)::decimal(10,2) AS percentage_of_host_cannot_cover
FROM(
    SELECT DISTINCT
    (SELECT COUNT(*) FROM CTE) AS total_number_of_host
    , (SELECT COUNT(*) FROM CTE WHERE total_revenue >= total_median_mortgage) AS total_number_of_host_can_cover_all
    , (SELECT COUNT(*) FROM CTE WHERE total_revenue >= total_median_mortgage*0.5) AS total_number_of_host_can_cover_half
    , (SELECT COUNT(*) FROM CTE WHERE total_revenue >= total_median_mortgage*0.2) AS total_number_of_host_can_cover_20per
    , (SELECT COUNT(*) FROM CTE WHERE total_revenue < total_median_mortgage) AS total_number_of_host_cannot_cover
);

