####################################################################################
#
# BDE-AST-3 - Building data pipelines with Airflow
# Student Name: Wenying Wu
# Student ID: 14007025
# This py scripy is used to load the raw data into Database
#
####################################################################################

import os
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import re
from psycopg2.extras import execute_values
from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


####################################################################################
#
#   Load Environment Variables
#
####################################################################################
# Connection variables
snowflake_conn_id = "snowflake_conn_id"


####################################################################################
#
#   DAG Settings
#
####################################################################################


dag_default_args = {
    'owner': 'Wenying Wu',
    'start_date': datetime.now(),  
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,                       
    'retry_delay': timedelta(minutes=5),            
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='Airbnb',                    
    default_args=dag_default_args,
    schedule_interval=None,             
    catchup=False,                      
    max_active_runs=1,
    concurrency=5
)

####################################################################################
#
#  Quary for dim_census
#
####################################################################################

query_refresh_raw_census = f"""
ALTER EXTERNAL TABLE raw.raw_censusG01 REFRESH;
ALTER EXTERNAL TABLE raw.raw_censusG02 REFRESH;
"""

query_get_staging_census = f"""
CREATE OR REPLACE TABLE staging.staging_census AS
SELECT 
SPLIT_PART(r1.value:c1, 'LGA', 2)::INTEGER AS lga_code
, r1.value:c4::NUMERIC AS Tot_P_P
, r1.value:c55::NUMERIC AS Indigenous_P_Tot_P
, r1.value:c70::NUMERIC AS Australian_citizen_P
, r2.value:c2::NUMERIC AS Median_age_persons
, r2.value:c3::NUMERIC AS Median_mortgage_repay_monthly
, r2.value:c9::NUMERIC AS Average_household_size
, r1.value:c13::NUMERIC AS Age_15_19_yr_P
, r1.value:c16::NUMERIC AS Age_20_24_yr_P
, r1.value:c19::NUMERIC AS Age_25_34_yr_P
, r1.value:c22::NUMERIC AS Age_35_44_yr_P
, r1.value:c25::NUMERIC AS Age_45_54_yr_P
, r1.value:c28::NUMERIC AS Age_55_64_yr_P
, r1.value:c31::NUMERIC AS Age_65_74_yr_P
, r1.value:c34::NUMERIC AS Age_75_84_yr_P
FROM raw.raw_censusG01 r1
FULL JOIN raw.raw_censusG02 r2
ON r1.value:c1 = r2.value:c1
;
"""

query_get_dim_census = f"""
CREATE OR REPLACE TABLE datawarehouse.dim_census (
    lga_code VARCHAR NOT NULL
    , tot_p_p NUMERIC NULL
    , indigenous_p_tot_p NUMERIC NULL
    , australian_citizen_p NUMERIC NULL
    , median_age_persons NUMERIC NULL
    , median_mortgage_repay_monthly NUMERIC NULL
    , average_household_size NUMERIC NULL
    , age_15_19_yr_p NUMERIC NULL
    , age_20_24_yr_p NUMERIC NULL
    , age_25_34_yr_p NUMERIC NULL
    , age_35_44_yr_p NUMERIC NULL
    , age_45_54_yr_p NUMERIC NULL
    , age_55_64_yr_p NUMERIC NULL
    , age_65_74_yr_p NUMERIC NULL
    , age_75_84_yr_p NUMERIC NULL
    , CONSTRAINT census_pk PRIMARY KEY (lga_code)
);


INSERT INTO datawarehouse.dim_census (
    lga_code
    , tot_p_p
    , indigenous_p_tot_p
    , australian_citizen_p
    , median_age_persons
    , median_mortgage_repay_monthly
    , average_household_size
    , age_15_19_yr_p
    , age_20_24_yr_p
    , age_25_34_yr_p
    , age_35_44_yr_p
    , age_45_54_yr_p
    , age_55_64_yr_p
    , age_65_74_yr_p
    , age_75_84_yr_p
)
SELECT * 
FROM staging.staging_census
;
"""


####################################################################################
#
#  Query for location data
#
####################################################################################
query_refresh_raw_location = f"""
ALTER EXTERNAL TABLE raw.raw_lga REFRESH;
ALTER EXTERNAL TABLE raw.raw_ssc REFRESH;
"""

query_get_staging_location = f"""
CREATE OR REPLACE TABLE staging.staging_location as
SELECT 
lga_code
, lga_name
, suburb_name
FROM (
    SELECT DISTINCT
    l.value:c2::VARCHAR AS lga_code
    , TRIM(UPPER(split_part(s.value:c3, ' (', 1)))::VARCHAR AS suburb_name
    , TRIM(UPPER(split_part(l.value:c3, ' (', 1)))::VARCHAR AS lga_name
    , SUM(s.value:c6) OVER(PARTITION BY lga_code) AS total_area
    FROM raw.raw_ssc s 
    FULL JOIN raw.raw_lga l
    ON s.value:c1 = l.value:c1
    WHERE lga_code IS NOT NULL
    ORDER BY lga_name, suburb_name
)
QUALIFY ROW_NUMBER() OVER(PARTITION BY suburb_name ORDER BY total_area DESC) = 1
;
"""


####################################################################################
#
#  Query for dim_listing
#
####################################################################################
query_refresh_raw_listing = f"""
ALTER EXTERNAL TABLE raw.raw_listing REFRESH;
"""

query_get_staging_listing = f"""
CREATE OR REPLACE TABLE staging.staging_listing as
SELECT 
    value:c1::VARCHAR AS id
    , value:c2::VARCHAR AS listing_url
    , value:c3::VARCHAR AS scrape_id
    , value:c4::date AS last_scraped
    , value:c5::VARCHAR AS name
    , value:c6::TEXT AS description
    , value:c7::TEXT AS neighborhood_overview
    , value:c8::TEXT AS picture_url
    , value:c9::INTEGER AS host_id
    , value:c10::TEXT AS host_url
    , value:c11::TEXT AS host_name
    , value:c12::TEXT AS host_since
    , value:c13::TEXT AS host_location
    , value:c14::TEXT AS host_about
    , value:c15::VARCHAR AS host_response_time
    , value:c16::VARCHAR AS host_response_rate
    , value:c17::VARCHAR AS host_acceptance_rate
    , value:c18::VARCHAR AS host_is_superhost
    , value:c19::TEXT AS host_thumbnail_url
    , value:c20::TEXT AS host_picture_url
    , value:c21::TEXT AS host_neighbourhood
    , value:c22::VARCHAR AS host_listings_count
    , value:c23::VARCHAR AS host_total_listings_count
    , value:c24::TEXT AS host_verifications
    , value:c25::VARCHAR AS host_has_profile_pic
    , value:c26::VARCHAR AS host_identity_verified
    , value:c27::VARCHAR AS neighbourhood
    , UPPER(value:c28)::VARCHAR AS neighbourhood_cleansed_raw
    , value:c29::VARCHAR AS neighbourhood_group_cleansed
    , value:c30::VARCHAR AS latitude
    , value:c31::VARCHAR AS longitude
    , value:c32::VARCHAR AS property_type
    , value:c33::VARCHAR AS room_type
    , value:c34::VARCHAR AS accommodates
    , value:c35::VARCHAR AS bathrooms
    , value:c36::TEXT AS bathrooms_text
    , value:c37::VARCHAR AS bedrooms
    , value:c38::VARCHAR AS beds
    , value:c39::TEXT AS amenities
    , TRY_CAST(split_part(value:c40, '$', -1) AS NUMERIC) AS price
    , value:c41::VARCHAR AS minimum_nights
    , value:c42::VARCHAR AS maximum_nights
    , value:c43::VARCHAR AS minimum_minimum_nights
    , value:c44::VARCHAR AS maximum_minimum_nights
    , value:c45::VARCHAR AS minimum_maximum_nights
    , value:c46::VARCHAR AS maximum_maximum_nights
    , value:c47::VARCHAR AS minimum_nights_avg_ntm
    , value:c48::VARCHAR AS maximum_nights_avg_ntm
    , value:c49::VARCHAR AS calendar_updated
    , value:c50::VARCHAR AS has_availability
    , value:c51::VARCHAR AS availability_30
    , value:c52::VARCHAR AS availability_60
    , value:c53::VARCHAR AS availability_90
    , value:c54::VARCHAR AS availability_365
    , value:c55::VARCHAR AS calendar_last_scraped
    , value:c56::VARCHAR AS number_of_reviews
    , value:c57::VARCHAR AS number_of_reviews_ltm
    , value:c58::VARCHAR AS number_of_reviews_l30d
    , value:c59::VARCHAR AS first_review
    , value:c60::VARCHAR AS last_review
    , value:c61::NUMERIC AS review_scores_rating
    , value:c62::NUMERIC AS review_scores_accuracy
    , value:c63::NUMERIC AS review_scores_cleanliness
    , value:c64::NUMERIC AS review_scores_checkin
    , value:c65::NUMERIC AS review_scores_communication
    , value:c66::NUMERIC AS review_scores_location
    , value:c67::NUMERIC AS review_scores_value
    , value:c68::TEXT AS license
    , value:c69::VARCHAR AS instant_bookable
    , value:c70::INTEGER AS calculated_host_listings_count
    , value:c71::INTEGER AS calculated_host_listings_count_entire_homes
    , value:c72::INTEGER AS calculated_host_listings_count_private_rooms
    , value:c73::INTEGER AS calculated_host_listings_count_shared_rooms
    , value:c74::NUMERIC AS reviews_per_month
    , SPLIT_PART(metadata$filename, '/', -1)::VARCHAR AS filename
FROM raw.raw_listing
QUALIFY row_number() OVER(PARTITION BY value:c1::VARCHAR, 
    SPLIT_PART(metadata$filename, '/', -1)::VARCHAR ORDER BY value:c1::VARCHAR desc) = 1
;
"""

query_get_fact_listing = f"""
CREATE OR REPLACE TABLE datawarehouse.fact_listing (
    id VARCHAR NOT NULL
    , listing_url VARCHAR NULL
    , scrape_id VARCHAR NULL
    , last_scraped date NULL
    , name VARCHAR NULL
    , description TEXT NULL
    , neighborhood_overview TEXT NULL
    , picture_url TEXT NULL
    , host_id INTEGER NOT NULL
    , host_url TEXT NULL
    , host_name TEXT NULL
    , host_since TEXT NULL
    , host_location TEXT NULL
    , host_about TEXT NULL
    , host_response_time VARCHAR NULL
    , host_response_rate VARCHAR NULL
    , host_acceptance_rate VARCHAR NULL
    , host_is_superhost VARCHAR NULL
    , host_thumbnail_url TEXT NULL
    , host_picture_url TEXT NULL
    , host_neighbourhood TEXT NULL
    , host_listings_count VARCHAR NULL
    , host_total_listings_count VARCHAR NULL
    , host_verifications TEXT NULL
    , host_has_profile_pic VARCHAR NULL
    , host_identity_verified VARCHAR NULL
    , neighbourhood VARCHAR NULL
    , neighbourhood_cleansed_raw VARCHAR NULL
    , neighbourhood_group_cleansed VARCHAR NULL
    , latitude VARCHAR NULL
    , longitude VARCHAR NULL
    , property_type VARCHAR NULL
    , room_type VARCHAR NULL
    , accommodates VARCHAR NULL
    , bathrooms VARCHAR NULL
    , bathrooms_TEXT TEXT NULL
    , bedrooms VARCHAR NULL
    , beds VARCHAR NULL
    , amenities TEXT NULL
    , price DECIMAL(10,2) NOT NULL
    , minimum_nights VARCHAR NULL
    , maximum_nights VARCHAR NULL
    , minimum_minimum_nights VARCHAR NULL
    , maximum_minimum_nights VARCHAR NULL
    , minimum_maximum_nights VARCHAR NULL
    , maximum_maximum_nights VARCHAR NULL
    , minimum_nights_avg_ntm VARCHAR NULL
    , maximum_nights_avg_ntm VARCHAR NULL
    , calendar_updated VARCHAR NULL
    , has_availability VARCHAR NULL
    , availability_30 VARCHAR NULL
    , availability_60 VARCHAR NULL
    , availability_90 VARCHAR NULL
    , availability_365 VARCHAR NULL
    , calendar_last_scraped VARCHAR NULL
    , number_of_reviews VARCHAR NULL
    , number_of_reviews_ltm VARCHAR NULL
    , number_of_reviews_l30d VARCHAR NULL
    , first_review VARCHAR NULL
    , last_review VARCHAR NULL
    , review_scores_rating NUMERIC NULL
    , review_scores_accuracy NUMERIC NULL
    , review_scores_cleanliness NUMERIC NULL
    , review_scores_checkin NUMERIC NULL
    , review_scores_communication NUMERIC NULL
    , review_scores_location NUMERIC NULL
    , review_scores_value NUMERIC NULL
    , license TEXT NULL
    , instant_bookable VARCHAR NULL
    , calculated_host_listings_count INTEGER NULL
    , calculated_host_listings_count_entire_homes INTEGER NULL
    , calculated_host_listings_count_private_rooms INTEGER NULL
    , calculated_host_listings_count_shared_rooms INTEGER NULL
    , reviews_per_month NUMERIC NULL
    , filename VARCHAR NULL
    , host_suburb VARCHAR NULL
    , neighbourhood_suburb VARCHAR NULL
    , file_month INTEGER NULL
    , file_year INTEGER NULL
    , listing_year INTEGER NULL
    , listing_month INTEGER NULL
    , neighbourhood_suburbname VARCHAR NULL
    , neighbourhood_lganame VARCHAR NULL
    , host_suburbname VARCHAR NULL
    , host_lganame VARCHAR NULL
    , file_date date NULL
    , neighbourhood_cleansed VARCHAR NULL
    , neighbourhood_lga VARCHAR NULL
    , host_lga VARCHAR NULL   
    , neighbourhood_lga_code VARCHAR NULL
    , host_lga_code VARCHAR NULL    
    , CONSTRAINT listing_pk PRIMARY KEY (id, filename)
    , CONSTRAINT location_fk FOREIGN KEY (neighbourhood_lga_code) REFERENCES datawarehouse.dim_census(lga_code)
);
    


INSERT INTO datawarehouse.fact_listing (
    id, listing_url, scrape_id, last_scraped, name, description, neighborhood_overview, picture_url, 
    host_id, host_url, host_name, host_since, host_location, host_about, host_response_time, 
    host_response_rate, host_acceptance_rate, host_is_superhost, host_thumbnail_url, host_picture_url, 
    host_neighbourhood, host_listings_count, host_total_listings_count, host_verifications, 
    host_has_profile_pic, host_identity_verified, neighbourhood, neighbourhood_cleansed_raw, 
    neighbourhood_group_cleansed, latitude, longitude, property_type, room_type, accommodates, 
    bathrooms, bathrooms_text, bedrooms, beds, amenities, price, minimum_nights, maximum_nights, 
    minimum_minimum_nights, maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights, 
    minimum_nights_avg_ntm, maximum_nights_avg_ntm, calendar_updated, has_availability, availability_30, 
    availability_60, availability_90, availability_365, calendar_last_scraped, number_of_reviews, 
    number_of_reviews_ltm, number_of_reviews_l30d, first_review, last_review, review_scores_rating, 
    review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, 
    review_scores_location, review_scores_value, license, instant_bookable, calculated_host_listings_count, 
    calculated_host_listings_count_entire_homes, calculated_host_listings_count_private_rooms, 
    calculated_host_listings_count_shared_rooms, reviews_per_month, filename, host_suburb, 
    neighbourhood_suburb, file_month, file_year, listing_year, listing_month, 
    neighbourhood_suburbname, neighbourhood_lganame, host_suburbname, host_lganame, file_date, 
    neighbourhood_cleansed, neighbourhood_lga, host_lga, neighbourhood_lga_code, host_lga_code
)
WITH CTE AS
(
    SELECT 
    f.*
    , s1.suburb_name AS neighbourhood_suburbname
    , s1.lga_name AS neighbourhood_lganame
    , s2.suburb_name AS host_suburbname
    , s2.lga_name AS host_lganame
    , date_from_parts(file_year, file_month, 1)::date AS file_date
    FROM (
        SELECT 
        *
        , UPPER(TRIM(split_part(split_part(host_location, ',', 1), '-', 1))) AS host_suburb
        , TRIM(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(split_part(split_part(neighbourhood, ',', 1), '/', 1)), 
            'COUNCIL', ''), 'CITY OF', ''), 'OF THE', ''), 'SAINT', 'ST'))::VARCHAR AS neighbourhood_suburb
        , split_part(filename, '_', 1)::INTEGER AS file_month
        , split_part(split_part(filename, '.', 1), '_', 2)::INTEGER AS file_year
        , YEAR(last_scraped) AS listing_year
        , MONTH(last_scraped) AS listing_month
        FROM staging.staging_listing
        WHERE price IS NOT NULL AND host_id IS NOT NULL 
    ) f
    LEFT JOIN staging.staging_location s1
    ON f.neighbourhood_suburb = s1.suburb_name
    LEFT JOIN staging.staging_location s2
    ON f.host_suburb = s2.suburb_name
) 
SELECT 
f.*
, s1.lga_code AS neighbourhood_lga_code
, s2.lga_code AS host_lga_code
FROM (
    SELECT 
    f.*
    , CASE
        WHEN f.neighbourhood_cleansed_raw IS NULL THEN 'OTHER' 
        ELSE f.neighbourhood_cleansed_raw
    END AS neighbourhood_cleansed
    , CASE WHEN neighbourhood_lganame IS NULL THEN
        CASE
            WHEN f.neighbourhood_suburb = 'AVALON' 
                OR f.neighbourhood_suburb = 'BILGOLA' 
                OR f.neighbourhood_suburb = 'COLLAROY BEACH' 
                OR f.neighbourhood_suburb = 'DEE WHY BEACH' 
                OR f.neighbourhood_suburb = 'GREAT MACKERAL BEACH' 
                OR f.neighbourhood_suburb = 'DEE WHY BEACH' 
                OR f.neighbourhood_suburb = 'MANLY BEACH'
                OR f.neighbourhood_suburb = 'MANLY BEACON HILL'
                OR f.neighbourhood_suburb = 'NEWPORT BEACH'
                OR f.neighbourhood_suburb like 'NORTH CURL CURL%' 
                OR f.neighbourhood_suburb = 'NORTH NORTH CURL CURL' 
                OR f.neighbourhood_suburb = 'NORTHERN BEACHES' 
                OR f.neighbourhood_suburb = 'WARRIEWOOD BEACH' 
                THEN 'NORTHERN BEACHES' 
            WHEN f.neighbourhood_suburb = 'BALMORAL BEACH' THEN 'MOSMAN'     
            WHEN f.neighbourhood_suburb = 'BARPOINT' THEN 'CENTRAL COAST'  
            WHEN f.neighbourhood_suburb = 'BEACONSFIED' THEN 'SYDNEY'     
            WHEN f.neighbourhood_suburb = 'BEROWRA CREEK' 
                OR f.neighbourhood_suburb = 'SYDNEY BEROWRA HEIGHTS' 
                THEN 'HORNSBY'  
            WHEN f.neighbourhood_suburb = 'BONDI JUNCTION SYDNEY' THEN 'WAVERLEY' 
            WHEN f.neighbourhood_suburb = 'BRIGHTON LE SANDS' THEN 'BAYSIDE' 
            WHEN f.neighbourhood_suburb like '%DARLING HARBOUR' 
                OR f.neighbourhood_suburb = 'DARLINGHURST SYDNEY' 
                OR f.neighbourhood_suburb = 'KINGS CROSS'
                OR f.neighbourhood_suburb = 'PORT JACKSON'
                OR f.neighbourhood_suburb = 'SYDNEY HARBOUR'
                OR f.neighbourhood_suburb = 'SYNDEY'
                OR f.neighbourhood_suburb = 'РЕДФЕРН'
                OR f.neighbourhood_suburb = '悉尼'
                THEN 'SYDNEY' 
            WHEN f.neighbourhood_suburb = 'HURSTVILLE SYDNEY' THEN 'GEORGES RIVER' 
            WHEN f.neighbourhood_suburb = 'KENSIGNTON' 
                OR f.neighbourhood_suburb = 'MAROUBRA BEACH' 
                OR f.neighbourhood_suburb = 'MAROUBRA JUNCTION' 
                OR f.neighbourhood_suburb = '悉尼' 
                THEN 'RANDWICK' 
            WHEN f.neighbourhood_suburb = 'LIDCOMBE -SYDNEY' THEN 'PARRAMATTA' 
            WHEN f.neighbourhood_suburb = 'MANAHAN' THEN 'CANTERBURY-BANKSTOWN' 
            WHEN f.neighbourhood_suburb = 'MOSMAN SYDNEY' THEN 'MOSMAN'
            WHEN f.neighbourhood_suburb = 'NSW 2065 AUSTRALIA' THEN 'WILLOUGHBY'
            WHEN f.neighbourhood_suburb = 'ROCKDALE CITY' 
                OR f.neighbourhood_suburb = '石谷市' 
                THEN 'BAYSIDE'
            WHEN f.neighbourhood_suburb = 'TOONGABBIE EAST' THEN 'BLACKTOWN'
            WHEN f.neighbourhood_suburb = '스트라스필드' THEN 'STRATHFIELD' 
            WHEN f.neighbourhood_suburb IS NULL THEN 'MISSING'   
            ELSE 'OTHER' 
        END
    ELSE neighbourhood_lganame            
    END AS neighbourhood_lga
    , CASE WHEN host_lganame IS NULL THEN
        CASE 
            WHEN f.host_suburb= 'AVALON' THEN 'NORTHERN BEACHES' 
            WHEN f.host_suburb= 'BELA VISTA' THEN 'THE HILLS SHIRE' 
            WHEN f.host_suburb= 'BEVERLY PARK' THEN 'GEORGES RIVER' 
            WHEN f.host_suburb= 'CENTRAL BUSINESS DISTRICT' THEN 'SYDNEY' 
            WHEN f.host_suburb= 'DECEYVILLE' THEN 'BAYSIDE' 
            WHEN f.host_suburb IS NULL THEN 'MISSING'
            ELSE 'OTHER' 
        END
    ELSE host_lganame
    END AS host_lga
    FROM CTE f
    WHERE last_scraped >= file_date AND last_scraped <= last_day(file_date, 'MONTH')
) f
LEFT JOIN (SELECT DISTINCT lga_name, lga_code FROM staging.staging_location) s1
ON f.neighbourhood_lga = s1.lga_name
LEFT JOIN (SELECT DISTINCT lga_name, lga_code FROM staging.staging_location) s2
ON f.host_lga = s2.lga_name
;
"""


####################################################################################
#
#  Query for KPI
#
####################################################################################
# -- 1. Per “neighbourhood_lga” and “month/year”:
# -- Note: 'neighbourhood_cleansed' include some error.
# 'neighbourhood_lga' (cleaned from neighborhood information) would be used instead for this KPI

query_datamart_kpi1= f"""
CREATE OR REPLACE VIEW datamart.kpi_neighbourhood_month AS
WITH CTE AS
(
    -- total listing
    SELECT 
    neighbourhood_lga
    , listing_year
    , listing_month
    -- Active listings
    , COUNT(*) AS total_listing
    -- Number of distinct hosts
    , COUNT(DISTINCT host_id) AS distinct_hosts
    FROM datawarehouse.fact_listing
    GROUP BY neighbourhood_lga, listing_year, listing_month
) 
SELECT 
t.neighbourhood_lga
, t.listing_year
, t.listing_month
-- Active listings rate
, ((a.total_active_listings / t.total_listing) * 100)::DECIMAL(10,2) AS active_listing_rate
-- Minimum, maximum, median and average price for active listings
, a.min_price
, a.max_price
, a.med_price
, a.avg_price
-- Number of distinct hosts
, t.distinct_hosts
-- Superhost rate 
, ((s.super_distinct_hosts / t.distinct_hosts) * 100)::DECIMAL(10,2) AS superhost_rate
-- Average of review_scores_rating for active listings
, a.avg_review_scores_rating
-- Percentage change for active listings
, (100 * (a.total_active_listings - a.original_total_active_listings) / 
    a.original_total_active_listings)::DECIMAL(10,2) AS percentage_change_active_listings
-- Percentage change for inactive listings
, (100 * (i.total_inactive_listings - i.original_total_inactive_listings) / 
    i.original_total_inactive_listings)::DECIMAL(10,2) AS percentage_change_inactive_listings
-- Number of stays 
, a.avg_number_stays 
, a.total_number_stays
-- Estimated revenue per active listings
, a.avg_estimated_revenue_per_active_listings
, a.total_estimated_revenue_active_listings
, a.avg_estimated_revenue_per_active_listings_per_host
FROM CTE t
-- join active listing
FULL JOIN (
    SELECT 
    neighbourhood_lga
    , listing_year
    , listing_month
    -- Active listings
    , COUNT(*) AS total_active_listings
    -- Minimum, maximum, median and average price for active listings
    , MIN(price) AS min_price
    , MAX(price) AS max_price
    , percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS med_price
    , AVG(price)::DECIMAL(10,2) AS avg_price
    -- Average of review_scores_rating for active listings
    , AVG(review_scores_rating)::DECIMAL(10,0) AS avg_review_scores_rating
    -- Percentage change for active listings
    , LAG(COUNT(*)) OVER (PARTITION BY neighbourhood_lga ORDER BY listing_year, listing_month) 
        AS original_total_active_listings
    -- Number of stays: (only for active listings) = 30 - availability_30 
    , AVG(30 - availability_30)::DECIMAL(10,0) AS avg_number_stays 
    , SUM(30 - availability_30) AS total_number_stays
    -- Estimated revenue per active listings
    , AVG((30 - availability_30)*price)::DECIMAL(10,2) AS avg_estimated_revenue_per_active_listings
    , SUM((30 - availability_30)*price)::DECIMAL(10,2) AS total_estimated_revenue_active_listings
    , (SUM((30 - availability_30)*price)/ COUNT(DISTINCT host_id))::DECIMAL(10,2) 
        AS avg_estimated_revenue_per_active_listings_per_host
    FROM datawarehouse.fact_listing 
    WHERE has_availability = 't'
    GROUP BY neighbourhood_lga, listing_year, listing_month
) a
ON a.neighbourhood_lga = t.neighbourhood_lga
AND a.listing_year = t.listing_year 
AND a.listing_month = t.listing_month 
-- join superhost listing --
FULL JOIN (
    SELECT 
    neighbourhood_lga
    , listing_year AS listing_year
    , listing_month AS listing_month
    -- number of superhost
    , COUNT(DISTINCT host_id) AS super_distinct_hosts
    FROM datawarehouse.fact_listing
    WHERE host_is_superhost = 't'
    GROUP BY neighbourhood_lga, listing_year, listing_month
) s
ON s.neighbourhood_lga = t.neighbourhood_lga
AND s.listing_year = t.listing_year 
AND s.listing_month = t.listing_month 
---------------------------
-- join inactive listing --
FULL JOIN (
    SELECT 
    neighbourhood_lga
    , listing_year AS listing_year
    , listing_month AS listing_month
    -- number of inactive listings
    , COUNT(*) AS total_inactive_listings
    -- number of inactive listings last month
    , LAG(COUNT(*)) OVER (PARTITION BY neighbourhood_lga 
        ORDER BY listing_year, listing_month) AS original_total_inactive_listings
    FROM datawarehouse.fact_listing
    WHERE has_availability = 'f'
    GROUP BY neighbourhood_lga, listing_year, listing_month
) i
ON i.neighbourhood_lga = t.neighbourhood_lga
AND i.listing_year = t.listing_year 
AND i.listing_month = t.listing_month 
ORDER BY neighbourhood_lga, listing_year, listing_month
;
"""


# -- below is only used to compare
# -- Per “neighbourhood_cleansed” and “month/year”:
query_datamart_kpi1_raw= f"""
CREATE OR REPLACE VIEW datamart.kpi_neighbourhood_month_raw AS
WITH CTE AS
(
    -- total listing
    SELECT 
    neighbourhood_cleansed
    , listing_year
    , listing_month
    -- Active listings
    , COUNT(*) AS total_listing
    -- Number of distinct hosts
    , COUNT(DISTINCT host_id) AS distinct_hosts
    FROM datawarehouse.fact_listing
    GROUP BY neighbourhood_cleansed, listing_year, listing_month
) 
SELECT 
t.neighbourhood_cleansed
, t.listing_year
, t.listing_month
-- Active listings rate
, ((a.total_active_listings / t.total_listing) * 100)::DECIMAL(10,2) AS active_listing_rate
-- Minimum, maximum, median and average price for active listings
, a.min_price
, a.max_price
, a.med_price
, a.avg_price
-- Number of distinct hosts
, t.distinct_hosts
-- Superhost rate 
, ((s.super_distinct_hosts / t.distinct_hosts) * 100)::DECIMAL(10,2) AS superhost_rate
-- Average of review_scores_rating for active listings
, a.avg_review_scores_rating
-- Percentage change for active listings
, (100 * (a.total_active_listings - a.original_total_active_listings) / 
    a.original_total_active_listings)::DECIMAL(10,2) AS percentage_change_active_listings
-- Percentage change for inactive listings
, (100 * (i.total_inactive_listings - i.original_total_inactive_listings) / 
    i.original_total_inactive_listings)::DECIMAL(10,2) AS percentage_change_inactive_listings
-- Number of stays 
, a.avg_number_stays 
, a.total_number_stays
-- Estimated revenue per active listings
, a.avg_estimated_revenue_per_active_listings
, a.total_estimated_revenue_active_listings
, a.avg_estimated_revenue_per_active_listings_per_host
FROM CTE t
-- join active listing
FULL JOIN (
    SELECT 
    neighbourhood_cleansed
    , listing_year
    , listing_month
    -- Active listings
    , COUNT(*) AS total_active_listings
    -- Minimum, maximum, median and average price for active listings
    , MIN(price) AS min_price
    , MAX(price) AS max_price
    , PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS med_price
    , AVG(price)::DECIMAL(10,2) AS avg_price
    -- Average of review_scores_rating for active listings
    , AVG(review_scores_rating)::DECIMAL(10,0) AS avg_review_scores_rating
    -- Percentage change for active listings
    , LAG(COUNT(*)) OVER (PARTITION BY neighbourhood_cleansed 
        ORDER BY listing_year, listing_month) AS original_total_active_listings
    -- Number of stays: (only for active listings) = 30 - availability_30 
    , AVG(30 - availability_30)::DECIMAL(10,0) AS avg_number_stays 
    , SUM(30 - availability_30) AS total_number_stays
    -- Estimated revenue per active listings
    , AVG((30 - availability_30)*price)::DECIMAL(10,2) AS avg_estimated_revenue_per_active_listings
    , SUM((30 - availability_30)*price)::DECIMAL(10,2) AS total_estimated_revenue_active_listings
    , (SUM((30 - availability_30)*price)/ COUNT(DISTINCT host_id))::DECIMAL(10,2) 
    AS avg_estimated_revenue_per_active_listings_per_host
    FROM datawarehouse.fact_listing 
    WHERE has_availability = 't'
    GROUP BY neighbourhood_cleansed, listing_year, listing_month
) a
ON a.neighbourhood_cleansed = t.neighbourhood_cleansed
AND a.listing_year = t.listing_year 
AND a.listing_month = t.listing_month 
-- join superhost listing --
FULL JOIN (
    SELECT 
    neighbourhood_cleansed
    , listing_year AS listing_year
    , listing_month AS listing_month
    -- number of superhost
    , COUNT(DISTINCT host_id) AS super_distinct_hosts
    FROM datawarehouse.fact_listing
    WHERE host_is_superhost = 't'
    GROUP BY neighbourhood_cleansed, listing_year, listing_month
) s
ON s.neighbourhood_cleansed = t.neighbourhood_cleansed
AND s.listing_year = t.listing_year 
AND s.listing_month = t.listing_month 
---------------------------
-- join inactive listing --
FULL JOIN (
    SELECT 
    neighbourhood_cleansed
    , listing_year AS listing_year
    , listing_month AS listing_month
    -- number of inactive listings
    , COUNT(*) AS total_inactive_listings
    -- number of inactive listings last month
    , LAG(count(*)) OVER (PARTITION BY neighbourhood_cleansed 
        ORDER BY listing_year, listing_month) AS original_total_inactive_listings
    FROM datawarehouse.fact_listing
    WHERE has_availability = 'f'
    GROUP BY neighbourhood_cleansed, listing_year, listing_month
) i
ON i.neighbourhood_cleansed = t.neighbourhood_cleansed
AND i.listing_year = t.listing_year 
AND i.listing_month = t.listing_month 
ORDER BY neighbourhood_cleansed, listing_year, listing_month
;
"""

# -- 2. Per “property_type”, “room_type” ,“accommodates” and “month/year”:
query_datamart_kpi2= f"""
CREATE OR REPLACE VIEW datamart.kpi_property_month AS
WITH CTE AS
(
    -- total listing
    SELECT 
    property_type
    , room_type
    , accommodates
    , listing_year
    , listing_month
    -- Active listings
    , COUNT(*) AS total_listing
    -- Number of distinct hosts
    , COUNT(DISTINCT host_id) as distinct_hosts
    FROM datawarehouse.fact_listing
    GROUP BY property_type, room_type, accommodates, listing_year, listing_month
)
SELECT 
t.property_type
, t.room_type
, t.accommodates
, t.listing_year
, t.listing_month
-- Active listings rate
, ((a.total_active_listings / t.total_listing) * 100)::DECIMAL(10,2) AS active_listing_rate
-- Minimum, maximum, median and average price for active listings
, a.min_price
, a.max_price
, a.med_price
, a.avg_price
-- Number of distinct hosts
, t.distinct_hosts
-- Superhost rate 
, ((s.super_distinct_hosts / t.distinct_hosts) * 100)::DECIMAL(10,2) AS superhost_rate
-- Average of review_scores_rating for active listings
, a.avg_review_scores_rating
-- Percentage change for active listings
, (100 * (a.total_active_listings - a.original_total_active_listings) / 
    a.original_total_active_listings)::DECIMAL(10,2) AS percentage_change_active_listings
-- Percentage change for inactive listings
, (100 * (i.total_inactive_listings - i.original_total_inactive_listings) / 
    i.original_total_inactive_listings)::DECIMAL(10,2) AS percentage_change_inactive_listing
-- Number of stays 
, a.avg_number_stays 
, a.total_number_stays
-- Estimated revenue per active listings
, a.avg_estimated_revenue_per_active_listings
, a.total_estimated_revenue_active_listings
, a.avg_estimated_revenue_per_active_listings_per_host

FROM CTE t
----------------------
-- join active listing --
FULL JOIN (
-- active listing
    SELECT 
    property_type
    , room_type
    , accommodates
    , listing_year AS listing_year
    , listing_month AS listing_month
    -- Active listings
    , COUNT(*) AS total_active_listings
    -- Minimum, maximum, median and average price for active listings
    , MIN(price) AS min_price
    , MAX(price) AS max_price
    , percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS med_price
    , AVG(price)::DECIMAL(10,2) AS avg_price
    -- Average of review_scores_rating for active listings
    , AVG(review_scores_rating)::DECIMAL(10,0) AS avg_review_scores_rating
    -- Percentage change for active listings
    , LAG(COUNT(*)) OVER (PARTITION BY property_type, room_type, accommodates 
        ORDER BY listing_year, listing_month) AS original_total_active_listings
    -- Number of stays: (only for active listings) = 30 - availability_30 
    , AVG(30 - availability_30)::DECIMAL(10,0) AS avg_number_stays 
    , SUM(30 - availability_30) AS total_number_stays
    -- Estimated revenue per active listings
    , AVG((30 - availability_30)*price)::DECIMAL(10,2) AS avg_estimated_revenue_per_active_listings
    , SUM((30 - availability_30)*price)::DECIMAL(10,2) AS total_estimated_revenue_active_listings
    , (SUM((30 - availability_30)*price)/ COUNT(DISTINCT host_id))::DECIMAL(10,2) 
        AS avg_estimated_revenue_per_active_listings_per_host
    FROM datawarehouse.fact_listing
    WHERE has_availability = 't'
    GROUP BY property_type, room_type, accommodates, listing_year, listing_month
) a
ON a.property_type = t.property_type
AND a.room_type = t.room_type
AND a.accommodates = t.accommodates
AND a.listing_year = t.listing_year 
AND a.listing_month = t.listing_month 
----------------------------
-- join superhost listing --
FULL JOIN (
    SELECT 
    property_type
    , room_type
    , accommodates
    , listing_year AS listing_year
    , listing_month AS listing_month
    -- number of superhost
    , COUNT(DISTINCT host_id) AS super_distinct_hosts
    FROM datawarehouse.fact_listing
    WHERE host_is_superhost = 't'
    GROUP BY property_type, room_type, accommodates, listing_year, listing_month
) s
ON s.property_type = t.property_type
AND s.room_type = t.room_type
AND s.accommodates = t.accommodates
AND s.listing_year = t.listing_year 
AND s.listing_month = t.listing_month 
---------------------------
-- join inactive listing --
FULL JOIN (
    SELECT 
    property_type
    , room_type
    , accommodates
    , listing_year AS listing_year
    , listing_month AS listing_month
    -- number of inactive listings
    , COUNT(*) AS total_inactive_listings
    -- number of inactive listings last month
    , LAG(COUNT(*)) OVER (PARTITION BY property_type, room_type, accommodates 
        ORDER BY listing_year, listing_month) AS original_total_inactive_listings
    FROM datawarehouse.fact_listing
    WHERE has_availability = 'f'
    GROUP BY property_type, room_type, accommodates, listing_year, listing_month
) i
ON i.property_type = t.property_type
AND i.room_type = t.room_type
AND i.accommodates = t.accommodates
AND i.listing_year = t.listing_year 
AND i.listing_month = t.listing_month 
ORDER BY property_type, room_type, accommodates, listing_year, listing_month
;
"""

# -- 3. Per “host_neighbourhood” and “month/year”:
query_datamart_kpi3= f"""
CREATE OR REPLACE VIEW datamart.kpi_host_neighbourhood_month AS
WITH CTE AS
( 
    SELECT 
    host_lga
    , listing_year
    , listing_month
    -- Number of distinct host
    , COUNT(DISTINCT host_id) AS distinct_count
    FROM datawarehouse.fact_listing
    GROUP BY host_lga, listing_year, listing_month
) 
SELECT 
t.*
, total_estimated_revenue_listings
, avg_estimated_revenue_per_listings
, estimated_revenue_per_host
FROM CTE t
FULL JOIN (
    -- join active listing --
    SELECT 
    host_lga
    , listing_year
    , listing_month
    -- Estimated Revenue
    , SUM((30 - availability_30)*price)::DECIMAL(10,2) AS total_estimated_revenue_listings
    , AVG((30 - availability_30)*price)::DECIMAL(10,2) AS avg_estimated_revenue_per_listings
    -- Estimated Revenue per host (distinct)
    , (SUM((30 - availability_30)*price) / COUNT(DISTINCT host_id))::DECIMAL(10,2) 
       AS estimated_revenue_per_host
    FROM datawarehouse.fact_listing
    WHERE has_availability = 't'
    GROUP BY host_lga, listing_year, listing_month
) a
ON a.host_lga = t.host_lga
AND a.listing_year = t.listing_year 
AND a.listing_month = t.listing_month 
ORDER BY host_lga, listing_year, listing_month
; 
"""

####################################################################################
#
#  ELT Operators Setup
#
####################################################################################

# for census data
refresh_raw_census = SnowflakeOperator(
    task_id='refresh_raw_census_task',
    sql=query_refresh_raw_census,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

get_staging_census = SnowflakeOperator(
    task_id='get_staging_census_task',
    sql=query_get_staging_census,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

get_dim_census = SnowflakeOperator(
    task_id='get_dim_census_task',
    sql=query_get_dim_census,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

# for location data
refresh_raw_location = SnowflakeOperator(
    task_id='refresh_raw_location_task',
    sql=query_refresh_raw_location,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

get_staging_location = SnowflakeOperator(
    task_id='get_staging_location_task',
    sql=query_get_staging_location,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

# for lisitng data
refresh_raw_listing = SnowflakeOperator(
    task_id='refresh_raw_listing_task',
    sql=query_refresh_raw_listing,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

get_staging_listing = SnowflakeOperator(
    task_id='get_staging_listing_task',
    sql=query_get_staging_listing,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

get_fact_listing = SnowflakeOperator(
    task_id='get_fact_listing_task',
    sql=query_get_fact_listing,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)


#  KPI
get_datamart_kpi1 = SnowflakeOperator(
    task_id='datamart_kpi1_task',
    sql=query_datamart_kpi1,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

get_datamart_kpi1_raw = SnowflakeOperator(
    task_id='datamart_kpi1_raw_task',
    sql=query_datamart_kpi1_raw,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

get_datamart_kpi2 = SnowflakeOperator(
    task_id='datamart_kpi2_task',
    sql=query_datamart_kpi2,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

get_datamart_kpi3 = SnowflakeOperator(
    task_id='datamart_kpi3_task',
    sql=query_datamart_kpi3,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)



refresh_raw_census >> get_staging_census >> get_dim_census
refresh_raw_location >> get_staging_location >> get_fact_listing
refresh_raw_listing >> get_staging_listing >> get_fact_listing
get_fact_listing >> get_datamart_kpi1
get_fact_listing >> get_datamart_kpi1_raw
get_fact_listing >> get_datamart_kpi2
get_fact_listing >> get_datamart_kpi3
