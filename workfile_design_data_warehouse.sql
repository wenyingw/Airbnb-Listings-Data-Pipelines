-- Part 2
-- a. Design and populate a data warehouse following an ELT pattern
-- Design the architecture of a data warehouse on Snowflake from the input datasets (layer to handle raw data, layer to transform data into star schema) using two months of listings and the census dataset (be careful of SCDs and choose a type to cater for it) .

-- 1. presetting and creating connection
-- [1.1] Create a new database and use
CREATE OR REPLACE DATABASE airbnb;

USE DATABASE airbnb;


-- [1.2] Create a schema and use
CREATE OR REPLACE SCHEMA raw;
USE SCHEMA airbnb.raw;


-- [1.3] Create a storage integration 
CREATE OR REPLACE STORAGE INTEGRATION GCP
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = GCS
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('gcs://australia-southeast1-bde-5debecee-bucket/dags/data');

DESCRIBE INTEGRATION GCP;


-- [1.4] create a stage
CREATE OR REPLACE STAGE stage_gcp
STORAGE_INTEGRATION = GCP
URL='gcs://australia-southeast1-bde-5debecee-bucket/dags/data'
;

-- [1.5] list all the files inside your stage:
LIST @stage_gcp;


-- [1.6] Create a file format called file_format_csv:
CREATE OR REPLACE FILE FORMAT file_format_csv 
TYPE = 'CSV' 
FIELD_DELIMITER = ',' 
SKIP_HEADER = 1
NULL_IF = ('\\N', 'NULL', 'NUL', '')
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
;


-- 2. Get column name 
-- [2.1] get the columns name of censusG01 and censusG01
CREATE OR REPLACE EXTERNAL TABLE censusG01_columns_name
WITH LOCATION = @stage_gcp
FILE_FORMAT = (TYPE=CSV)
PATTERN = '.*G01.*[.]csv';

SELECT * 
FROM censusG01_columns_name
LIMIT 1;


CREATE OR REPLACE EXTERNAL TABLE censusG02_columns_name
WITH LOCATION = @stage_gcp
FILE_FORMAT = (TYPE=CSV)
PATTERN = '.*G02.*[.]csv';

SELECT * 
FROM censusG02_columns_name
LIMIT 1;

-- [2.2] get the columns name of lga data
CREATE OR REPLACE EXTERNAL TABLE lga_columns_name
WITH LOCATION = @stage_gcp
FILE_FORMAT = (TYPE=CSV)
PATTERN = '.*LGA_2020_NSW.*[.]csv';

SELECT * 
FROM lga_columns_name
LIMIT 1;

CREATE OR REPLACE EXTERNAL TABLE ssc_columns_name
WITH LOCATION = @stage_gcp
FILE_FORMAT = (TYPE=CSV)
PATTERN = '.*SSC.*[.]csv';

SELECT * 
FROM ssc_columns_name
LIMIT 1;


-- [2.3] Get column name for sample listing data
CREATE OR REPLACE EXTERNAL TABLE sample_listings_columns_name
WITH LOCATION = @stage_gcp
FILE_FORMAT = (TYPE=CSV)
PATTERN = '.*04_2021.*[.]csv';
SELECT * 
FROM sample_listings_columns_name
LIMIT 1;



-- 3. create table of raw
-- [3.1] create table of raw listing
CREATE OR REPLACE EXTERNAL TABLE raw.raw_listing
WITH LOCATION = @stage_gcp 
FILE_FORMAT = file_format_csv
PATTERN = '.*listings.*[.]csv';


-- [3.2] create table of raw census data
CREATE OR REPLACE EXTERNAL TABLE raw.raw_censusG01
WITH LOCATION = @stage_gcp 
FILE_FORMAT = file_format_csv
PATTERN = '.*G01.*[.]csv';

CREATE OR REPLACE EXTERNAL TABLE raw.raw_censusG02
WITH LOCATION = @stage_gcp 
FILE_FORMAT = file_format_csv
PATTERN = '.*G02.*[.]csv';

-- [3.3] create table of raw lga
CREATE OR REPLACE EXTERNAL TABLE raw.raw_lga
WITH LOCATION = @stage_gcp 
FILE_FORMAT = file_format_csv
PATTERN = '.*LGA_2020_NSW.*[.]csv';


CREATE OR REPLACE EXTERNAL TABLE raw.raw_ssc
WITH LOCATION = @stage_gcp 
FILE_FORMAT = file_format_csv
PATTERN = '.*SSC.*[.]csv';




-- 4. create table of staging
CREATE OR REPLACE SCHEMA staging;
USE SCHEMA airbnb.staging;



-- [4.1] create table of staging_census
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


-- [4.2] create table of staging_location
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



-- [4.3] create table of staging_lisitng
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



-- 5. create table of datawarehouse

CREATE OR REPLACE SCHEMA datawarehouse;
USE SCHEMA datawarehouse;


-- [5.1] create table for dim_census
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
    
  
-- [5.2] create table for fact_listing
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

        
-- 6. Part 3: Design and populate a data mart:
CREATE OR REPLACE SCHEMA datamart;
USE SCHEMA airbnb.datamart;


-- ALTER EXTERNAL TABLE raw.raw_censusG01 REFRESH;
-- ALTER EXTERNAL TABLE raw.raw_censusG02 REFRESH;
-- ALTER EXTERNAL TABLE raw.raw_lga REFRESH;
-- ALTER EXTERNAL TABLE raw.raw_ssc REFRESH;
-- ALTER EXTERNAL TABLE raw.raw_listing REFRESH;