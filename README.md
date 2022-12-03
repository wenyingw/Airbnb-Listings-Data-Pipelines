# Airbnb Listings Data Pipelines

### Overview:
This project is to build production-ready ELT pipelines using Airflow for Airbnb listings. The ELT pipeline includes processing and cleaning two provided datasets and loading the data into a data warehouse and data mart for analysis.

The ELT pipeline will be built by python scripts, SQL scripts, Airflow, GCP Cloud Composer, and Snowflake. The raw data will be extracted from the provided CSV files, loaded into the Snowflake database, then transformed into star schema in the data warehouse. After that, a datamart will be designed and populated through an ETL pipeline from the data warehouse.


### Output:
- [Written Report](https://github.com/wenyingw/Airbnb-Listings-Data-Pipelines/blob/main/report_airbnb_listings_data_pipelines.pdf)

- [Workfile - Data Exploration](https://github.com/wenyingw/Airbnb-Listings-Data-Pipelines/blob/main/workfile_preprocess.ipynb)

- [Workfile - Design a data warehouse](https://github.com/wenyingw/Airbnb-Listings-Data-Pipelines/blob/main/workfile_design_data_warehouse.sql) 

- [Workfile - Populate the data warehouse](https://github.com/wenyingw/Airbnb-Listings-Data-Pipelines/blob/main/workfile_populate_data_warehouse.py) 

- [Workfile - Ad-hoc analysis](https://github.com/wenyingw/Airbnb-Listings-Data-Pipelines/blob/main/workfile_ad-hoc_analysis.sql)


<sub><sup>Edit on May 17, 2020</sup></sub>