# lego_data_engineering
This repo will demonstrate a simple end to end data engineering pipeline using lego dataset from [kaggle](https://www.kaggle.com/datasets/rtatman/lego-database).

## 1. Download csv file from kaggle and insert to Google BigQuery
[scripts](https://github.com/zhafar3adib/lego_data_engineering/tree/main/scripts) folder contains a python file that will download data from kaggle and insert it into Google BigQuery as a table. The default schema from kaggle looks like this 

![lego dataset default schema](https://github.com/zhafar3adib/lego_data_engineering/blob/main/images/kaggle_lego_dataset_schema.png) 

## 2. Transform data with dbt
The inserted table into BigQuery is just a raw layer/schema, in [dbt](https://github.com/zhafar3adib/lego_data_engineering/tree/main/dbt) folder we build the staging, model, and mart schema.
- staging : fix column type from raw schema
- model : table fact and table dimension also test every column , the relation looks like this
  ![model schema](https://github.com/zhafar3adib/lego_data_engineering/blob/main/images/lego_data_engineering_schema_model.jpg)
- marts : aggregation table from model schema, and connected to [looker's](https://lookerstudio.google.com/reporting/05ed5d52-8261-47db-bfdd-01a2c9c03d72) dashboard. it's divided between lego sets and parts
  
  sets dashboard
  ![dashboard sets](https://github.com/zhafar3adib/lego_data_engineering/blob/main/images/dashboard_sets.png)
  parts dashboard
  ![dashboard parts](https://github.com/zhafar3adib/lego_data_engineering/blob/main/images/dashboard_parts.png)

## 3. Data orchestration using airflow
Dockerize airflow to build data orchestration and notify when sucess or failed via discord's server. The setup folder in [airflow](https://github.com/zhafar3adib/lego_data_engineering/tree/main/airflow) folder, [dags](https://github.com/zhafar3adib/lego_data_engineering/tree/main/dags) folder, and [docker-compose.yaml](https://github.com/zhafar3adib/lego_data_engineering/blob/main/docker-compose.yaml)

I hope this repository will help you or just find a reference to create a similar program.
