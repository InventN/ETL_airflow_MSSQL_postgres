import time
import pandas as pd
from datetime import datetime
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'invent',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@task()
def extract_data(table):
    start_time = time.time()

    sql = f'SELECT * FROM {table}'
    hook = MsSqlHook(mssql_conn_id= "mssql_adven")
    df = hook.get_pandas_df(sql)
    print(f'Done. {str(round(time.time() - start_time, 2))} total seconds elapsed')
    print("Data imported successful")
    return df

#Tranform
@task()
def transform_srcProduct(df):
    #drop columns
    df = df[['ProductKey', 'ProductAlternateKey', 'ProductSubcategoryKey','WeightUnitMeasureCode', 'SizeUnitMeasureCode', 'EnglishProductName',
                   'StandardCost','FinishedGoodsFlag', 'Color', 'SafetyStockLevel', 'ReorderPoint','ListPrice', 'Size', 'SizeRange', 'Weight',
                   'DaysToManufacture','ProductLine', 'DealerPrice', 'Class', 'Style', 'ModelName', 'EnglishDescription', 'StartDate','EndDate', 'Status']]
    #replace nulls
    df['WeightUnitMeasureCode'].fillna('0', inplace=True)
    df['ProductSubcategoryKey'].fillna('0', inplace=True)
    df['SizeUnitMeasureCode'].fillna('0', inplace=True)
    df['StandardCost'].fillna('0', inplace=True)
    df['ListPrice'].fillna('0', inplace=True)
    df['ProductLine'].fillna('NA', inplace=True)
    df['Class'].fillna('NA', inplace=True)
    df['Style'].fillna('NA', inplace=True)
    df['Size'].fillna('NA', inplace=True)
    df['ModelName'].fillna('NA', inplace=True)
    df['EnglishDescription'].fillna('NA', inplace=True)
    df['DealerPrice'].fillna('0', inplace=True)
    df['Weight'].fillna('0', inplace=True)
    # Rename columns with rename function
    df = df.rename(columns={"EnglishDescription": "Description", "EnglishProductName":"ProductName"})
    df['StartDate'] = df['StartDate'].dt.strftime('%Y%m%d%H%M%S')
    df['EndDate'] = df['EndDate'].dt.strftime('%Y%m%d%H%M%S')
    df = df.to_dict()
    return df

#
@task()
def transform_srcProductSubcategory(df):
    #drop columns
    df = df[['ProductSubcategoryKey', 'ProductSubcategoryAlternateKey','EnglishProductSubcategoryName', 'ProductCategoryKey']]
    # Rename columns with rename function
    df = df.rename(columns={"EnglishProductSubcategoryName": "ProductSubcategoryName"})
    return df

@task()
def transform_srcProductCategory(df):
    #drop columns
    df = df[['ProductCategoryKey', 'ProductCategoryAlternateKey','EnglishProductCategoryName']]
    # Rename columns with rename function
    df = df.rename(columns={"EnglishProductCategoryName": "ProductCategoryName"})
    return df

#load
@task()
def load(df_P, df_PS, df_PC):
    conn = BaseHook.get_connection('postgres_localhost')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    start_time = time.time()
    df_P = pd.DataFrame.from_dict(df_P) 
    df_P['ProductSubcategoryKey'] = df_P['ProductSubcategoryKey'].astype(float)
    df_P['ProductSubcategoryKey'] = df_P['ProductSubcategoryKey'].astype(int)
    merged = df_P.merge(df_PS, on='ProductSubcategoryKey').merge(df_PC, on='ProductCategoryKey')
    merged.to_sql(f'prd_DimProductCategory', engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported successful"}

with DAG(
    dag_id='lmao_ver17',
    default_args=default_args,
    start_date=datetime(2023, 10, 14),
    schedule_interval='0 0 * * *'
) as dag:
    extract_DimProduct = extract_data('DimProduct')
    extract_DimProductSubcategory = extract_data('DimProductSubcategory')
    extract_DimProductCategory = extract_data('DimProductCategory')

    tranform_P = transform_srcProduct(extract_DimProduct)
    tranform_PS = transform_srcProductSubcategory(extract_DimProductSubcategory)
    tranform_PC = transform_srcProductCategory(extract_DimProductCategory)

    Load_postgres = load(tranform_P, tranform_PS, tranform_PC)
    # Load_postgres = load_test(tranform_P)

    extract_DimProduct >> tranform_P
    extract_DimProductSubcategory >> tranform_PS
    extract_DimProductCategory >> tranform_PC 
    
    [tranform_P, tranform_PS, tranform_PC]>> Load_postgres
    