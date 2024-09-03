from airflow.decorators import dag, task
from datetime import datetime
from astro import sql as aql
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Import Cosmos for DBT Taskgroup in Airflow
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig
from include.snowflake_dbt.cosmos_config import Profile_Config, Project_Config

GCP_CONN_ID = 'gcp'
SNOWFLAKE_CONN_ID = 'snowflake_default'
MY_BUCKET = 'snowflake_staging_area'

@dag(
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False,
    tags=['retail']
)


def retail():

    upload_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_data_to_gcs',
        src = '/usr/local/airflow/include/dataset/Online_Retail.csv',
        dst = 'raw/online_retail_no.csv',
        bucket = MY_BUCKET,
        gcp_conn_id = GCP_CONN_ID,
        gzip = False,
    )

    create_source_table = SnowflakeOperator(
        task_id = 'create_source_table',
        snowflake_conn_id = SNOWFLAKE_CONN_ID,
        warehouse = 'COMPUTE_WH',
        database = 'DBT_DB',
        schema = 'retail',
        role = 'accountadmin',
        sql="""
            CREATE OR REPLACE TABLE raw_invoices (
                InvoiceNo VARCHAR,
                StockCode VARCHAR,
                Description VARCHAR,
                Quantity NUMBER,
                InvoiceDate VARCHAR,
                UnitPrice NUMBER(38, 10),
                CustomerID NUMBER(38, 10),
                Country VARCHAR
            );
        """
    )

    populate_source_table = SnowflakeOperator(
        task_id = 'populate_source_table',
        snowflake_conn_id = SNOWFLAKE_CONN_ID,
        warehouse = 'COMPUTE_WH',
        database = 'DBT_DB',
        schema = 'retail',
        role = 'accountadmin',
        sql="""
            COPY INTO dbt_db.retail.raw_invoices
            FROM @dbt_db.public.gcs_stage/raw/online_retail.csv;      
        """
    )

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load():
        from include.soda.checks.check_function import check
        return check(scan_name='check_load', checks_subpath='check_load')
    # check_load()


    dim_modeling = DbtTaskGroup(
        group_id = 'dim_modeling',
        project_config = Project_Config,
        profile_config = Profile_Config,
        render_config = RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_model():
        from include.soda.checks.check_function import check
        return check(scan_name='check_model', checks_subpath='check_model')
    # check_model()

    create_marts = DbtTaskGroup(
        group_id = 'create_marts',
        profile_config = Profile_Config,
        project_config = Project_Config,
        render_config = RenderConfig(
            load_method = LoadMode.DBT_LS,
            select = ['path:models/marts']
        )
    )

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_marts():
        from include.soda.checks.check_function import check
        return check(scan_name='check_marts', checks_subpath='check_marts')
    # check_marts()

    chain(
        upload_data_to_gcs,
        create_source_table,
        populate_source_table,
        check_load(),
        dim_modeling,
        check_model(),
        create_marts,
        check_marts()
    )


retail()