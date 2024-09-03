from cosmos.config import ProfileConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

Profile_Config = ProfileConfig(
    profile_name='default',
    target_name='dev',
    profile_mapping = SnowflakeUserPasswordProfileMapping(
        conn_id='snowflake_default',
        profile_args = {'database': 'dbt_db', 'schema': 'retail'}
    )
)

Project_Config = ProjectConfig('/usr/local/airflow/include/snowflake_dbt/')
