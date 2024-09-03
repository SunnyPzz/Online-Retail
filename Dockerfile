FROM quay.io/astronomer/astro-runtime:11.8.0

# install soda into a virtual environment
RUN python -m venv soda_venv && source soda_venv/bin/activate && \
    pip install --no-cache-dir soda-core-snowflake==3.0.45 &&\
    pip install --no-cache-dir soda-core-scientific==3.0.45 && deactivate

# Install dbt_snowflake into virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake==1.5.3 && deactivate

# Install soda into a virtual environment
# RUN python -m venv soda_venv && source soda_venv/bin/activate && \
#     pip install --no-cache-dir soda-core-snowflake==3.0.45 && \
#     pip install --no-cache-dir soda-core-scientific==3.0.45 && deactivate

