FROM  ghcr.io/dbt-labs/dbt-snowflake:1.7.1
COPY  ./container-services-dbt/app/ /usr/app/
WORKDIR  /usr/app