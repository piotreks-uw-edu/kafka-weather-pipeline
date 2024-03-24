
#!/bin/bash

# Configure Databricks CLI using the token
databricks configure --token

databricks secrets create-scope --scope AIR_IN_EUROPE_SCOPE --scope-backend-type DATABRICKS

databricks secrets put --scope AIR_IN_EUROPE_SCOPE --key primaryConnectionString --string-value $PRIMARY_CONNECTION_STRING
databricks secrets put --scope AIR_IN_EUROPE_SCOPE --key pathCore --string-value $PATH_CORE
databricks secrets put --scope AIR_IN_EUROPE_SCOPE --key kafkaPort --string-value $KAFKA_PORT
databricks secrets put --scope AIR_IN_EUROPE_SCOPE --key azureSqlDatabaseUrl --string-value $AZURE_SQL_DATABASE_URL
databricks secrets put --scope AIR_IN_EUROPE_SCOPE --key azureSqlDatabaseUser --string-value $AZURE_SQL_DATABASE_USER
databricks secrets put --scope AIR_IN_EUROPE_SCOPE --key azureSqlDatabasePassword --string-value $AZURE_SQL_DATABASE_PASSWORD

