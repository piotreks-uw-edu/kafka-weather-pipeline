docker run --rm -it \
--name databricks-cli  \
--volume $(pwd -W)/../../databricks_cli:/data/ \
--env-file $(pwd -W)/enviroment/databricks.env \
databricks-cli:latest bash
# 
