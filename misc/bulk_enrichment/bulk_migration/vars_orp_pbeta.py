AWS_REGION = "eu-west-2"
DB_IP = "localhost"
DB_NAME = "orp-pbeta-v2"
SCHEMA_TQL = "schema/orp-pbeta-gdb-schema.tql"
SCHEMA_JSON = "schema/orp-gdb-schema.json"
LOGFILE = "logs/bulk_ingestion.log"
DIR_PATH = "data/processed/"
DATA_PATH='../glue_jobs/data/glue/'

import logging


handler = logging.FileHandler(LOGFILE)        
handler.setFormatter(logging.Formatter("%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s"))

logger = logging.getLogger("Bulk_Migrator")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)


logging.getLogger().addHandler(logging.StreamHandler())
