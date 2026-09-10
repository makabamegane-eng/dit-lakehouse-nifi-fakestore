-- =============================================================================
--  Bases de métadonnées de la plateforme.
--  Un seul serveur PostgreSQL, trois bases logiques isolées :
--    * airflow : métadonnées d'orchestration (DAG runs, task instances, XCom)
--    * nessie  : version store JDBC du catalogue Iceberg (commits, refs)
--    * dremio  : réservée à un usage futur (Dremio OSS embarque son propre
--                KV store RocksDB, mais la base est provisionnée pour rester
--                cohérent avec la pile imposée « PostgreSQL = métadonnées »)
-- =============================================================================

CREATE DATABASE airflow;
CREATE DATABASE nessie;
CREATE DATABASE dremio;

GRANT ALL PRIVILEGES ON DATABASE airflow TO lakehouse;
GRANT ALL PRIVILEGES ON DATABASE nessie  TO lakehouse;
GRANT ALL PRIVILEGES ON DATABASE dremio  TO lakehouse;
