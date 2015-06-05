ALTER TABLE ngas_files ADD COLUMN ingestion_rate numeric(20, 0) NULL DEFAULT -1;
ALTER TABLE ngas_files ADD COLUMN container_id   varchar(36)    NULL;

CREATE TABLE ngas_containers
(
  container_id        VARCHAR(36)    NOT NULL,
  parent_container_id VARCHAR(36)    NULL,
  container_name      VARCHAR(255)   NOT NULL,
  ingestion_date      VARCHAR(23)    NULL,
  container_size      NUMERIC(20, 0) NOT NULL,
  container_type      VARCHAR(10)    NOT NULL,
  CONSTRAINT container_idx PRIMARY KEY(container_id),
  CONSTRAINT container_uni UNIQUE(parent_container_id, container_name)
);
