CREATE TABLE ngas_containers
(
  container_id        VARCHAR(36)    NOT NULL,
  parent_container_id VARCHAR(36)    NULL,
  container_name      VARCHAR(255)   NOT NULL,
  ingestion_date      VARCHAR(23)    NULL,
  container_size      NUMERIC(20, 0) NOT NULL,
  container_type      VARCHAR(10)    NOT NULL,
  CONSTRAINT container_idx PRIMARY KEY(container_id),
  CONSTRAINT container_uni UNIQUE(parent_container_id, container_name),
  CONSTRAINT container_parent FOREIGN KEY (parent_container_id) REFERENCES ngas_containers(container_id)
);

ALTER TABLE ngas_files ADD COLUMN container_id   varchar(36)    NULL;
ALTER TABLE ngas_files ADD CONSTRAINT file_container FOREIGN KEY (container_id) REFERENCES ngas_containers(container_id);
