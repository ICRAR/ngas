ALTER TABLE ngas_files ADD COLUMN io_time        numeric(20, 0) DEFAULT -1;
ALTER TABLE ngas_files ADD COLUMN ingestion_rate numeric(20, 0) NULL DEFAULT -1;
