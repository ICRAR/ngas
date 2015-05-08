CREATE ROLE ngas LOGIN PASSWORD 'ngas$dba';

CREATE TABLESPACE ngas OWNER ngas LOCATION '/Users/chen/proj/mwa/testNGAS/NGAS/pgtbspace';
CREATE DATABASE ngas OWNER ngas TABLESPACE ngas;

GRANT ALL PRIVILEGES ON DATABASE ngas TO ngas;
CREATE SCHEMA ngas AUTHORIZATION ngas;

CREATE ROLE ngas_ro LOGIN PASSWORD 'ngas$ro';
GRANT SELECT ON TABLE ngas_disks TO ngas_ro;
GRANT SELECT ON TABLE ngas_disks_hist TO ngas_ro;
GRANT SELECT ON TABLE ngas_files TO ngas_ro;
GRANT SELECT ON TABLE ngas_hosts TO ngas_ro;
GRANT SELECT ON TABLE ngas_subscr_back_log TO ngas_ro;
GRANT SELECT ON TABLE ngas_subscribers TO ngas_ro;




