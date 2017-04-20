create table mitfile(file_id varchar(256), disk_id varchar(256), file_version integer, obs_id varchar(20), host_id varchar(30), file_path varchar(256), compression_date varchar(23), status integer, comment varchar(256));
create index host_file_path on mitfile(host_id, file_path)
.separator ","
.import mit_compress.csv mitfile
