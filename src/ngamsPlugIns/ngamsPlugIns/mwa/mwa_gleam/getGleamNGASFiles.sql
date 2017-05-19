.mode csv
.header off
.out gleam_file.csv
select a.file_id, '/mnt/gleam/NGAS/volume1/' || a.file_name from ngas_files a,
(select ngas_files.file_id, MAX(ngas_files.file_version) AS max_ver 
from ngas_files group by ngas_files.file_id) b
where a.file_id = b.file_id and
a.file_version = b.max_ver;

