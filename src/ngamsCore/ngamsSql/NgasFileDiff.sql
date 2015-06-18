select 'FE34_to_FE12', a.file_id, a.file_version, a.disk_id, a.file_full_path, a.ingestion_date, a.format, -2, '2014-03-28T22:22:49.245', 'created' 
from
(select ngas_files.file_id, ngas_files.file_version, ngas_files.disk_id, ngas_disks.mount_point || '/' || ngas_files.file_name file_full_path, ngas_files.ingestion_date, ngas_files.format
 from ngas_files, ngas_disks 
 where ngas_files.disk_id = ngas_disks.disk_id and 
 ngas_disks.host_id in ('fe3:7777', 'fe4:7777')) 
a 
left outer join
(select file_id from ngas_files where disk_id in ('921d259d7bc2a0ae7d9a532bccd049c7', '54ab8af6c805f956c804ee1e4de92ca4', 'e3d87c5bc9fa1f17a84491d03b732afd', '35ecaa0a7c65795635087af61c3ce903')) 
b
on (a.file_id = b.file_id) 
where b.file_id is null 
order by a.file_id;