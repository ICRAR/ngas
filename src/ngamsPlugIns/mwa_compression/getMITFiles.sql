\f ','
\a
\t
\o mit_compress.csv
select b.file_id, b.disk_id, b.file_version, substring(b.file_id, 1,10) obs_id, a.host_id, a.mount_point || '/' || b.file_name as file_full_path, 'None', -1 
from ngas_disks a, ngas_files b 
where a.disk_id = b.disk_id 
order by obs_id, host_id