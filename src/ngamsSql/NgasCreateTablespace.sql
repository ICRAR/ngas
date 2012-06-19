create tablespace ngas
logging
  datafile '/u02/oradata/alma/ngas_ts' size 500m
  autoextend on next 100m maxsize 2048m
  extent management local;

commit;
disconnect;
quit
