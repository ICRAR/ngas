

drop table ngas.ngas_mirroring_bookkeeping;
commit;

Create table ngas.ngas_mirroring_bookkeeping (
       file_id            varchar2(64)    not null, 
       file_version       number(22)      not null, 
       file_size          number(20)          null,
       disk_id            varchar2(128)       null,
       host_id            varchar2(32)        null,
       format             varchar2(32)        null,
       status             char(8)             null,
       target_cluster     varchar2(64)    not null,
       target_host        varchar2(64)        null, 
       archive_command    varchar2(118)       null,
       source_host        varchar2(64)        null,
       retrieve_command   varchar2(118)       null,
       ingestion_date     varchar2(23)        null,
       ingestion_time     float(126)          null, 
       constraint ngas_mirroring_bookkeeping_idx primary key(file_id, file_version, target_cluster)
);

grant insert, update, delete, select on ngas.ngas_mirroring_bookkeeping to ngas;
grant select on ngas.ngas_mirroring_bookkeeping to public;
commit;

