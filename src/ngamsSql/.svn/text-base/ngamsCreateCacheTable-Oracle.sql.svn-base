

drop table ngas.ngas_cache;
commit;


Create table ngas.ngas_cache
(
    disk_id       varchar(128)    not null,
    file_id       varchar(64)     not null,
    file_version  int             not null,
    cache_time    numeric(16, 6)  not null,
    cache_delete  smallint        not null,
    constraint ngas_cache_idx primary key(disk_id, file_id, file_version)
);
grant insert, update, delete, select on ngas.ngas_cache to ngas;
grant select on ngas.ngas_cache to public;
commit;

