use ngastst1
go

drop table ngas_cache
go


Create table ngas_cache
(
    disk_id       varchar(128)    not null,
    file_id       varchar(64)     not null,
    file_version  int             not null,
    cache_time    numeric(16, 6)  not null,
    cache_delete  tinyint         not null
)
go
create unique clustered index dcode_cluster on ngas_cache(disk_id, file_id, file_version)
go
grant insert, update, delete, select on ngas_cache to ngas
go
grant select on ngas_cache to public
go



