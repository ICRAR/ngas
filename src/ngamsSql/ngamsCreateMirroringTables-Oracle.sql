

drop table ngas.ngas_mirroring_queue;
commit;


Create table ngas.ngas_mirroring_queue
(
	instance_id           varchar(32)     not null,
	file_id               varchar(64)     not null,
	file_version          int             not null,
	ingestion_date        varchar(23)     not null,
	srv_list_id           int             not null,
	xml_file_info         varchar(2000)   not null,
	status                int             not null,
	message               varchar(2000)   null,
	last_activity_time    varchar(23)     not null,
	scheduling_time       varchar(23)     not null,
	constraint ngas_mirroring_queue_idx primary key(file_id, file_version) 
);
grant insert, update, delete, select on ngas.ngas_mirroring_queue to ngas;
grant select on ngas.ngas_mirroring_queue to public;
commit;

drop table ngas.ngas_mirroring_hist;
commit;

Create table ngas.ngas_mirroring_hist
(
	instance_id           varchar(32)     not null,
	file_id               varchar(64)     not null,
	file_version          int             not null,
	ingestion_date        varchar(23)     not null,
	srv_list_id           int             not null,
	xml_file_info         varchar(2000)   not null,
	status                int             not null,
	message               varchar(2000)   null,
	last_activity_time    varchar(23)     not null,
	scheduling_time       varchar(23)     not null,
	constraint ngas_mirroring_hist_idx primary key(file_id, file_version) 
);
grant insert, update, delete, select on ngas.ngas_mirroring_hist to ngas;
grant select on ngas.ngas_mirroring_hist to public;
commit;

