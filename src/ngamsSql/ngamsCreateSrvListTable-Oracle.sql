

drop table ngas.ngas_srv_list;
commit;


Create table ngas.ngas_srv_list
(
	srv_list_id           int             not null,
	srv_list              varchar(255)    not null,
        creation_date         varchar(23)     not null,
	constraint ngas_srv_list_idx primary key(srv_list_id) 
);
grant insert, update, delete, select on ngas.ngas_srv_list to ngas;
grant select on ngas.ngas_srv_list to public;
commit;

