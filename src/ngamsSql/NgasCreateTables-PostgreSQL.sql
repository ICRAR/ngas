\o NgasCreateTables.log

-- Ascertain to be connected as the NGAS user
-- NGAS schema will be used by default if search_path has not been altered
SET session AUTHORIZATION ngas;
-- Define the tablespace to be used for object creation
SET default_tablespace = ngas;

\qecho drop table ngas_cfg 
drop table if exists ngas_cfg cascade;
\qecho drop table ngas_cfg_pars
drop table if exists ngas_cfg_pars cascade;
\qecho drop table ngas_disks
drop table if exists ngas_disks cascade;
\qecho drop table ngas_disks_hist
drop table if exists ngas_disks_hist cascade;
\qecho drop table ngas_files
drop table if exists ngas_files cascade;
\qecho drop table ngas_subscribers
drop table if exists ngas_subscribers cascade;
\qecho drop table ngas_subscr_back_log
drop table if exists ngas_subscr_back_log cascade;
\qecho drop table ngas_hosts
drop table if exists ngas_hosts cascade;

\qecho create table ngas_cfg
create table ngas_cfg
(
	cfg_name  		varchar(32)	not null,
	cfg_par_group_ids	varchar(1024)  not null,
	cfg_comment		varchar(255)	null,
	constraint cfg_idx primary key(cfg_name)
);

\qecho create table ngas_cache
create table ngas_cache
(
	disk_id			varchar(128)	not null,
	file_id			varchar(64)	not null,
	file_version		int		not null,
	cache_time		numeric(16, 6) 	not null,
	cache_delete		smallint	not null,		
	constraint ngas_cache_idx     primary key(disk_id, file_id, file_version)
);

\qecho create table ngas_cfg_pars
create table ngas_cfg_pars
(
	cfg_group_id	varchar(32)	not null,
	cfg_par		varchar(128)	not null,
	cfg_val		varchar(255)	null,
	cfg_comment 	varchar(255)	null
);

\qecho create unique index cfg_group_idx
create unique index cfg_group_idx on ngas_cfg_pars(cfg_group_id,cfg_par,cfg_val);

\qecho create table ngas_disks
create table ngas_disks
(
	disk_id			varchar(128)	not null,
	archive			varchar(64)	not null,
	installation_date	varchar(23)		not null,
	type			varchar(64)	not null,
	manufacturer		varchar(64)     null,
	logical_name		varchar(128)	not null,
	host_id			varchar(32)	null,
	slot_id			varchar(32)	null,
	mounted			smallint		null,
	mount_point		varchar(128)	null,
	number_of_files		int		not null,
	available_mb		int		not null,
	bytes_stored		numeric(20, 0)	not null,
	completed               smallint         not null,
	completion_date         varchar(23)	null,
	checksum		varchar(64)     null,
	total_disk_write_time	real		null,
	last_check		varchar(23)	null,
	last_host_id		varchar(32)	null,
	constraint disk_idx	primary key(disk_id)
);

\qecho create table ngas_disks_hist
create table ngas_disks_hist
(
	disk_id			varchar(128)	not null,
	hist_date		varchar(23)	not null,
	hist_origin		varchar(64)	not null,
	hist_synopsis		varchar(255)	not null,
	hist_descr_mime_type	varchar(64)	null,
	hist_descr		varchar(4000)	null
);

\qecho create index ngas_disks_hist_disk_id
create index ngas_disks_hist_disk_id on ngas_disks_hist(disk_id,hist_date,hist_origin);

\qecho create table ngas_files
create table ngas_files
(
	disk_id			varchar(128)	not null,
	file_name		varchar(255)	not null,
	file_id			varchar(64)	not null,
	file_version    int             default 1,
	format			varchar(32)	not null,
	file_size		numeric(20, 0)	not null,
	uncompressed_file_size	numeric(20, 0)	not null,
	compression		varchar(32)	null,
	ingestion_date		varchar(23)	not null,
	file_ignore                  smallint        null,
	checksum	        varchar(64)	null,
	checksum_plugin         varchar(64)	null,
	file_status             char(8)         default '00000000',
        creation_date           varchar(23)     null,
	constraint file_idx	primary key(file_id,file_version,disk_id)
);

\qecho create table ngas_hosts
create table ngas_hosts
(
	host_id 		varchar(32)	not null,
	domain			varchar(60)	not null,
	ip_address		varchar(20)	not null,
	mac_address		varchar(20)	null,
	n_slots			smallint	null,
	cluster_name		varchar(32)	null,
	installation_date	varchar(23)	null,
	ngas_type               varchar(32)     null,
	idate			varchar(23)	null,
	srv_version		varchar(40)	null,
	srv_port		int		null,
	srv_archive             smallint		null,
	srv_retrieve		smallint		null,
	srv_process    		smallint		null,
	srv_remove    		smallint		null,
	srv_state		varchar(20)	null,
	srv_data_checking	smallint		null,
	srv_check_start         varchar(23)	null,
	srv_check_remain        int             null,
	srv_check_end           varchar(23)	null,
	srv_check_rate          real		null,
	srv_check_mb            real		null,
	srv_checked_mb          real           null,
	srv_check_files         numeric(20, 0)	null,
	srv_check_count         numeric(20, 0)	null,
	srv_suspended		smallint		null,
	srv_req_wake_up_srv	varchar(32)	null,
	srv_req_wake_up_time	varchar(23)	null
) ;

\qecho create unique index host_idx
create unique index host_idx on ngas_hosts(host_id,srv_port);

\qecho create table ngas_subscribers
create table ngas_subscribers
(
	host_id				varchar(32)	not null,
	srv_port			int		not null,
	subscr_prio         smallint         not null,
	subscr_id			varchar(255)	not null,
	subscr_url			varchar(255)	not null,
	subscr_start_date		varchar(23)	null,
	subscr_filter_plugin		varchar(64)	null,
	subscr_filter_plugin_pars	varchar(128)	null,
	last_file_ingestion_date	varchar(23)	null,
	concurrent_threads    int             default 1
);

\qecho create unique index subscr_id_idx
create unique index subscr_id_idx on ngas_subscribers(subscr_id);


\qecho create table ngas_subscr_back_log
create table ngas_subscr_back_log
(
	host_id				varchar(32)	not null,
	srv_port			int		not null,
	subscr_id			varchar(255)	not null,
	subscr_url			varchar(255)	not null,
	file_id				varchar(64)	not null,
	file_name			varchar(255)	not null,	
	file_version        int             not null,
	ingestion_date      varchar(23)        not null,
	format              varchar(32)	not null
);

\o
