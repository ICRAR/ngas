--
-- This scripts (re)creates the Oracle database schema
-- needed by the NGAS server version 9.0.
--
-- If you are looking to upgrade an existing installation
-- have a look at the deltas directory for schema alterations
--
--
-- ICRAR - International Centre for Radio Astronomy Research
-- (c) UWA - The University of Western Australia, 2017
-- Copyright by UWA (in the framework of the ICRAR)
-- All rights reserved
--
-- This library is free software; you can redistribute it and/or
-- modify it under the terms of the GNU Lesser General Public
-- License as published by the Free Software Foundation; either
-- version 2.1 of the License, or (at your option) any later version.
--
-- This library is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
-- Lesser General Public License for more details.
--
-- You should have received a copy of the GNU Lesser General Public
-- License along with this library; if not, write to the Free Software
-- Foundation, Inc., 59 Temple Place, Suite 330, Boston,
-- MA 02111-1307  USA
--

create table ngas_cache
(
    disk_id varchar2(128) constraint nn_ngas_cache_disk_id not null,
    file_id varchar2(64) constraint nn_ngas_cache_file_id not null,
    file_version number(*, 0) constraint nn_ngas_cache_file_version not null,
    cache_time number(16, 6) constraint nn_ngas_cache_cache_time not null,
    cache_delete number(*, 0) constraint nn_ngas_cache_cache_delete not null,
    constraint pk_ngas_cache primary key (disk_id, file_id, file_version)
);

create table ngas_cfg
(
    cfg_name varchar2(32) constraint nn_ngas_cfg_cfg_name not null,
    cfg_par_group_ids varchar2(1024) constraint nn_ngas_cfg_cfg_par_group_ids not null,
    cfg_comment varchar2(255) null,
    constraint pk_ngas_cfg primary key (cfg_name)
);

create table ngas_cfg_pars
(
    cfg_group_id varchar2(32) constraint nn_ngas_cfg_pars_cfg_group_id not null,
    cfg_par varchar2(128) constraint nn_ngas_cfg_pars_cfg_par not null,
    cfg_val varchar2(255) null,
    cfg_comment varchar2(255) null,
    constraint pk_ngas_cfg_pars primary key (cfg_group_id, cfg_par)
);

create table ngas_containers
(
    container_id varchar2(36) constraint nn_ngas_containers_container_id not null,
    parent_container_id varchar2(36) null,
    container_name varchar2(255) constraint nn_ngas_containers_container_name not null,
    ingestion_date varchar2(23) null,
    container_size number(20, 0) constraint nn_ngas_containers_container_size not null,
    container_type varchar2(10) constraint nn_ngas_containers_container_type not null,
    constraint pk_ngas_containers primary key (container_id),
    constraint fk_ngas_containers_parent_container_id foreign key (parent_container_id) references ngas_containers (container_id),
    constraint uk_ngas_containers_parent_container_id_container_name unique (parent_container_id, container_name)
);

create table ngas_disks
(
    disk_id varchar2(128) constraint nn_ngas_disks_disk_id not null,
    archive varchar2(64) constraint nn_ngas_disks_archive not null,
    installation_date varchar2(23) constraint nn_ngas_disks_installation_date not null,
    type varchar2(64) constraint nn_ngas_disks_type not null,
    manufacturer varchar2(64) null,
    capacity_mb number(20, 0) default 0 null,
    logical_name varchar2(128) constraint nn_ngas_disks_logical_name not null,
    host_id varchar2(128) null,
    slot_id varchar2(32) null,
    mounted number(*, 0) null,
    mount_point varchar2(128) null,
    number_of_files number(*, 0) constraint nn_ngas_disks_number_of_files not null,
    available_mb number(*, 0) constraint nn_ngas_disks_available_mb not null,
    bytes_stored number(20, 0) constraint nn_ngas_disks_bytes_stored not null,
    completed number(*, 0) constraint nn_ngas_disks_completed not null,
    completion_date varchar2(23) null,
    checksum varchar2(64) null,
    total_disk_write_time float(126) null,
    last_check varchar2(23) null,
    last_host_id varchar2(128) null,
    constraint pk_ngas_disks primary key (disk_id)
);

create index idx_ngas_disks_hostid on ngas_disks (host_id);

create table ngas_disks_hist
(
    disk_id varchar2(128) constraint nn_ngas_disks_hist_disk_id not null,
    hist_date varchar2(23) constraint nn_ngas_disks_hist_hist_date not null,
    hist_origin varchar2(64) constraint nn_ngas_disks_hist_hist_origin not null,
    hist_synopsis varchar2(255) constraint nn_ngas_disks_hist_hist_synopsis not null,
    hist_descr_mime_type varchar2(64) null,
    hist_descr varchar2(4000) null,
    constraint pk_ngas_disks_hist primary key (disk_id)
);

create index idx_ngas_disks_hist_disk_id_hist_date_hist_origin on ngas_disks_hist (disk_id, hist_date, hist_origin);

create table ngas_files
(
    disk_id varchar2(128) constraint nn_ngas_files_disk_id not null,
    file_name varchar2(255) constraint nn_ngas_files_file_name not null,
    file_id varchar2(220) constraint nn_ngas_files_file_id not null,
    file_version number(*, 0) default 1 null,
    format varchar2(32) constraint nn_ngas_files_format not null,
    file_size number(20, 0) constraint nn_ngas_files_file_size not null,
    uncompressed_file_size number(20, 0) constraint nn_ngas_files_uncompressed_file_size not null,
    compression varchar2(32) null,
    ingestion_date varchar2(23) constraint nn_ngas_files_ingestion_date not null,
    file_ignore number(*, 0) null,
    checksum varchar2(64) null,
    checksum_plugin varchar2(64) null,
    file_status char(8) default '00000000' null,
    creation_date varchar2(23) null,
    io_time number(20, 0) default -1 null,
    ingestion_rate number(20, 0) default -1 null,
    container_id varchar2(36) null,
    constraint pk_ngas_files primary key (file_id, file_version, disk_id),
    constraint fk_ngas_files_container_id foreign key (container_id) references ngas_containers (container_id)
);

create index idx_ngas_files_disk_id_file_ignore on ngas_files (disk_id, file_ignore);
create index idx_ngas_files_file_size on ngas_files (file_size);
create index idx_ngas_files_ingestion_date_1 on ngas_files (substr(ingestion_date, 1, 10));
create index idx_ngas_files_ingestion_date_2 on ngas_files (to_date(substr(replace(ingestion_date, 'T', ' '), 1, 16), 'YYYY-MM-DD HH24:MI'));

-- Please check bit-mapped indexes are supported in your Oracle DB before uncommenting
-- create bitmap index idx_ngas_files_disk_id on ngas_files (disk_id);

create table ngas_hosts
(
    host_id varchar2(128) constraint nn_ngas_hosts_host_id not null,
    domain varchar2(30) constraint nn_ngas_hosts_domain not null,
    ip_address varchar2(20) constraint nn_ngas_hosts_ip_address not null,
    mac_address varchar2(20) null,
    n_slots number(*, 0) null,
    cluster_name varchar2(128) null,
    installation_date varchar2(23) null,
    ngas_type varchar2(32) null,
    idate varchar2(23) null,
    srv_version varchar2(40) null,
    srv_port number(*, 0) null,
    srv_archive number(*, 0) null,
    srv_retrieve number(*, 0) null,
    srv_process number(*, 0) null,
    srv_remove number(*, 0) null,
    srv_state varchar2(20) null,
    srv_data_checking number(*, 0) null,
    srv_check_start varchar2(23) null,
    srv_check_remain number(*, 0) null,
    srv_check_end varchar2(23)  null,
    srv_check_rate float(126) null,
    srv_check_mb float(126) null,
    srv_checked_mb float(126) null,
    srv_check_files number(20, 0) null,
    srv_check_count number(20, 0) null,
    srv_suspended number(*, 0) null,
    srv_req_wake_up_srv varchar2(32) null,
    srv_req_wake_up_time varchar2(23) null,
    constraint pk_ngas_hosts primary key (host_id, srv_port)
);

create index idx_ngas_hosts_cluster_name_srv_archive_srv_state on ngas_hosts (cluster_name, srv_archive, srv_state, substr(host_id, 0, instr(host_id || ':', ':')) || '.' || domain || ':' || to_char(srv_port));

create table ngas_subscribers
(
    host_id varchar2(128) constraint nn_ngas_subscribers_host_id not null,
    srv_port number(*, 0) constraint nn_ngas_subscribers_srv_port not null,
    subscr_prio number(*, 0) constraint nn_ngas_subscribers_subscr_prio not null,
    subscr_id varchar2(255) constraint nn_ngas_subscribers_subscr_id not null,
    subscr_url varchar2(255) constraint nn_ngas_subscribers_subscr_url not null,
    subscr_start_date varchar2(23) null,
    subscr_filter_plugin varchar2(64) null,
    subscr_filter_plugin_pars varchar2(128) null,
    last_file_ingestion_date varchar2(23) null,
    concurrent_threads number(*, 0) default 1 null,
    constraint pk_ngas_subscribers primary key (host_id, srv_port)
);

create unique index idx_ngas_subscribers_subscr_id on ngas_subscribers (subscr_id);
--create unique index idx_ngas_subscribers_host_id_srv_port on ngas_subscribers (host_id, srv_port);

--Check the primary key is correct for this table
create table ngas_subscr_back_log
(
    host_id varchar2(128) constraint nn_ngas_subscr_back_log_host_id not null,
    srv_port number(*, 0) constraint nn_ngas_subscr_back_log_srv_port not null,
    subscr_id varchar2(255) constraint nn_ngas_subscr_back_log_subscr_id not null,
    subscr_url varchar2(255) constraint nn_ngas_subscr_back_log_subscr_url not null,
    file_id varchar2(64) constraint nn_ngas_subscr_back_log_file_id not null,
    file_name varchar2(255) constraint nn_ngas_subscr_back_log_file_name not null,
    file_version number(*, 0) constraint nn_ngas_subscr_back_log_file_version not null,
    ingestion_date varchar2(23) constraint nn_ngas_subscr_back_log_ingestion_date not null,
    format varchar2(32) constraint nn_ngas_subscr_back_log_format not null,
    constraint pk_ngas_subscr_back_log primary key (host_id, srv_port, subscr_id, file_id, file_version)
);

create table ngas_subscr_queue
(
    subscr_id varchar2(255) constraint nn_ngas_subscr_queue_subscr_id not null,
    file_id varchar2(64) constraint nn_ngas_subscr_queue_file_id not null,
    file_version number(*, 0) default 1 null,
    disk_id varchar2(128) constraint nn_ngas_subscr_queue_disk_id not null,
    file_name varchar2(255) constraint nn_ngas_subscr_queue_file_name not null,
    ingestion_date varchar2(23) constraint nn_ngas_subscr_queue_ingestion_date not null,
    format varchar2(32) constraint nn_ngas_subscr_queue_format not null,
    status number(*, 0) default -2 null,
    status_date varchar2(23) constraint nn_ngas_subscr_queue_status_date not null,
    "comment" varchar2(255) null,
    constraint pk_ngas_subscr_queue primary key (subscr_id, file_id, file_version, disk_id)
);

