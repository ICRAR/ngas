--
-- This scripts (re)creates the Sybase ASE database schema
-- needed by the NGAS server version 9.0.
--
-- If you are looking to upgrade an existing installation
-- have a look at the deltas directory for schema alterations
--
--
-- ICRAR - International Centre for Radio Astronomy Research
-- (c) UWA - The University of Western Australia, 2018
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

DROP TABLE ngas_cfg;
DROP TABLE ngas_cache;
DROP TABLE ngas_cfg_pars;
DROP TABLE ngas_disks;
DROP TABLE ngas_disks_hist;
DROP TABLE ngas_files;
DROP TABLE ngas_containers;
DROP TABLE ngas_hosts;
DROP TABLE ngas_subscribers;
DROP TABLE ngas_subscr_back_log;
DROP TABLE ngas_subscr_queue;


create table ngas_cfg
(
    cfg_name            varchar(32)    not null,
    cfg_par_group_ids   varchar(1024)  not null,
    cfg_comment         varchar(255)   null,
    constraint cfg_idx  primary key(cfg_name)
);

create table ngas_cache
(
    disk_id        varchar(128)    not null,
    file_id        varchar(64)     not null,
    file_version   int             not null,
    cache_time     numeric(16, 6)  not null,
    cache_delete   smallint        not null,
    constraint ngas_cache_idx primary key(disk_id, file_id, file_version)
);

create table ngas_cfg_pars
(
    cfg_group_id   varchar(32)   not null,
    cfg_par        varchar(128)  not null,
    cfg_val        varchar(255)  null,
    cfg_comment    varchar(255)  null
);
create unique index cfg_group_idx on ngas_cfg_pars(cfg_group_id,cfg_par);

create table ngas_disks
(
    disk_id                 varchar(128)    not null,
    archive                 varchar(64)     not null,
    installation_date       varchar(23)     not null,
    type                    varchar(64)     not null,
    manufacturer            varchar(64)     null,
    capacity_mb             numeric(20,0)   default 0,
    logical_name            varchar(128)    not null,
    host_id                 varchar(32)     null,
    slot_id                 varchar(32)     null,
    mounted                 smallint        null,
    mount_point             varchar(128)    null,
    number_of_files         int             not null,
    available_mb            int             not null,
    bytes_stored            numeric(20, 0)  not null,
    completed               smallint        not null,
    completion_date         varchar(23)     null,
    checksum                varchar(64)     null,
    total_disk_write_time   real            null,
    last_check              varchar(23)     null,
    last_host_id            varchar(32)     null,
    constraint disk_idx primary key(disk_id)
);

create table ngas_disks_hist
(
    disk_id               varchar(128)   not null,
    hist_date             varchar(23)    not null,
    hist_origin           varchar(64)    not null,
    hist_synopsis         varchar(255)   not null,
    hist_descr_mime_type  varchar(64)    null,
    hist_descr            varchar(4000)  null
);
create index ngas_disks_hist_disk_id on ngas_disks_hist(disk_id,hist_date,hist_origin);

CREATE TABLE ngas_containers
(
  container_id         VARCHAR(36)     NOT NULL,
  parent_container_id  VARCHAR(36)     NULL,
  container_name       VARCHAR(255)    NOT NULL,
  ingestion_date       VARCHAR(23)     NULL,
  container_size       NUMERIC(20, 0)  NOT NULL,
  container_type       VARCHAR(10)     NOT NULL,
  CONSTRAINT container_idx PRIMARY KEY(container_id),
  CONSTRAINT container_uni UNIQUE(parent_container_id, container_name),
  CONSTRAINT container_parent FOREIGN KEY (parent_container_id) REFERENCES ngas_containers(container_id)
);

create table ngas_files
(
    disk_id                 varchar(128)    not null,
    file_name               varchar(255)    not null,
    file_id                 varchar(64)     not null,
    file_version            int             default 1,
    format                  varchar(32)     not null,
    file_size               numeric(20, 0)  not null,
    uncompressed_file_size  numeric(20, 0)  not null,
    compression             varchar(32)     null,
    ingestion_date          varchar(23)     not null,
    file_ignore             smallint        null,
    checksum                varchar(64)     null,
    checksum_plugin         varchar(64)     null,
    file_status             char(8)         default '00000000',
    creation_date           varchar(23)     null,
    io_time                 numeric(20, 0)  default -1,
    ingestion_rate          numeric(20, 0)  DEFAULT -1 NULL,
    container_id            varchar(36)     NULL,
    CONSTRAINT file_idx PRIMARY KEY(file_id, file_version, disk_id),
    CONSTRAINT file_container FOREIGN KEY (container_id) REFERENCES ngas_containers(container_id)
);

create table ngas_hosts
(
    host_id               varchar(32)     not null,
    domain                varchar(60)     not null,
    ip_address            varchar(20)     not null,
    mac_address           varchar(20)     null,
    n_slots               smallint        null,
    cluster_name          varchar(32)     null,
    installation_date     varchar(23)     null,
    srv_version           varchar(40)     null,
    srv_port              int             not null,
    srv_archive           smallint        null,
    srv_retrieve          smallint        null,
    srv_process           smallint        null,
    srv_remove            smallint        null,
    srv_state             varchar(20)     null,
    srv_data_checking     smallint        null,
    srv_check_start       varchar(23)     null,
    srv_check_remain      int             null,
    srv_check_end         varchar(23)     null,
    srv_check_rate        real            null,
    srv_check_mb          real            null,
    srv_checked_mb        real            null,
    srv_check_files       numeric(20, 0)  null,
    srv_check_count       numeric(20, 0)  null,
    srv_suspended         smallint        null,
    srv_req_wake_up_srv   varchar(32)     null,
    srv_req_wake_up_time  varchar(23)     null
);
create unique index host_idx on ngas_hosts(host_id,srv_port);

create table ngas_subscribers
(
    host_id                    varchar(32)   not null,
    srv_port                   int           not null,
    subscr_prio                smallint      not null,
    subscr_id                  varchar(255)  not null,
    subscr_url                 varchar(255)  not null,
    subscr_start_date          varchar(23)   null,
    subscr_filter_plugin       varchar(64)   null,
    subscr_filter_plugin_pars  varchar(128)  null,
    last_file_ingestion_date   varchar(23)   null,
    concurrent_threads         int           default 1
);
create unique index subscr_id_idx on ngas_subscribers(subscr_id);


create table ngas_subscr_back_log
(
    host_id         varchar(32)   not null,
    srv_port        int           not null,
    subscr_id       varchar(255)  not null,
    subscr_url      varchar(255)  not null,
    file_id         varchar(64)   not null,
    file_name       varchar(255)  not null,
    file_version    int           not null,
    ingestion_date  varchar(23)   not null,
    format          varchar(32)   not null
);

create table ngas_subscr_queue
(
    subscr_id       varchar(255)  not null,
    file_id         varchar(64)   not null,
    file_version    int           default 1,
    disk_id         varchar(128)  not null,
    file_name       varchar(255)  not null,
    ingestion_date  varchar(23)   not null,
    format          varchar(32)   not null,
    status          int           default -2,
    status_date     varchar(23)   not null,
    comment         varchar(255)  null,
    constraint subscr_queue_idx primary key(subscr_id,file_id,file_version,disk_id)
);
create index subscr_queue_subscr_id_idx on ngas_subscr_queue(subscr_id);