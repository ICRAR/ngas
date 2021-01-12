--
-- This scripts creates the ALMA mirroring database tables.
-- ALMA uses an Oracle database engine, and thus this script
-- is provided in Oracle-flavour only.
--
--
-- ICRAR - International Centre for Radio Astronomy Research
-- (c) UWA - The University of Western Australia, 2012
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

-- Create ALMA NGAS mirroring tables
create table alma_mirroring_exclusions
(
    id number(*, 0) constraint nn_alma_mirroring_exclusions_id not null,
    rule_type varchar2(8) constraint nn_alma_mirroring_exclusions_rule_type not null,
    file_pattern varchar2(64) constraint nn_alma_mirroring_exclusions_file_pattern not null,
    ingestion_start timestamp(6) constraint nn_alma_mirroring_exclusions_ingestion_start not null,
    ingestion_end timestamp(6) constraint nn_alma_mirroring_exclusions_ingestion_end not null,
    arc varchar2(6) null,
    constraint pk_alma_mirroring_exclusions primary key (id)
);

create table ngas_mirroring_bookkeeping
(
    file_id varchar2(220) constraint nn_ngas_mirroring_bookkeeping_file_id not null,
    file_version number(22, 0) constraint nn_ngas_mirroring_bookkeeping_file_version not null,
    file_size number(20, 0) constraint nn_ngas_mirroring_bookkeeping_file_size not null,
    disk_id varchar2(128) null,
    host_id varchar2(128) null,
    format varchar2(32) null,
    status char(8) constraint nn_ngas_mirroring_bookkeeping_status not null,
    target_cluster varchar2(64) constraint nn_ngas_mirroring_bookkeeping_target_cluster not null,
    target_host varchar2(64) null,
    source_host varchar2(64) constraint nn_ngas_mirroring_bookkeeping_source_host not null,
    ingestion_date varchar2(23) null,
    ingestion_time float(126) default 0 constraint nn_ngas_mirroring_bookkeeping_ingestion_time not null,
    iteration number(22, 0) default 0 constraint nn_ngas_mirroring_bookkeeping_iteration not null,
    checksum varchar2(64) default '?' constraint nn_ngas_mirroring_bookkeeping_checksum not null,
    checksum_plugin varchar2(64) null,
    staging_file varchar2(305) null,
    attempt number(4, 0) default 0 null,
    downloaded_bytes number(22, 0) default 0 null,
    source_ingestion_date date default sysdate constraint nn_ngas_mirroring_bookkeeping_source_ingestion_date not null,
    constraint pk_ngas_mirroring_bookkeeping primary key (file_id, file_version, iteration)
);

create index idx_ngas_mirroring_bookkeeping_target_cluster_target_host_status_source_host on ngas_mirroring_bookkeeping (target_cluster, target_host, status, source_host);
create index idx_ngas_mirroring_bookkeeping_file_size on ngas_mirroring_bookkeeping (file_size);
create index idx_ngas_mirroring_bookkeeping_iteration_status on ngas_mirroring_bookkeeping (iteration, status);

