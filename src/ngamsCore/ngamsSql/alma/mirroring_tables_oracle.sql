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


CREATE TABLE ALMA_MIRRORING_EXCLUSIONS
(
   ID decimal(38,0) PRIMARY KEY NOT NULL,
   RULE_TYPE varchar2(8) NOT NULL,
   FILE_PATTERN varchar2(64) NOT NULL,
   INGESTION_START timestamp NOT NULL,
   INGESTION_END timestamp NOT NULL,
   ARC varchar2(6)
);
CREATE UNIQUE INDEX MIRR_EXCLUSIONS_PK ON ALMA_MIRRORING_EXCLUSIONS(ID);


CREATE TABLE NGAS_MIRRORING_BOOKKEEPING
(
   FILE_ID varchar2(220) NOT NULL,
   FILE_VERSION decimal(22,0) NOT NULL,
   FILE_SIZE decimal(20,0) NOT NULL,
   DISK_ID varchar2(128),
   HOST_ID varchar2(32),
   FORMAT varchar2(32),
   STATUS char(8) NOT NULL,
   TARGET_CLUSTER varchar2(64),
   TARGET_HOST varchar2(64),
   SOURCE_HOST varchar2(64) NOT NULL,
   INGESTION_DATE varchar2(23),
   INGESTION_TIME float(126),
   ITERATION decimal(22,0) NOT NULL,
   CHECKSUM varchar2(64) NOT NULL,
   STAGING_FILE varchar2(305),
   ATTEMPT decimal(4,0),
   DOWNLOADED_BYTES decimal(22,0),
   SOURCE_INGESTION_DATE timestamp DEFAULT sysdate NOT NULL,
   CONSTRAINT NGAS_MIRRORING_BOOKKEEPING_IDX PRIMARY KEY (FILE_ID,FILE_VERSION,ITERATION)
);
CREATE INDEX NMB_THOST_STATUS_SHOST_IDX ON NGAS_MIRRORING_BOOKKEEPING (TARGET_CLUSTER, TARGET_HOST, STATUS, SOURCE_HOST);
CREATE INDEX NMB_FILE_SIZE_IDX ON NGAS_MIRRORING_BOOKKEEPING (FILE_SIZE);
CREATE INDEX NMB_ITER_STATUS_IDX ON NGAS_MIRRORING_BOOKKEEPING (ITERATION, STATUS);
