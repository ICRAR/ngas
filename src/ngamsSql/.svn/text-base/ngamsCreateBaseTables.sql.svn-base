/*
 *
 *   ALMA - Atacama Large Millimiter Array
 *   (c) European Southern Observatory, 2002
 *   Copyright by ESO (in the framework of the ALMA collaboration),
 *   All rights reserved
 *
 *   This library is free software; you can redistribute it and/or
 *   modify it under the terms of the GNU Lesser General Public
 *   License as published by the Free Software Foundation; either
 *   version 2.1 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public
 *   License along with this library; if not, write to the Free Software
 *   Foundation, Inc., 59 Temple Place, Suite 330, Boston,
 *   MA 02111-1307  USA
 *
 */

/******************************************************************************
 *
 * "@(#) $Id: ngamsCreateBaseTables.sql,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
 *
 * Who       When        What
 * --------  ----------  -----------------------------------------------------
 * jknudstr  15/05/2001  Created
 */

/*
 * Invoke as follows (e.g.): 
 *
 * % isql -STESTSRV -Ungas -P***** -D<DB Name> < ngamsCreateTables.sql
 */

print "Using database: ngas"
use ngas
go

print "Dropping existing NGAS tables"
drop table ngas_cfg
drop table ngas_cfg_pars
drop table ngas_disks
drop table ngas_disks_hist
drop table ngas_files
drop table ngas_subscribers
drop table ngas_subscr_back_log
drop table ngas_hosts
go


/* --------------------------------------------------------------------- */
print "Creating ngas_cfg table"
go
/*-
 * Table:
 *   NGAS Configuration Table - ngas_cfg
 *
 * Description:
 *   The table is used to define NG/AMS Configurations, used in an operational
 *   environment.
 * 
 *   Each configuration used is allocated a name. This is associated with a
 *   set of Configuration Parameter Groups, which constitutes the
 *   configuration.
 *
 * Parameters:
 *   cfg_name:            Name of the configuration. This is used by instances
 *                        of NG/AMS Server when starting up to refer to the
 *                        desired configuration.
 *
 *   cfg_par_group_ids:   The set of Configuration Group IDs constituting the
 *                        configuration.
 *
 *   cfg_comment:         Optional comment.
-*/
Create table ngas_cfg
(
	cfg_name  		varchar(32)	not null,
	cfg_par_group_ids	text		not null,
	cfg_comment		varchar(255)	null
)
go
print "Create index on ngas_cfg"
create unique clustered index dcode_cluster on ngas_cfg(cfg_name)
go
print "Granting"
grant insert, update, delete, select on ngas_cfg to ngas
go
grant select on ngas_cfg to public
go


/* --------------------------------------------------------------------- */
print "Creating ngas_cfg_pars table"
go
/*-
 * Table: 
 *   NGAS Configuration Parameters Table - ngas_cfg_pars
 *
 * Description:
 *   Table to contain the configuration parameters from the XML Configuration
 *   Document. These are loaded into the table with the given Configuration
 *   Group ID, which associate each parameter to a group.
 *
 * Columns:
 *   cfg_group_id:  Configuration Group ID, which defines the category to 
 *                  which the parameter belongs.
 *
 *   cfg_par:       Name of the parameter in the XML Dictionary Keyword format.
 *
 *   cfg_val:       Value of the parameter in string format.
 *
 *   cfg_comment:   Optional comment to the parameter.
-*/
Create table ngas_cfg_pars
(
	cfg_group_id	varchar(32)	not null,
	cfg_par		varchar(128)	not null,
	cfg_val		varchar(255)	not null,
	cfg_comment 	varchar(255)	null
)
go
print "Create index on ngas_cfg_pars"
create unique clustered index dcode_cluster on ngas_cfg_pars(cfg_group_id,
                                                             cfg_par,
                                                             cfg_val)
go
print "Granting"
grant insert, update, delete, select on ngas_cfg_pars to ngas
go
grant select on ngas_cfg_pars to public
go


/* --------------------------------------------------------------------- */
print "Creating ngas_disks table"
go
/*-
 * Table:
 *   NGAS Disks Table - ngas_disks
 *
 * Description:
 *   The ngas_disks table is used to store all the information in connection
 *   with the disks registered in the system. When a new disk is introduced
 *   it is automatically updated in the table. When the disk appear in a new
 *   NGAS Site, the ngas_disks table is updated accprdingly to reflect the 
 *   current status of the disk.
 *
 * Columns:
 *   disk_id:                The ID of the disk. This information is extracted
 *                           by the Online Plug-In from the BIOS of the disk
 *                           drive. This is the unique identifier of the disk.
 *
 *   archive:	             The name of the archive to which the disk belongs.
 *                           The value for this is taken from the NG/AMS
 *                           Configuration.
 *
 *   installation_date:      The date for registering the disk the first time.
 *                           Subsequent re-registering do not change the value
 *                           of this column.
 *
 *   type:                   Indicates the type of the media, e.g.: 
 *                           "MAGNETIC DISK/ATA". The value for this is
 *                           generated by the Online Plug-In
 *
 *   manufacturer:           The manufacturer of the disk. Could e.g. be "IBM"
 *                           or "Seagate". This value is generated by the
 *                           Online Plug-In.
 *
 *   logical_name:           The Logical Name of the disk, is a 'human 
 *                           readable' (unique) ID for the disk. It is
 *                           generated by NG/AMS when the disk is registered
 *                           the first time.
 *
 *   host_id:                The ID of the host where a disk is currently
 *                           registered. If the disk is not registered in any
 *                           NGAS Host, this will be set to "".
 *
 *   slot_id:                The ID of the slot in the NGAS Host, in which the
 *                           disk is currently registered. If the disk is not 
 *                           registered, this will be "".
 *
 *   mounted:                Used to indicate if a disk is mounted or not 
 *                           (1 = mounted, 0 - not mounted).
 *
 *   mount_point:            Used to give the (complete) name of the mount
 *                           point where the disk is mounted. If the disk is
 *                           not mounted this will be "".
 *
 *   number_of_files:        Indicates how many data files that have been
 *                           archived on the disk.
 *
 *   available_mb:           Used to indicate the amount of available storage
 *                           capacity still free on the disk (given in MBs).
 *
 *   bytes_stored:           Used to indicate the amount of data stored on the
 *                           disk(given in bytes).
 *
 *   completed:              Used to indicate that the disk is 'completed',
 *                           i.e., NG/AMS has been archiving files on the disk,
 *                           and has reached the threshold specified in the
 *                           configuration file.
 *
 *   completion_date:        Set by NG/AMS when the disk reached the threshold
 *                           for completion.
 *
 *   checksum:               The global checksum value for the disk. Note, this
 *                           is not set for the moment!
 *
 *   total_disk_write_time:  Total time spent on writing data on the disk.
 *
 *   last_check:             Timestamp for when the last check was carried out.
 *                           This is used to schedule the checking of the data
 *                           holdings on the disks so that the disks that were
 *                           not checked or the ones that was checked the
 *                           longest time ago, are checked first.
 *
 *   last_host_id:           The ID of the host were the disk was registered
 *                           the last time. This is used in order to identify
 *                           where a file/disk is located even though the host
 *                           has been suspended.
-*/
Create table ngas_disks
(
	disk_id			varchar(128)	not null,
	archive			varchar(64)	not null,
	installation_date	datetime	not null,
	type			varchar(64)	not null,
	manufacturer		varchar(64)     null,
	logical_name		varchar(128)	not null,
	host_id			varchar(32)	null,
	slot_id			varchar(32)	null,
	mounted			tinyint		null,
	mount_point		varchar(128)	null,
	number_of_files		int		not null,
	available_mb		int		not null,
	bytes_stored		numeric(20, 0)	not null,
	completed               tinyint         not null,
	completion_date         datetime	null,
	checksum		varchar(64)     null,
	total_disk_write_time	float		null,
	last_check		datetime	null,
	last_host_id		varchar(32)	null
)
go
print "Create index on ngas_disks"
create unique clustered index dcode_cluster on ngas_disks(disk_id)
go
print "Granting"
grant insert, update, delete, select on ngas_disks to ngas
go
grant select on ngas_disks to public
go


/* --------------------------------------------------------------------- */
print "Creating ngas_disks_hist table"
go
/*-
 * Table:
 *   NGAS Disks History Table - ngas_disks_hist
 *
 * Description:
 *   The ngas_disks_hist is used to keep track of the life-time of each disk.
 *   Each important event in the life-cycle of the disk is logged in the table
 *   by NG/AMS. Examples of event is new registration, completion and removal.
 *
 * Columns:
 *   disk_id:               See "disk_id" in "ngas_disks" table.
 *
 *   hist_date:             Timestamp indicating when the event happened in the
 *                          life-time of the disk.
 *
 *   hist_origin:           The originator of the event, i.e., identification
 *                          of the NG/AMS hosting the disk.
 *
 *   hist_synopsis:         Short headline indicating the type of event that
 *                          occurred.
 *
 *   hist_descr_mime_type:  Mime-type of the data stored in the history
 *                          description column.
 *
 *   hist_descr:            Additional information in connection with the
 *                          event. This will typically be a snap-shot of the
 *                          NgasDiskInfo file at the time the event occurred.
 *                          This is e.g. the case when new disks are registered
 *                          and when disks are removed from the system.
-*/
Create table ngas_disks_hist
(
	disk_id			varchar(128)	not null,
	hist_date		datetime	not null,
	hist_origin		varchar(64)	not null,
	hist_synopsis		varchar(255)	not null,
	hist_descr_mime_type	varchar(64)	null,
	hist_descr		text		null
)
go
print "Create index on ngas_disks_hist"
create index ngas_disks_hist_disk_id  on ngas_disks_hist(disk_id)
create index ngas_disks_hist_date     on ngas_disks_hist(hist_date)
create index ngas_disks_hist_origin   on ngas_disks_hist(hist_origin)
go
print "Granting"
grant insert, update, delete, select on ngas_disks_hist to ngas
go
grant select on ngas_disks_hist to public
go


/* --------------------------------------------------------------------- */
print "Creating ngas_files table"
go
/*-
 * Table: 
 *   NGAS Files Table - ngas_files
 *
 * Description:
 *   The ngas_files table is used to store the information about each file
 *   archived into the NGAS System. The table provide for each file all the 
 *   information to handle and track down the given file.
 *
 * Columns:
 *   disk_id:                 ID of the disk where the file is stored.
 *
 *   file_name:               Name of the file. This must be given relative to
 *                            the mount point of the disk.
 *
 *   file_id:                 File ID allocated to the file by the DAPI. The
 *                            set of File ID, Disk ID and File Version,
 *                            uniquely defines a file.
 *
 *   file_version:            Version of the file. The first version is 
 *                            number 1.
 *
 *   format:                  Format of the file. This is generated by the
 *                            DAPI. Should be the mime-type of the file, as
 *                            stored on the disk.
 *
 *   file_size:               Size of the file. This must be given in bytes. If
 *                            the file is compressed, the compressed file size
 *                            must between given as value for this column.
 *
 *   uncompressed_file_size:  If the file was compressed this indicates the
 *                            size of the uncompressed file. If the file is not
 *                            compressed this will be equal to the file_size.
 *
 *   compression:             The compression method applied on the file. Could
 *                            be e.g. "gzip". This should indicate clearly how
 *                            the file has been compressed, to make it possible
 *                            to decompress it at a later stage.
 *
 *  ingestion_date:           Date the file was ingested/archived.
 *
 *  ignore:                   Used to indicate that this file should be ignored
 *                            (1 = ignore). If set to one, this entry for this
 *                            file, will not be taken into account by NG/AMS
 *                            when files or information about files is queried.
 *
 *  checksum:                 Checksum of the file. This value is generated by
 *                            the checksum plug-in specified in the configura-
 *                            tion.
 *
 *  checksum_plugin:          Name of the checksum plug-in used to generate the
 *                            checksum for the file. This is used by NG/AMS
 *                            when performing the Data Consistency Checking of
 *                            data files. NG/AMS in this way, invokes the same
 *                            plug-in as was used to generate the checksum
 *                            originally.
 *
 *  file_status:              Current status of the file. The status should
 *                            between seen as a sequence of bytes, each with a
 *                            certain signification what concerns the condition
 *                            and status of the file. These bytes are used to
 *                            indicate the following (when set to 1). The bytes
 *                            in the status are counted from left to right:
 *
 *                              1:    The File Checksum is incorrect.
 *                              2:    File being checked.
 *                              3-8:  Not used.
 *
 *                            The bytes 3-8 may be used at a later stage.
 *
 *  creation_date:            Date the file was created. This may between
 *                            different from ingestion_date since this
 *                            indicates when the file was archived into the
 *                            system. If a file was cloned, the creation_date
 *                            will be more recent than the ingestion_date.
-*/
Create table ngas_files
(
	disk_id			varchar(128)	not null,
	file_name		varchar(255)	not null,
	file_id			varchar(200)	not null,
	file_version            int             default 1,
	format			varchar(32)	not null,
	file_size		numeric(20, 0)	not null,
	uncompressed_file_size	numeric(20, 0)	not null,
	compression		varchar(32)	null,
	ingestion_date		datetime	not null,
	ignore                  tinyint         null,
	checksum	        varchar(64)	null,
	checksum_plugin         varchar(64)	null,
	file_status             char(8)         default '00000000',
        creation_date           datetime        null
)
go
print "Create index on ngas_files"
create unique clustered index dcode_cluster on ngas_files(file_id,
                                                          file_version,
                                                          disk_id)
go
print "Granting"
grant insert, update, delete, select, delete on ngas_files to ngas
go
grant select on ngas_files  to public
go


/* --------------------------------------------------------------------- */
print "Creating ngas_hosts table"
go
/*-
 * Table: 
 *   NGAS Hosts Table - ngas_hosts
 *
 * Description:
 *   The ngas_hosts table is used to store all the information related to
 *   each NGAS Node in the system. Among this information the location of the
 *   node, the contact information and the current status.
 *
 * Columns:
 *   host_id:              ID of the NGAS Host, e.g. "jewel65". Should be given
 *                         only as the name, i.e., without the domain name.
 * 
 *   domain:               Domain name of the NGAS Host, e.g. "hq.eso.org".
 *
 *   ip_address:           The IP address of the NGAS Host.
 *
 *   ngas_type:            The type of NGAS Host, i.e., which role it has. The
 *                         value of this is not used by the NG/AMS SW.
 *                         Suggested values could be "NAU" - NGAS Archiving
 *                         Unit, "NBU" - NGAS Buffering Unit, "NCU" - NGAS
 *                         Central Unit, "NMU" - NGAS Master Unit and "AHU" - 
 *                         Archive Handling Unit.
 *
 *  mac_address:           MAC address coded into the network card used for
 *                         waking update suspended hosts via network (WOL).
 *                         This is an address of the format, e.g.:
 *                        "05:4E:14:8A:11:2B".
 *
 *  n_slots:               Number of slots in the NGAS Node.
 *
 *  cluster_name:          Name of the NGAS Cluster this system belongs to. The
 *                         Cluster Name is identical to the Master Unit of the
 *                         cluster.
 *
 *  installation_date:     Date the OS and NG/AMS running on the NGAS Host have
 *                         been installed.
 * 
 *  srv_version:           Version of the NG/AMS Server.
 *
 *  srv_port:              Port used by the NG/AMS Server.
 * 
 *  srv_archive:           Indicates if the NG/AMS Server is configured to
 *                         allow Archive Requests (1 = accept Archive
 *                         Requests).
 *
 *  srv_retrieve:          Indicates if the NG/AMS Server is configured to
 *                         allow Retrieve Requests (1 = accept Retrieve
 *                         Requests).
 *
 *  srv_process:           Indicates if the NG/AMS Server is configured to
 *                         allow Processing Requests (1 = accept Processing
 *                         Requests).
 *
 *  srv_data_checking:     Indicates if this server is carrying out Data
 *                         Consistency Checking (see section 3.9).
 *
 *  srv_check_start:       Indicates when the last Data Consistency Checking
 *                         was initiated.
 *
 *  srv_check_remain:      Indicates the approximate remaining time to
 *                         execute the Data Consistency Checking.
 *
 *  srv_check_end:         Indicates when the last Data Consistency Check
 *                         ended.
 *
 *  srv_check_rate:        Indicates the rate with which data is/was checked.
 *                         Given in MB/s.
 *
 *  srv_check_mb:          The amount of data (in MB) to between checked.
 *
 *  srv_checked_mb:        The amount of data checked.
 *
 *  srv_check_files:       The number of files to check.
 *
 *  srv_check_count:       The amount of data files checked.
 *
 *  srv_state:             Indicates the State of the server.
 *
 *  srv_suspended:         Set to 1 if the server is suspended.
 *
 *  srv_req_wake_up_srv:   Name of an NG/AMS Server, which is requested to wake
 *                         up this host/server that has suspended itself.
 *
 *  srv_req_wake_up_time:  Time when the host/server would like to be woken up.
-*/
Create table ngas_hosts
(
	host_id 		varchar(32)	not null,
	domain			varchar(30)	not null,
	ip_address		varchar(20)	not null,
	mac_address		varchar(20)	null,
	n_slots			tinyint		null,
	cluster_name		varchar(16)	null,
	installation_date	datetime	null,
	ngas_type               varchar(32)     null,
	idate			datetime	null,

	srv_version		varchar(20)	null,
	srv_port		int		null,
	srv_archive             tinyint		null,
	srv_retrieve		tinyint		null,
	srv_process    		tinyint		null,
	srv_remove    		tinyint		null,
	srv_state		varchar(20)	null,

	srv_data_checking	tinyint		null,
	srv_check_start         datetime	null,
	srv_check_remain        int             null,
	srv_check_end           datetime	null,
	srv_check_rate          float		null,
	srv_check_mb            float		null,
	srv_checked_mb          float           null,
	srv_check_files         numeric(20, 0)	null,
	srv_check_count         numeric(20, 0)	null,

	srv_suspended		tinyint		null,
	srv_req_wake_up_srv	varchar(32)	null,
	srv_req_wake_up_time	datetime	null
)
go
print "Create index on ngas_hosts"
create unique clustered index dcode_cluster on ngas_hosts(host_id,
                                                          srv_port)
go
print "Granting"
grant insert, update, select on ngas_hosts to ngas
go
grant select on ngas_hosts to public
go


/* --------------------------------------------------------------------- */
print "Creating ngas_subscribers table"
go
/*-
 * Table:
 *   NGAS Subscribers Table - ngas_subscribers
 *
 * Description:
 *   The table is used to keep track of the Subscribers to data from each
 *   NGAS Node. All the information needed for delivering data to the
 *   individual Subscriber is contained in the table.
 *
 * Columns:
 *  host_id:                    The ID of the host where the Data Provider
 *                              NG/AMS Server is running.
 *
 *  srv_port:                   The port number used by the Data Provider
 *                              NG/AMS Server.
 *
 *  subscr_prio:                The priority of the Subscriber as indicated by
 *                              the Subscriber itself. The priority indicates
 *                              how fast the data will be delivered to this
 *                              Subscriber, i.e., how much CPU is allocated to
 *                              deliver the file.
 *
 *  subscr_id:                  See "ngas_subscr_back_log.susbcr_id".
 *
 *  subscr_url:                 See "ngas_subscr_back_log.subscr_url".
 *
 *  subscr_start_date:          Date from which data should be considered for
 *                              delivery for that Subscriber.
 *
 *  subscr_filter_plugin:       A Filter Plug-In (see chapter 11.11), which
 *                              will be applied on the data to determine
 *                              whether to deliver it or not to a Subscriber.
 *
 *  subscr_filter_plugin_pars:  Plug-In Parameters to hand over to the Filter
 *                              Plug-In.
 *
 *  last_file_ingestion_date:   The Ingestion Date of the last file delivered
 *                              to the Subscriber. Used to avoid delivering the
 *                              same data files in multiple copies to the same
 *                              Subscriber.
-*/
Create table ngas_subscribers
(
	host_id				varchar(32)	not null,
	srv_port			int		not null,
	subscr_prio                     tinyint         not null,
	subscr_id			varchar(255)	not null,
	subscr_url			varchar(255)	not null,
	subscr_start_date		datetime	null,
	subscr_filter_plugin		varchar(64)	null,
	subscr_filter_plugin_pars	varchar(128)	null,
	last_file_ingestion_date	datetime	null
)
go
print "Create index on ngas_subscribers"
create unique index subscr_id_idx on ngas_subscribers(subscr_id)
go
create unique index host_id_srv_port_idx on ngas_subscribers(host_id, srv_port)
go
print "Granting"
grant insert, update, delete, select on ngas_subscribers to ngas
go
grant select on ngas_subscribers to public
go


/* --------------------------------------------------------------------- */
print "Creating ngas_subscr_back_log table"
go
/*-
 * Table: 
 *   NGAS Subscriber Back-Log Table - ngas_subscr_back_log
 *
 * Description:
 *   The table is used to keep track of files, which were supposed to between
 *   delivered to Subscribers but which could not between delivered. Such files
 *   are buffered internally in NG/AMS wnd it will between tried periodically,
 *   based on the information in this table, to deliver the data.
 *
 * Columns:
 *   host_id:          The ID of the host where the Data Provider NG/AMS Server
 *                     is running.
 *
 *   srv_port:         The port number used by the Data Provider NG/AMS Server.
 * 
 *   subscr_id:        The ID of the Subscriber.
 *
 *   subscr_url:       The Delivery URL submitted by the Subscriber. The Data
 *                     Provider will POST the data on this URL.
 *
 *   file_id:          NGAS ID of file that could not be delivered.
 *
 *   file_name:        Complete filename of file that could not be delivered.
 *
 *   file_version:     Version of file that could not be delivered.
 *
 *   ingestion_date:   Date the file was ingested into NGAS.
 *
 *   format:           The format (mime-type) of the file..
-*/
Create table ngas_subscr_back_log
(
	host_id				varchar(32)	not null,
	srv_port			int		not null,
	subscr_id			varchar(255)	not null,
	subscr_url			varchar(255)	not null,
	file_id				varchar(64)	not null,
	file_name			varchar(255)	not null,	
	file_version            	int             not null,
	ingestion_date                  datetime        not null,
	format                          varchar(32)	not null
)
go
print "Granting"
grant insert, update, delete, select on ngas_subscr_back_log to ngas
go
grant select on ngas_subscr_back_log to public
go


/* EOF */
