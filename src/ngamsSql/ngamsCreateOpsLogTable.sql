/*
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
 * "@(#) $Id: ngamsCreateOpsLogTable.sql,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
 *
 * Who       When        What
 * --------  ----------  -----------------------------------------------------
 * jknudstr  17/12/2003  Created
 */

/* isql -SELECT<DB Server> -Ungas -P***** < ngamsCreateOpsLogTable.sql
 */

print "Using database ngas"
use ngas
go

print "Dropping old ngas_ops_log_book table"
drop table ngas_ops_log_book
go

/* --------------------------------------------------------------------- */
print "Creating ngas_ops_log_book table"
go
/*-
 * Table:
 *   NGAS Operators Log Book Table - ngas_ops_log_book
 *
 * Description:
 *   The table is used by the NGAS Operators to log all maintenance actions
 *   that have been performed on the system. In this way it is possible to
 *   trace if a disk e.g. was sent back to the vendor for repair etc.
 *
 * Columns:
 *   action_date:   Date the action was performed.
 *
 *   ref_id:        Reference to the event. If the item in question is a file,
 *                  this should be the File ID. If the item is a disk, this
 *                  should be the Disk ID.
 *
 *   action:        Action carried out, e.g. "Removed Disk".
 *
 *   operator_id:   ID of the operator initiating the action.
 *
 *   comment:       Optional comment.
 *
 *   remarks:       Other remarks or information in connection with the event.
-*/
Create table ngas_ops_log_book
(
	action_date             datetime	not null,
	ref_id			varchar(128)	not null,
	action			varchar(64)	not null,
	operator_id		varchar(64)	not null,
	comment	 		varchar(128)	null,
	remarks			text		null
)
go
print "Create index on ngas_ops_log_book"
create unique clustered index dcode_cluster on ngas_ops_log_book(action_date, ref_id)
go
print "Granting"
grant insert, update, delete, select on ngas_ops_log_book to ngas
go
grant select on ngas_ops_log_book to public
go


/* --------------------------------------------------------------------- */
print "Creating ngas_ops_names table"
go
/*-
 * Table:
 *   NGAS Operators Names Table - ngas_ops_names
 *
 * Description:
 *   The table is used to store the names of the operators.
 *
 * Columns:
 *   op_name:       Full name of operator/team.
 *
 *   op_id:         ID of operator/team.
-*/
print "Dropping old ngas_ops_names table"
drop table ngas_ops_names
go
Create table ngas_ops_names
(
	op_name			varchar(128)	not null,
	op_id			varchar(32)	not null
)
go
print "Create index on ngas_ops_names"
print "Granting"
grant insert, update, delete, select on ngas_ops_names to ngas
go
grant select on ngas_ops_names to public
go


/* EOF */
