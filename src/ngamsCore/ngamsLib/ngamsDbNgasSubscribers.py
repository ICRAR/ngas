#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
#******************************************************************************
#
# "@(#) $Id: ngamsDbNgasSubscribers.py,v 1.5 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/03/2008  Created
#
"""
Contains queries for accessing the NGAS Subscribers tables.

This class is not supposed to be used standalone in the present implementation.
It should be used as part of the ngamsDbBase parent classes.
"""
import logging

from ngamsCore import fromiso8601
import ngamsDbCore
import ngamsSubscriber


logger = logging.getLogger(__name__)

class ngamsDbNgasSubscribers(ngamsDbCore.ngamsDbCore):
    """
    Contains queries for accessing the NGAS subscription-related tables.
    """

    subscribers_columns = [
        "host_id", "srv_port", "subscr_prio", "subscr_id", "subscr_url",
        "subscr_start_date", "subscr_filter_plugin", "subscr_filter_plugin_pars",
        "last_file_ingestion_date", "concurrent_threads", "active"
    ]

    @classmethod
    def _to_subscriber(cls, row):
        host_id, port, priority, subscriber_id, url = row[0], row[1], row[2], row[3], row[4]
        start_date = fromiso8601(row[5]) if row[5] else None
        filter_plugin, filter_plugin_pars = row[6], row[7]
        concurrent_threads, active = row[9], True if row[10] == 1 else False
        return ngamsSubscriber.ngamsSubscriber(subscriber_id, host_id, port, priority,
                                               url, start_date, concurrent_threads,
                                               active, filter_plugin, filter_plugin_pars)

    select_from_subscribers = "SELECT %s FROM ngas_subscribers" % ', '.join(subscribers_columns)
    def get_subscriber(self, subscrId=None, hostId=None, portNo=-1, active=None):
        """
        Return one or more ngamsSubscriber objects for the given criteria.
        If subscrId is given, at most one subscriber is expected.
        """
        vals = []
        sql = []
        conditions = []
        sql.append(ngamsDbNgasSubscribers.select_from_subscribers)

        if subscrId is not None:
            conditions.append("subscr_id = {}")
            vals.append(subscrId)

        if hostId is not None:
            conditions.append("host_id = {}")
            vals.append(hostId)

        if portNo != -1:
            conditions.append("srv_port = {}")
            vals.append(portNo)

        if active is not None:
            conditions.append("active = {}")
            vals.append(1 if active else 0)

        conditions = ' AND '.join(conditions)
        if conditions:
            sql.append(' WHERE ')
            sql.append(conditions)

        result = self.query2(''.join(sql), args = vals)
        result = [ngamsDbNgasSubscribers._to_subscriber(r) for r in result]

        # A specific subscriber was requested, return that or None
        if subscrId is not None:
            return result[0] if result else None

        # It's a list
        return result

    def insertSubscriberEntry(self, subscriber):

        sql = ("INSERT INTO ngas_subscribers"
               " (host_id, srv_port, subscr_prio, subscr_id,"
               " subscr_url, subscr_start_date,"
               " subscr_filter_plugin,"
               " subscr_filter_plugin_pars,"
               " last_file_ingestion_date, concurrent_threads, active) "
               " VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10})")

        vals = (subscriber.host_id, subscriber.port, subscriber.priority,
                subscriber.id, subscriber.url, self.asTimestamp(subscriber.start_date),
                subscriber.filter_plugin, subscriber.filter_plugin_pars,
                None, subscriber.concurrent_threads, 1 if subscriber.active else 0)
        self.query2(sql, args = vals)


    def updateSubscriber(self, subscriber_id, active=None, priority=None,
                         url=None, concurrent_threads=None):
        """
        Updates details of a subscriber.
        """

        fields = []
        vals = []
        if active is not None:
            fields.append('active={}')
            vals.append(1 if active else 0)
        if priority is not None:
            fields.append('subscr_prio={}')
            vals.append(priority)
        if url is not None:
            fields.append('subscr_url={}')
            vals.append(url)
        if concurrent_threads is not None:
            fields.append('concurrent_threads={}')
            vals.append(concurrent_threads)

        if not fields:
            logger.warning("updateSubscriber called without values to update")
            return

        sql = ["UPDATE ngas_subscribers SET ", ", ".join(fields), " WHERE subscr_id={}"]
        vals.append(subscriber_id)
        self.query2(''.join(sql), args = vals)


    def delete_subscriber_and_deliveires(self,
                         subscrId):
        """
        Delete the information for one Subscriber from the NGAS DB.

        subscrId:   Subscriber ID (string).

        Returns:    Reference to object itself.
        """
        with self.transaction() as t:
            t.execute('DELETE FROM ngas_subscr_delivery_queue WHERE subscr_id={0}', args=(subscrId,))
            t.execute("DELETE FROM ngas_subscribers WHERE subscr_id={0}", args=(subscrId,))


    def add_to_delivery_queue(self, deliveries):
        """Insert all the given deliveries into the delivery queue table"""

        sql = ('INSERT INTO ngas_subscr_delivery_queue (subscr_id, file_id, file_version, disk_id)'
               ' VALUES ({}, {}, {}, {})')
        with self.transaction() as t:
            for d in deliveries:
                args = (d.subscriber_id, d.file_id, d.file_version, d.disk_id)
                t.execute(sql, args=args)

    def get_all_deliveries(self, host_id):
        """Return a sequence with all deliveries for the given host_id"""
        sql = ('SELECT q.file_id, q.file_version, q.disk_id, f.file_name, d.mount_point,'
               '       f.format, f.checksum, f.checksum_plugin,'
               '       s.subscr_id, s.subscr_prio, s.subscr_url'
               ' FROM ngas_subscr_delivery_queue q'
               ' INNER JOIN ngas_subscribers s ON s.subscr_id = q.subscr_id'
               ' INNER JOIN ngas_files f ON f.file_id = q.file_id AND f.file_version = q.file_version AND f.disk_id = q.disk_id'
               ' INNER JOIN ngas_disks d ON d.disk_id = q.disk_id'
               ' WHERE d.host_id={} AND s.active = 1')
        return self.query2(sql, args=(host_id,))

    def remove_from_delivery_queue(self, subscr_id, file_id, file_version, disk_id):
        """
        Remove the corresponding entry from the database and return the number
        of remaining entries for the given file_id/file_version/disk_id combination.
        """

        del_sql = ('DELETE FROM ngas_subscr_delivery_queue'
                   ' WHERE subscr_id={} AND file_id={} AND file_version={} AND disk_id={}')
        count_sql = ('SELECT COUNT(*) FROM ngas_subscr_delivery_queue'
                     ' WHERE file_id={} AND file_version={} AND disk_id={}')
        del_args = (subscr_id, file_id, file_version, disk_id)
        count_args = (file_id, file_version, disk_id)
        with self.transaction() as t:
            t.execute(del_sql, args=del_args)
            return t.execute(count_sql, args=count_args)[0][0]