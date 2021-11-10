#
#    ALMA - Atacama Large Millimiter Array
#    (c) European Southern Observatory, 2009
#    Copyright by ESO (in the framework of the ALMA collaboration),
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
"""
NGAS utility class to help assign a file to be mirrored to a disk volume on an NGAS node
"""

import logging


logger = logging.getLogger(__name__)

class TargetVolumeAssigner:

    def __init__(self, target_nodes, already_assigned_files, required_free_space_buffer):
        # round-robin pointer for the host
        self.current_host_index = 0
        # round-robin pointer for each hosts volumes
        self.current_host_volume_index = {}
        # all the host names in a list
        self.hosts = list(target_nodes.keys())
        # list all the volume names for each host, dictionary(host_name -> list(volume_name))
        self.host_volume_names = {}
        # dictionary((host_name, volume_name) -> bytes_scheduled_to_be_mirrored)
        self.available_bytes = {}
        # dictionary(host_name -> num_files_to_be_mirrored)
        self.num_files_to_be_mirrored = {}
        for target_host in target_nodes:
            # I don't care about the order of the volumes
            self.host_volume_names[target_host] = list(target_nodes[target_host].keys())
            self.current_host_volume_index[target_host] = 0
            for target_volume in target_nodes[target_host]:
                # get the (total_file_size, num_files) for all files still to be mirrored
                assigned_files_for_this_volume = already_assigned_files.get((target_host, target_volume), (0, 0))
                logger.info('XXX %s:%s avail %s, buffer %s, assigned %s', target_host, target_volume, str(target_nodes[target_host][target_volume]), str(required_free_space_buffer), str(assigned_files_for_this_volume[0]))
                self.available_bytes[(target_host, target_volume)] = target_nodes[target_host][target_volume] - required_free_space_buffer - assigned_files_for_this_volume[0]
                self.num_files_to_be_mirrored[target_host] = self.num_files_to_be_mirrored.get(target_host, 0) + assigned_files_for_this_volume[1]

    def remove_hosts_without_available_threads(self, num_threads_per_host):
        """Could be that the hosts we are considering mirroring too already have theior hands full with mirroring. In
         theory each host has a limited number of threads which they should use for mirroring. We try not to exceed this
         Remove any hosts which do not have at least one thread available for mirroring"""
        for next_host in self.hosts[:]:
            if self.num_files_to_be_mirrored[next_host] >= num_threads_per_host:
                logger.info('host %s does not have any threads available for mirroring - removing as a target', next_host)
                self.hosts.remove(next_host)
        logger.info('remaining target nodes %r', self.hosts)
        # now we need to update the start host index

        # return is for unit testing
        return self.hosts

    def get_next_target_with_space_for(self, required_bytes):
        if not self.hosts:
            logger.info("There are no hosts available with mirroring threads")
            return None, None

        num_assignment_attempts = 0
        while 1:
            logger.info('XXX: iteration %d / %d', num_assignment_attempts, len(self.hosts))
            current_host = self.hosts[self.current_host_index]
            volume_index = self.current_host_volume_index[current_host]

            current_volume = self.host_volume_names[current_host][volume_index]

            # update the round-robin pointers
            volume_index = (volume_index + 1) % (len(self.host_volume_names[current_host]))
            self.current_host_volume_index[current_host] = volume_index
            self.current_host_index = (self.current_host_index + 1) % (len(self.hosts))

            if self.__has_sufficient_disk_space(current_host, current_volume, required_bytes):
                self.__decrease_available_bytes(current_host, current_volume, required_bytes)
                break
            # emergency exit - have we tried to find space in all volumes?
            if num_assignment_attempts > len(self.hosts):
                logger.error("There are no volumes available with space to store file of size %s bytes" % required_bytes)
                current_host = None
                current_volume = None
                break
            num_assignment_attempts += 1

        return current_host, current_volume

    def get_available_bytes(self, target_host, target_volume):
        return self.available_bytes[(target_host, target_volume)]

    def __has_sufficient_disk_space(self, target_host, target_volume, file_size_bytes):
        logger.info('XXX: available space for volume (%s, %s): %s MB', target_host, target_volume, str(self.available_bytes[(target_host, target_volume)]))
        has_space = self.available_bytes[(target_host, target_volume)] >= file_size_bytes
        if not has_space:
            logger.info("%s:%s does not have sufficient space", target_host, target_volume)
        return has_space

    def __decrease_available_bytes(self, target_host, target_volume, file_size_bytes):
        self.available_bytes[(target_host, target_volume)] -= file_size_bytes
        logger.info('XXX: decreased space for volume (%s, %s) to: %s MB', target_host, target_volume, str(self.available_bytes[(target_host, target_volume)]))

