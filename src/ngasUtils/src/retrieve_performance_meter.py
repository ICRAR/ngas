#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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
import argparse
import functools
import json
import logging
import multiprocessing
import sys
import time

from ngamsPClient import ngamsPClient


logger = logging.getLogger(__name__)

def retrieve(client, file_id, iterations, _):

    times = []
    for _ in range(iterations):
        start = time.time()
        try:
            client.retrieve(file_id, targetFile='/dev/null')
            times.append(time.time() - start)
        except:
            logger.exception('Problem while RETRIEVEing file')

    return times

def meter_performance():

    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host',       help='The host where the server is running', default='localhost')
    parser.add_argument('-p', '--port',       help='The port where the server is running', type=int, default=7777)
    parser.add_argument('-n', '--nret',       help='Number of retrievals to run in parallel', type=int, default=60)
    parser.add_argument('-f', '--file-id',    help='FileID to retrieve. Defaults to a random file in the server', default=None)
    parser.add_argument('-i', '--iterations', help='Iterations per thread', type=int, default=1)

    opts = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    client = ngamsPClient.ngamsPClient(opts.host, opts.port, timeout=5)

    # Which file?
    file_id = opts.file_id
    if not file_id:
        status = client.get_status('QUERY', pars=(('query', 'files_list'), ('format', 'json')))
        data = json.loads(status.getData())
        #print(data)
        file_id = data['files_list'][0]['col3']
        print("Using file_id = %s" % (file_id,))

    # How big it is?
    status = client.status(pars=(('file_id', file_id),))
    fsize = int(status.getDiskStatusList()[0].getFileObjList()[0].getUncompressedFileSize())
    fsize_mb = fsize / 1024. / 1024.

    # Go, go, go!
    pool = multiprocessing.Pool(opts.nret)
    start = time.time()
    times = pool.map(functools.partial(retrieve, client, file_id, opts.iterations), range(opts.nret))
    duration = time.time() - start

    # Overall stats
    n_files = reduce(lambda x, y: x + y, map(len, times))
    logger.info('Successfully retrieved %d files of %.1f [MB] each in %.3f [s] at %.1f [MB/s]',
                n_files, fsize_mb, duration, n_files * fsize_mb / duration)

if __name__ == '__main__':
    meter_performance()