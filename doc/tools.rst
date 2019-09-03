NGAS tools
##########

This section lists
the command-line tools installed
by the different NGAS python packages.
A very brief description of each tool is given.
For more details on each of them
you can follow the relevant links,
or get the corresponding command-line help message
for each of them.


Server tools
============

These are programs and scripts
that are only relevant for the server-side of NGAS.

.. _tools.server:

``ngamsServer``
---------------

The main workhorse of NGAS,
the ``ngamsServer`` tool
starts up an NGAS server.

For details on how to start the server,
run ``ngamsServer -h``.
Alternatively you can read :ref:`post_inst.run_server`.
For more documentation on the server itself,
its organization and features,
please check the :doc:`server documentation <server>`.


.. _tools.daemon:

``ngamsDaemon``
---------------

The ``ngamsDaemon`` tool
starts an NGAS server is daemon mode.

For details on how to start a daemon
run ``ngamsDaemon -h``.
Alternatively you can read :ref:`post_inst.run_server`.


.. _tools.prepare_volume:

``ngas-prepare-volume``
-----------------------

.. note::
 This tools was previous known as ``ngasPrepareVolume``
 but had not been properly kept up to date.

The ``ngas-prepare-volume`` tool
prepares a directory to be used
as an :ref:`NGAS volume <server.storage>`.
This preparation consists simply
on recording some meta-data about the volume
into a specific place and format.


Client tools
============

These are programs
relevant for the client-side of NGAS.

.. _tools.pclient:

``ngamsPClient``
----------------

The ``ngamsPClient`` tools
is a generic NGAS client written in python,
and accessible via the command-line
for easy use and integration.

Use ``ngamsPClient -h`` for more help.


.. _tools.cclient:

``ngamsCClient``
----------------

The ``ngamsCClient`` tools
is a generic NGAS client written in C,
and accessible via the command-line
for easy use and integration.
The C client (and corresponding library)
are compiled optionally,
so they might not be available for use
(refer to the :ref:`NGAS installation <inst.manual>` for details).

Use ``ngamsCClient -h`` for help on how to use
the C client.


.. _tools.fs_monitor:

``ngas-fs-monitor-client``
--------------------------

.. note::
 This tool was previously known as ``ngasArchiveClient``,
 but had not been properly kept up to date.

The ``ngas-fs-monitor-client`` tool
continuously scans files in a specific directory,
archives them into an NGAS server as they appear,
and performs a check on the server
to ensure the file has been received successfully.
After a successful check, the file is locally removed.

For a given file, its lifecycle looks like this::

     /----------\
     |          |
     v          |
  queue --> archiving --> archived -> (file removed)
               / \
              /   \
             v     v
            bad   backlog

For more information,
run ``ngas-fs-monitor-client -h``.
