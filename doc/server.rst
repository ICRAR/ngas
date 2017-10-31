######
Server
######

The NGAS server is the heart of NGAS.


.. _server.config:

Configuration
=============

The NGAS server is configured via an XML configuration file,
which is indicated to the server at startup time
via the ``-cfg`` command-line flag
(see under :doc:`running`).

To see more details about the XML documentation
go to the :doc:`configuration` section.


.. _server.proxy:

Proxy mode
==========

When NGAS servers are deployed in a cluster configuration
(i.e., many servers connected to a single central database),
clients can issue commands to any server in the cluster.

In this scenario it can happen
that clients contact a server with a command
that can only be fulfilled by a different server
(e.g., fetching data).
In those cases the server contacted by the client
can either respond with an HTTP redirect answer to the client,
or it can act as a proxy,
issuing the command to the second server on behalf of the client,
and forwarding the response as it comes.
This behaviour can be set in a per-server basis
via their corresponding configuration file.
See :ref:`config.server` for details.

.. _server.storage:

Storage organization
====================

Volumes
-------

When NGAS was first designed,
data was mostly transported manually
by swapping physical disks in and out of server.
Thus, data was organized by *disks*,
which were mounted onto the filesystem hierarchy
into a particular directory.

Nowadays transport happens mostly through the network,
even for long-distance transmissions,
with disks staying fixed, or rarely replaced.
Data still gets archived into a directory though,
usually corresponding to the root
of a filesystem mount point.
Thus, we usually prefer to refer
to these top-level data directories
as **volumes**, a more generic term.

Volumes are directories in the filesystem,
usually under the NGAS root directories
(makes it easier to see them all together).
Because they are used to organize how data gets archived,
users will still want to map them
to disk partitions or mounted filesystems
via symbolic links.
Otherwise (specially in testing scenarios)
a simple directory can also be used.

Storage sets
------------

Volumes are grouped in **storage sets**.
A storage set will usually consist
of only one volume, the *main volume*,
but it can optionally contain also
a *replication volume*.
Replication volumes are usually not required nowadays
(because data is replicated via the network,
and/or because mount points are backed up
by different RAID setups),
and therefore you will very unlikely need one.

Streams
-------

Storage sets form the base for organizing data storage.
An NGAS server is configured to store
certain types of data into certain storage sets.
Such mappings from a data type (i.e., a MIME type)
and one or more storage sets
is called a **stream**.

Streams made it easy to collect all data of a certain type
in one or more disks,
which then could be swapped out for data movement.
Because of this, in practice only
the :ref:`commands.archive` command follows this configuration
to determine the target disk to host the incoming data.
On the other hand, the :ref:`commands.qarchive` command
doesn't obey these rules,
as it was designed with network transport
as means of replication.
With network-based replication
the physical volume hosting the data locally
does not have a great impact anymore,
and therefore the system tries to fill them evenly.


.. _server.crc:

CRC
===

When a file is being archived into NGAS
the server will calculate its CRC as part of the archiving process.
The CRC is saved into the database,
and is used later to check the integrity of the file.

Two CRC variants are currently supported by the NGAS server,
which in the future might expand:

* ``crc32``: This is the default implementation.
  It uses python's ``binascii.crc32`` method to calculate the CRC,
  and therefore it is fully implemented in software.
* ``crc32c``: A hardware-based implementation available as part of Intel's
  SSE 4.2 instruction set. A C module exposes this functionality to NGAS
  via a ``crc32c.crc32c`` method.

Depending on your environment choosing to use one method over the other
might bring significant improvements on archiving times for large files.
To configure which method should be used across an entire NGAS installation
change the ``ArchiveHandling.CRCVariant`` setting
on the :ref:`NGAS configuration <config.archivehandling>`.

Also, users can install NGAS without ``crc32c`` support
(see `<inst>`_ for details).


.. _server.archiving_events:

Archiving events
================

The NGAS server features an *archiving event* mechanism.
Each time a new file is archived, a new archiving event is generated,
and a list of *event handlers* is invoked with the given event.
The NGAS server has its won internal event handlers,
but users can also provide their own via plug-ins.
This mechanism is a flexible way of enabling archiving notifications
and reacting on these events.

Users wanting to implement their own event handlers
should :doc:`write a python class <plugins/archiving_events>` to handle it,
and :ref:`configure the server <config.archivehandling>` to use that class.

.. _server.states:

States
======

An NGAS server can be in one of two states at any given time: **ONLINE** and
**OFFLINE**. The state is meant to represent the availability of the NGAS
service. In addition, an **IDLE** or **BUSY** sub-state represents the activity
that is going on on the server.

States are used by the different :ref:`commands` to decide whether a
particular action can take place or not. If the current state/sub-state allows
the operation it will continue without interruptions; otherwise the user will
receive an error stating that the server is in the wrong state/sub-state.

The NGAS server starts by default on the **OFFLINE** state. If the server is
started with the ``-autoOnline`` command-line flag (see how to :ref:`run the
server <running.server>`) it will move itself automatically to the **ONLINE**
state after initializing. At runtime the state can be toggled via
different :ref:`commands`.


.. _server.request_db:

Requests database
=================

The NGAS server keeps a rotating set
of all incoming client requests
for future status querying.
When a client request comes in,
it is first registered into a *requests database*.
After the request is served as usual,
the corresponding item in the request database
is updated to reflect the final state of the request.
If a request is asynchronous in nature
(e.g., it spawns a background task
that will finish later in time),
the entry in the requests database may also be updated
as it logic is executed,
even if the initial response has already been sent
to the user.
This, together with the :ref:`commands.status` command,
are the basis for asynchronous command execution
and monitoring (used only the :ref:`commands.clone` command).

The requests database has three different implementations.
The implementation used by the server is configured
by the ``RequestDbBackend`` attribute
in the :ref:`config.server` configuration element.
The first, a BSDDB-based one, is the most expensive to use,
as it needs to lock during I/O access,
but it provides persistence across executions.
A second, memory-based implementation is also available.
This is faster as it doesn't involve disk I/O,
but doesn't provide persistence.
Finally, a null implementation is provided.
This implementation is provided for cases
when a request database is known not to be needed
(e.g., no asynchronous commands are ever issued).


.. _server.logical_containers:

Logical Containers
==================

NGAS supports the concepts of *logical containers*.
They are called *logical* to distinguish them from *physical* containers.
Physical containers are currently only envisioned and not implemented,
so for the rest of the document we use *container*
and *logical containers* interchangeably.

Logical containers are a way of grouping files together,
which in turn allows to perform container-wise operations
like retrieval or archiving.
Files can be added to or removed from a container independently,
but can belong to only one container (or none) at a time.
Finally, containers can be hierarchically organized,
with one parent container (or none) allowed per container.

Container thus allow to organize files stored in NGAS
in a filesystem-like structure, where directories are NGAS containers
and files are NGAS files.

Containers are handled via the different :doc:`container commands
<commands/containers>`.


.. _server.authorization:

Authorization
=============

NGAS supports authentication
via the standard HTTP ``Authorization`` header.
Currently only ``Basic`` authentication is supported,
but more authentication methods could be added in the future.
On top of authentication, a binary authorization scheme
is implemented which allows users or not
to run a command.

In other words,
NGAS can be set up to allow different users
to run different commands.
Details on how to set up this configuration
can be found in :ref:`config.authorization`.


.. _server.logging:

Logging
=======

The NGAS server outputs its logs to two different places:
the standard output, and a logfile.
Users will mostly be interested in the logfile,
as it provides a persistent location
to inspect logs.
To avoid cluttering,
the NGAS server rotates these logfiles
after a fixed amount of time,
and after each time the server starts.

Each time the logfile is rotated,
its name is first changed to make space for the next logfile.
If the ``Log.ArchiveRotatedLogFiles`` option is set
in the configuration file,
then the logfile is archived into the NGAS server itself
for easier retrieval.
Finally, users can also write more code
to handle a rotated logfile.

Details on how to configure logging in NGAS
can be found in :ref:`config.log`.
To learn how to write logfile handler plug-ins
see :doc:`plugins/logging`.
