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
