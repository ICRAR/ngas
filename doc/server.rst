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
