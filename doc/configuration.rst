Configuration
=============

This section details the contents of the XML configuration file used by NGAS.

Each sub-section describes an XML Element,
while the items listed on each subsection refer to an attribute
unless specified otherwise.

All elements share an *Id* attribute to uniquely identify them.

Server
------

Contains the overall server configuration.

* *IpAddress*: The IP address to bind the server to. If not specified the server
  will bind itself to ``127.0.0.1``. To bind the server to all interfaces
  ``0.0.0.0`` can be set.
* *Port*: The port to bind the server to. It defaults to 7777 if unspecified.

.. _config.archivehandling:

ArchiveHandling
---------------

Contains archiving-related configuration.

 * *CRCVariant*: The CRC algorithm (and implementation) to use.
   See :ref:`server.crc` for details.
   If not specified the server will use the ``crc32`` variant. If specified,
   ``0`` means ``crc32`` and ``1`` means ``crc32c``.


JanitorThread
-------------

The ``JanitorThread`` element defines the behavior
of the :ref:`Janitor Thread <janthread>`
(now actually implemented as a separate process).
The following attributes are available:

 * *SuspensionTime*: The sleep time after a janitor cycle.
 * *MinSpaceSysDirMb*: The minimum space to be found on each volume during each
   cycle. If not enough space is found the system is sent to OFFLINE state.
 * *PlugIn*: An XML sub-element with a *Name* attribute, naming a python module
   where a Janitor plug-in resides. Multiple *Plugin* elements can be defined.
