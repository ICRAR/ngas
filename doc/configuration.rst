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

* *RootDirectory*: The root directory which most of the other configuration
  items are relative to.
* *IpAddress*: The IP address to bind the server to. If not specified the server
  will bind itself to ``127.0.0.1``. To bind the server to all interfaces
  ``0.0.0.0`` can be set.
* *Port*: The port to bind the server to. It defaults to 7777 if unspecified.
* *PluginsPath*: A directory where NGAS plug-ins can be loaded from.

.. _config.archivehandling:

ArchiveHandling
---------------

Contains archiving-related configuration.

 * *CRCVariant*: The CRC algorithm (and implementation) to use.
   See :ref:`server.crc` for details.
   If not specified the server will use the ``crc32`` variant. If specified,
   ``0`` means ``crc32`` and ``1`` means ``crc32c``.

Log
---

The server outputs its logs to stdout, to a file, and to syslog,
all of which are optional.
The ``Log`` element of the configuration file
contains the details to configure the server logging output.

* *LocalLogFile*: The file where the logs are dumped to. If given as a
  relative path it is relative to the NGAS root directory.
* *LocalLogLevel* An integer from 1 to 5 indicating the log levels that the server
  should output to ``LocalLogFile``.
* *LogRotateInt*: The interval after which the ``LocalLogFile`` is rotated.
  Specified as ``THH:mm:SS``. Defaults to 10 minutes.
* *SysLog*: An integer indicating whether syslog logging is enabled
  (``1``) or disabled (``0``).
* *SysLogPrefix*: The string used as prefix for all syslog messages.
