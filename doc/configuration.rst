Configuration
=============

This section details the contents of the XML configuration file used by NGAS.

Each sub-section describes an XML Element,
while the items listed on each subsection refer to an attribute
unless specified otherwise.

All elements share an *Id* attribute to uniquely identify them.

For a sample configuration file
see the `sample configuration file <https://github.com/ICRAR/ngas/blob/master/cfg/sample_server_config.xml>`_
shipped with NGAS.

.. _config.server:

Server
------

Contains the overall server configuration.

* *RootDirectory*: The root directory which most of the other configuration
  items are relative to.
* *ArchiveName*: The logical name under which
  disks found by the server are grouped into.
  Using this, disks found in different servers
  may belong to the same logically distributed archive.
* *BlockSize*: The block size used for disk and network access,
  and checksum calculation.
  In the future different configuration options may be offered
  for these different operations.
* *IpAddress*: The IP address to bind the server to. If not specified the server
  will bind itself to ``127.0.0.1``. To bind the server to all interfaces
  ``0.0.0.0`` can be set.
* *PortNo*: The port to bind the server to. It defaults to 7777 if unspecified.
* *VolumeDirectory*: The base directory where volumes are searched for.
  It relative, it is considered relative to the NGAS root directory.
  Defaults to ``.``.
* *MaxSimReqs*: The maximum number of requests the server can be serving
  at a given time. If a new request comes in and the server has reached
  the limit already, it will respond with an ``503`` HTTP code.
* *PluginsPath*: A colon-separated list of directories
  where external python code, like NGAS plug-ins or database drivers,
  can be loaded from.
* *ProxyMode*: Whether this server should act as a proxy when serving requests that
  are addressed to a different server within the same cluster (``1``)
  or not (``0``).
  See :ref:`server.proxy` for details.
* *RequestDbBackend*: The implementation of the request database
  that should be used.
  Allowed values are ``memory``, ``bsddb`` and ``null``.
  See :ref:`server.request_db` for details.
  Defaults to ``null``.


.. _config.permissions:

Permissions
-----------

This element defines the set of actions
this server is allowed to perform.

* *AllowArchiveReq*: Whether archiving is allowed on this server.
* *AllowProcessingReq*: Whether processing is allowed on this server.
* *AllowRemoveReq*: Whether removal of files is allowed on this server.
* *AllowRetrieveReq*: Whether retrieval of files is allowed on this server.


.. _config.db:

Db
--

This element contains the database connection parameters.

* *Interface*:
  The python module implementing the PEP-249
  Database API Specification v2.0.
* *MaxPoolConnections*:
  The maximum number of connections to be contained in the connection pool.
* *Snapshot*:
  Whether the *snapshoting* feature of NGAS will be turned on or off.
  It is recommended to leave it off.
* *UseFileIgnore*:
  Whether the code should use ``file_ignore`` or simply ``ignore``
  as the column name to store the ``ignore`` flag of files
  in the ``ngas_files`` table.
  The latter was used by some particular combinations
  of old versions of the NGAS code and database engines,
  while the former is the default nowadays.
* *SessionSql*:
  Zero or more XML sub-elements,
  each with an ``sql`` attribute denoting
  an SQL statement that will be executed whenever
  a physical connection is established
  by the connection pool to the database server.
  Usually these will not be required,
  but can be useful, for instance,
  if one needs to execute a command
  to switch to a different database.

The rest of the attributes on the *Db* element
are used as keyword arguments to create connection
from the database module
selected with the *Interface* attribute,
and therefore don't have fixed names on them
as they depend on the module in use.

For example, to connect to a PostgreSQL database
using the ``psycopg2`` module
one could use:

.. code:: xml

   <Db Id="db-config"
       Snapshot="0"
       UseFileIgnore="false"
       Interface="psycopg2"
       host="db_host.example.com"
       dbname="ngas_db"
       user="ngas_user"
       password="ngas_password"
   />

In the example,
the ``Snapshot``, ``UserFileIgnore`` and ``Interface`` attributes
work as described above,
while ``host``, ``dbname``, ``user`` and ``password``
are keyword arguments accepted by the ``psycopg2.connect`` method.


.. _config.commands:

Commands
--------

This element lists user-defined command plug-ins.
For details on commands in general
see the :doc:`commands overview <commands-index>` section.
For details on command plug-ins
see the :doc:`commands plug-in <plugins/commands>` section.

The ``Comands`` element contains zero or more
XML sub-elements named ``Command``,
each of which must define the following attributes:

* *Name*: The command name, case-sensitive.
* *Module*: The python module implementing this command.

.. _config.mime_types:

MimeTypes
---------

Lists a mapping of filename extensions and mime types.
It contains one or more ``MimeTypeMap`` elements,
each one listing the following attributes:

 * *Extension*: A filename extension.
 * *MimeType*: The mime-type associated to that filename extension.

This information is used, for example,
by the :ref:`commands.archive` command
when no mime-type information has been sent by the user.

.. _config.storage_sets:

StorageSets
-----------

Lists the storage sets (i.e., groups of disks) available to NGAS.
Inside the ``StorageSets`` element one or many ``StorageSet`` elements
can be found, each one listing the following attributes:

 * *StorageSetId*: The name this storage set can be referenced by.
 * *MainDiskSlotId*: The name of the directory where the data will be stored.
   If a relative path is given, it is considered to be relative to the NGAS
   volumes directory.
 * *RepDiskSlotId*: The name of the directory where the data will be replicated.
   If a relative path is given, it is considered to be relative to the NGAS
   volumes directory.

For an explanation on volumes, main/replication disks,
directories and storage sets
please read :ref:`server.storage`.

.. _config.streams:

Streams
-------

Lists the mappings from data types to storage sets.
This element contains one or more ``Stream`` elements,
each of which lists the following attributes:

 * *MimeType*: The data type of this stream.
 * *PlugIn*: The plug-in used to process incoming data of this type.
 * *PlugInPars*: An optional, comma-separated, key=value string
   with parameters that can be communicated to the plug-in.

References to storage sets are included by adding ``StorageSetRef``
sub-elements, each of which should have a ``StorageSetId`` attribute
pointing to the corresponding storage set.

For an explanation on streams please read :ref:`server.storage`.

.. _config.archivehandling:

ArchiveHandling
---------------

Contains archiving-related configuration.
For an explanation on most of these terms
see :ref:`server.storage` for reference.

 * *PathPrefix*: The top-level directory on each volume
   under which NGAS will store incoming data.
 * *Replication*: Whether data will be replicated during archiving
   from the Main disk to a Replication disk
 * *BackLogBuffering*: whether data stored
   during a failed ``ARCHIVE`` command
   *might* be temporarily kept in storage
   to try to finish its archiving later on in the background.
 * *BackLogBufferDirectory*: The top-level directory on each volume
   where backlogged files will be temporarily stored.
 * *CRCVariant*: The CRC algorithm (and implementation) to use
   to calculate the checksum of incoming files.
   See :ref:`server.crc` for details.
   If not specified the server will use the ``crc32`` variant. If specified,
   ``0`` means ``crc32``, ``1`` means ``crc32c`` and ``2`` means ``crc32z``.
 * *EventHandlerPlugIn*: Zero or more sub-elements defining additional modules
   that will handle :ref:`archiving events <server.archiving_events>`.
   Each element should have a ``Name`` attribute with the fully-qualified
   class name implementing :doc:`the plug-in <plugins/archiving_events>`,
   and an optional ``PlugInPars`` attribute
   with a comma-separated ``key=value`` definitions,
   which are passed down to the class constructor as keyword arguments.
 * *FreeSpaceDiskChangeMb*: How much available free space
   in a disk will trigger an error notification to change that disk
   (see :ref:`config.notification` for details).
 * *MinFreeSpaceWarningMb*: Minimum amount of free space a disk should have.
   If a disk has less free space than that
   a warning email is sent (see :ref:`config.notification`).


.. _config.processing:

Processing
----------

The ``Processing`` element defines the behavior
of the optional :ref:`on-the-fly processing capabilities <server.processing>`
attached to the :ref:`RETRIEVE <commands.retrieve>` command.
The following attributes are supported:

* *ProcessingDirectory*: The directory
  (potentially relative to the NGAS root directory)
  where a ``processing`` directory will be created on,
  under which temporary files used during on-the-fly processing
  will be put under.

Under the ``Processing`` element,
one or more ``PlugIn`` sub-elements can be placed,
one per processing plug-in to be declared.
Each ``PlugIn`` element accepts the following attributes:

* *Name*: The name of the python module
  (with a similarly-named function)
  where the plug-in is implemented.
* *PlugInPars*: A comma-separated list
  of ``key=value`` parameter definitions
  to be passed to the plug-in.

Finally, inside each ``PlugIn`` element
one or more ``MimeType`` elements can be added
to specify which MIME types will be processed by the plug-in.
Each ``MimeType`` element needs to have a ``Name`` attribute
with specifying the MIME type.


.. _config.register:

Register
--------

The ``Register`` element configures
the plug-ins to be used by the :ref:`REGISTER <commands.register>` command.

Plug-ins are configured per mime-type.
Like :ref:`config.processing`,
one or more ``PlugIn`` sub-elements can be placed
under the ``Register`` element,
following the same guidelines.


.. _config.notification:

Notification
------------

The ``Notification`` element defines the behavior
of the server :ref:`email notifications <server.notifications>`.
The following attributes are available:

 * *Active*: Whether notifications are enabled or not.
   Note that even if disabled, there are some notifications
   (that are considered too important to be missed)
   that will still be sent.
 * *SmtpHost*: The SMTP host to use as the email agent.
 * *Sender*: The email address that will appear
   in the ``Sender:`` field of emails sent by this mechanism.
 * *MaxRetentionTime*: Maximum amount of time
   an undelivered email will be internally kept for
   before the system decides not to deliver it.
 * *MaxRetentionSize*: Maximum amount of undelivered emails
   the system will keep internally
   before it starts dropping old emails.

Emails resulting from different events
can be configured to be sent to one or more
email addresses.
This is done
by defining ``EmailRecipient`` elements,
each with an ``Address`` attribute
whose value is the target email address.
These ``EmailRecipient`` elements are then added as children
of the following sub-elements of ``Notification``:

* *AlertNotification*: (*Deprecated*) Never sent.
* *ErrorNotification*: Sent in a number
  of different error situations.
* *DiskSpaceNotification*: Sent when, during operations,
  one or more disk are found to have less free space
  than the configured amount (see :ref:`config.archivehandling`).
* *DiskChangeNotification*: Sent when a disk is full,
  potentially requiring a change.
* *NoDiskSpaceNotification*: Sent when, during operations,
  no sufficient space can be found in one or more disks.
* *DataCheckNotification*: Sent by the :ref:`bg.datacheck_thread`
  informing about the results of the data checking process.
  Normally sent only if there are errors to be reported,
  but can be configured to be always sent
  (see :ref:`config.datacheck_thread`)

Below is an example
illustrating a valid configuration:

.. code:: xml

  <Notification Id="Notification"
                Active="0" MaxRetentionSize="1" MaxRetentionTime="00T00:30:00"
                Sender="ngas@host.com" SmtpHost="localhost">
    <AlertNotification>
      <EmailRecipient Address="address@example.com"/>
    </AlertNotification>
    <ErrorNotification>
      <EmailRecipient Address="address@example.com"/>
    </ErrorNotification>
    <DiskSpaceNotification>
      <EmailRecipient Address="address@example.com"/>
    </DiskSpaceNotification>
    <DiskChangeNotification>
      <EmailRecipient Address="address@example.com"/>
    </DiskChangeNotification>
    <NoDiskSpaceNotification>
      <EmailRecipient Address="address@example.com"/>
    </NoDiskSpaceNotification>
    <DataCheckNotification>
      <EmailRecipient Address="address@example.com"/>
    </DataCheckNotification>
  </Notification>

.. _config.janthread:

JanitorThread
-------------

The ``JanitorThread`` element defines the behavior
of the :ref:`Janitor Thread <bg.janitor_thread>`
(now actually implemented as a separate process).
The following attributes are available:

 * *SuspensionTime*: The sleep time after a janitor cycle.
 * *MinSpaceSysDirMb*: The minimum space to be found on each volume during each
   cycle. If not enough space is found the system is sent to OFFLINE state.
 * *PlugIn*: An XML sub-element with a *Name* attribute, naming a python module
   where a Janitor plug-in resides. Multiple *Plugin* elements can be defined.

.. _config.datacheck_thread:

DataCheckThread
---------------

The ``DataCheckThread`` element defines the behavior
of the :ref:`bg.datacheck_thread`.
The following attributes are available:

 * *Active*: Whether the data-check thread should be allowed to run or not.
 * *MaxProcs*: Maximum number of worker processes used to carry out the data
   checking work load.
 * *MinCycle*: The time to leave between data-check cycles.
 * *ForceNotif*: Forces the sending of a notification report after each
   data-check cycle, even if not problems were found.
 * *Scan*: Whether files should be scanned only (1) or actually checksumed (0).

The following attributes are present in old configuration files
but are not used anymore: *FileSeq*, *DiskSeq*, *LogSummary*, *Prio*,
*ChecksumPlugIn* (see :ref:`CRCVariant <config.archivehandling>` instead)
and *ChecksumPlugInPars*.


.. _config.caching:

Caching
-------

The ``Caching`` element defines the behavior
of the :ref:`cache control thread <bg.cache_thread>`.
When enabled, it is said that the NGAS server
is running in :ref:`cache mode <server.modes.cache>`.
The following attributes are available:

 * *Enable*: Whether the cache control thread should run or not.
 * *Period*: The period at which the cache control thread runs.
 * *MaxTime*: The maximum time files can stay in the cache.
 * *MaxCacheSize*: The maximum total allowed volume of files in the cache.
 * *MaxFiles*: The maximum allowed number of files in the cache.
 * *CacheControlPlugIn*: A user-provided cache deletion plug-in
   that decides whether individual files
   should be marked for deletion.
 * *CacheControlPlugInPars*: Parameters for the plug-in above.
 * *CheckCanBeDeleted*: Check if a file marked for deletion
   has been sent to all subscribers yet
   before actual deletion occurs.


.. _config.log:

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
* *LogRotateCache*: The amount of rotated files to retain. If more rotated files
  are found, they are removed by the system.
* *SysLog*: An integer indicating whether syslog logging is enabled
  (``1``) or disabled (``0``).
* *SysLogPrefix*: The string used as prefix for all syslog messages.
* *SysLogAddress*: The address where the syslog messages should be sent to.
  If not specified a platform-dependent default value is used.
* *ArchiveRotatedLogfiles*: An integer indicating whether rotated logfiles
  should be locally archived by NGAS (``1``) or not (``0``). Defaults to ``0``.
* *LogfileHandlerPlugIn*: Zero or more sub-elements defining additional modules
  that will handle rotated logfiles. Each element should have a ``Name``
  attribute with the fully-qualified module name implementing the plug-in inside
  a ``run`` method, and a ``PlugInPars`` element with a comma-separated,
  ``key=value`` pairs.

.. _config.authorization:

Authorization
-------------

The ``Authorization`` element defines the authentication and authorization rules
that the NGAS server will follow when receiving commands from clients.
For details see :ref:`server.authorization`.

The ``Authorization`` element has an ``Enable`` attribute
which determines whether authentication and authorization
is enabled (``1``) or not (``0``).
Zero or more ``User`` XML sub-elements
also describe a different user recognized by NGAS.
Each ``User`` element should have the following attributes:

* *Name*: The username.
* *Password*: The base64-encoded password.
* *Commands*: A comma-separated list of commands this user is allowed to
  execute. The special value ``*`` is interpreted as all commands.


SubscriptionAuth
----------------

The ``SubscriptionAuth`` element defines the authentication/authorisation
configuration to use when acting as a client when using the subscription
service. Currently it has only one element ``PlugInName``, which follows the
usual rules for plugins as noted above, with ``PlugInName`` being the name of
the module to import. This module should have a callable which matches with the
signature:

.. py:function:: ngas_subscriber_auth(filename, url)

    Provides authentication information needed to send ``filename`` to ``url``.

    This function should return an object that can be handled by the ``auth``
    keyword argument of requests.requests, which is generally either a string,
    or an instance of ``requests.auth.AuthBase``. ``None`` can be returned in
    the case where the authentication is not needed.

    :param str filename: The filename to be sent
    :param str url: The url to send the filename to
    :return: An object used by requests to authenticate the connection
    :rtype: requests.auth.AuthBase, None, str


.. _config.suspension:

HostSuspension
--------------

The ``HostSuspension`` element defines
the behavior of the :ref:`server suspension <server.suspension>`.
The following attributes are defined:

* *IdleSuspension*: Whether suspension is enabled (``1``) or not (``0``).
* *IdleSuspensionTime*: The amount of idle time
  after which a server will suspend itself.
* *SuspensionPlugIn* and *SuspensionPlugInPars*:
  The plug-in used to perform suspension, and its parameters.
* *WakeUpServerHost*: The server in charge
  of waking up server that are idling.
* *WakeUpPlugIn* and *WakeUpPlugInPars*:
  The plug-in used to perform the wake-up, and its parameters.
* *WakeUpCallTimeOut*: Maximum amount of time
  that a wake up call should take.
  If a server cannot be woken up after this timeout
  it is considered to be still idling.


.. _config.system_plugins:

SystemPlugIns
-------------

The ``SystemPlugIns`` element defines
a collection of system-level plug-ins.
These plug-ins are used for different purposes,
either by a command or by the core system.
The ``*PlugIn`` attributes name
a python module that offers a function with the same name,
while the ``*PlugInPars`` attributes
are a comma-separated key=value parameter pairs:

 * *LabelPrinterPlugIn* and *LabelPrinterPlugInPars*:
   The plug-in that brings hardware-specific capabilities
   to the ``LABEL`` command.
 * *OfflinePlugIn* and *OfflinePlugInPars*:
   The plug-in used to bring the server to ``OFFLINE`` state
   (see :ref:`server.states`).
 * *OnlinePlugIn* and *OnelinePlugInPars*:
   The plug-in used to bring the server to ``ONLINE`` state
   (see :ref:`server.states`).
 * *DiskSyncPlugIn* and *DiskSyncPlugInPars*:
   The plug-in used to perform a full disk sync.
