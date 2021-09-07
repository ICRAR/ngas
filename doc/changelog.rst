Changelog
=========

.. rubric:: Development version

* Using ``sendfile(2)`` when POSTing files through HTTP connections.
  This should lower the overhead of using python to perform the transfer,
  bringing benefits to ``ngamsPClient`` class and command-line tool,
  and to the subscription thread.
* Improved NGAS's mail sending capabilities.
  The code is now automatically tested by our unit tests,
  which allowed us to ensure it sends mails correctly,
  and works with python3.
  On top of that mail messages are now created
  using the standard library modules for correct mail composition,
  instead of the "hand-written" logic we had previously.
* Fixed a small issue during backlog file archival
  where the checksum plugin name was incorrectly recorded in the database.
  This shouldn't affect operational installations,
  and most database drivers happily worked with the previous code,
  but the Sybase driver caught this one.
* HTTP requests for archival with ``Content-Length`` equals to 0,
  or with missing ``Content-Length``,
  now return an 411 HTTP status code
  instead of the more generic 400 HTTP status code.
* Cleaned up and aligned the way in which volume information
  is created and processed by NGAS.
  The code around volume creation and scanning has been revised,
  unit tests have been improved
  to test the functionality more thoroughly,
  and the existing, not-well maintained script
  has now been :ref:`better integrated <tools.prepare_volume>`
  into the NGAS ecosystem.
  Instructions on how to :ref:`set up a volume directory <post_inst.setup_volumes>`
  have also been updated,
* Renamed the old ``ngasArchiveClient`` tool
  into ``fs-monitor``,
  and moved it into the ``ngamsPClient`` package.
  :ref:`This utility <tools.fs_monitor>`
  had been kept until now
  under the unmaintained ``ngasUtils`` package,
  and therefore hadn't been ported
  alongside the rest of the code until now.
  Not only was this tool renamed,
  but it was completely overwritten
  to simplify its maintenance in the long run,
  and to enable easy unit testing,
  which we have also added now.
  Moving it into the ``ngamsPClient``
  ensure it will have continued testing and visibility.
* Added tests to ensure all plugin modules can be correctly imported.
  This ensures the code is compatible with python 3 up to some degree,
  and it also increases our code coverage.
* Unit tests don't need to be run from within the ``test`` directory anymore.
  This makes using unit test tools
  like ``pytest`` or the built-in ``unittest`` module
  easier to use.
* Fixed issues with the ``BBCPARC`` command,
  which `didn't work <https://github.com/ICRAR/ngas/issues/19>`_
  for remote host transfers, only for ``localhost`` ones.
* Fixed small issue in ``QUERY`` command
  where column names where not correctly aligned
  with the underlying column data.
* Added support for using HTTPS with both the client and with subscriptions.
  Support for wrapping the server with TLS has also been added, but this should
  only be used for testing, rather than in production (we recommend handling
  HTTPS with more traditional HTTPS servers such as Apache or Nginx).
* Added support for using more diverse authentication plugins for subscription,
  via the authentication mechanisms provided by requests. Note that with the
  current setup, the authentication plugins can only be used with HTTPS (this
  may change in the future).
* Default FITS plug-in doesn't mandate an ``ARCFILE`` keyword to be present
  in the incoming FITS file if the ``ignore_arcfile=1`` option is given in the
  HTTP parameters. This is useful for archiving FITS files that don't have this
  keyword, but that want to use the default FITS plug-in.
* Removing the ``create_venv.sh`` script from the sources,
  in favour of letting users create one by themselves if they want,
  or let the fabric tasks create one.
* Added `Exclude` attribute to the `Authorization` element for defining
  a list of commands that are to be excluded from authorization
* Added new partner sites feature that provides the capability of configuring
  a remote NGAS cluster as a proxy for retrieving files that are not available
  in the local NGAS cluster.
* Fixed the mirroring plugin modules: `ngamsCmd_HTTPFETCH.py`,
  `ngamsCmd_MIRRARCHIVE.py`, `ngamsCmd_MIRREXEC.py` and `ngamsCmd_MIRRTABLE.py`
* Ported NGAS utility scripts: `ngasCheckFileCopies.py`, `ngasCheckFileList.py`
  `ngasDiscardFiles.py`, `ngasVerifyCloning.py` and `ngasXSyncTool.py`
* Fixed an issue with the subscription mechanism,
  where upstream files with checksum values
  calculated with checksum variants other than ``crc32``
  failed to be pushed downstream.
* Fixed logging of C utilities,
  and implemented the logic behind the ``-v`` flag
  of the ``ngamsCClient`` program.
* Improved error message sent back by the ``REGISTER`` command
  when registration of a file
  with a MIME type with no configured plug-in
  is requested.
* The ``bad-files`` directory
  now exists on each volume
  rather that there being a single one
  outside of any volume directory.
  This allows for faster movement of files
  into the ``bad-files`` directory,
  a more consistent directory structure,
  and better traceability of files that end up in ``bad-files``.
* The ngamsDaemon.py now checks the `-force` command line option and will
  forcibly start-up and clean up any existing PID lock files.
* Lots of code clean up for the mirroring plugin code using PEP8 style
  guidelines. Replaced deprecated rfc822 module with email module in the
  ngamsDAPIMirroring.py and ngamsAlmaMultipart.py plugins. Added
  test_dapi_mirroring.py unit tests. General code clean up changes in both
  mirroring and SDM multipart plug-ins.
* Fixed 'Connection refused' exception on mirror plugin start-up. Now logs a
  warning when NGAS server is not available. Now uses exception class instead
  of string for handling stop events.

.. rubric:: 11.0.2

* Fixed an important bug that was preventing the ``STATUS`` command
  from being imported correctly in normal NGAS installations.

.. rubric:: 11.0.1

* Fixed an important bug that was causing data to be removed
  from the NGAS root directory.

.. rubric:: 11.0

* Initial python 3 support.
  The code not only correctly imports under python 3,
  but also all unit tests pass correctly.
  The code is both 2.7/3.5+ compatible,
  so users don't need to immediately switch to python 3.
  Given that our test coverage currently sits at about 65%,
  it is likely that there are code paths that need further work.
* :doc:`Command plug-ins <plugins/commands>` can be implemented
  as user-provided plug-ins.
  This was almost the case until now, as they still had the restriction
  of having to reside on the ``ngamsPlugIns`` package,
  which is not the case anymore.
  Moreover, a single python module can implement the logic
  of more than one command.
* Unit tests can be run against :ref:`arbitrary filesystems <post_inst.run_tests.tmp_dir>`,
  and they default to run under ``/dev/shm`` for faster execution.
* Added new CRC variant called ``crc32z``.
  It behaves exactly like ``crc32``, except that its values,
  *as stored in the database*, should be consistent
  across python 2.7 and 3.
  The ``crc32`` variant does not have this property,
  although we can still (and do) normalize them
  when checking files' checksums.
* Changed the server to use a thread pool to serve requests
  instead of creating a brand new thread every time a request comes in.
* Improving how the :ref:`RETRIEVE <commands.retrieve>` command works
  when returning compressed files.
* Adding support to the ``CRETRIEVE`` command
  to retrieve all files as a tarball.
  It internally uses ``sendfile(2)`` when possible.
* Users can configure NGAS to issue a specific SQL statement
  at connection-establishment time, similarly to how other connection pools do.
* Fixed a few details regarding expected v/s real datatypes
  used in some SQL queries.
  These affected only the Sybase ASE official driver,
  which is now working correctly.
* Unit tests moved to the top-level ``test`` directory,
  and renamed to ``test_*.py``.
  This makes it more straight-forward to use unit test runners
  which usually rely on this layout for test discovery.
* A new sample configuration file replaces the old, large set
  of configuration files that used to be shipped with NGAS.
* Starting a server in cache mode is now be done
  via a configuration file preference rather than a command-line argument.
* The subscription code and the cache handling thread
  update the file status flags atomically.
  Before they had a race condition which resulted in files
  not being deleted on the cache server.
* Improving handling of overwriting flags for archiving commands.
  Now all archiving commands obey the same logic,
  which has been detached from the individual
  data-archiving plug-ins.
* Improving and simplifying the ``QUERY`` command.
* Removed many unnecessary internal usage
  of ``.bsddb`` files.
* Added a MacOS build
  to our `Travis CI <https://travis-ci.org/ICRAR/ngas>`_ set up.
* Misc bug fixes and code improvements.

.. rubric:: 10.0

* The ``ARCHIVE``, ``QARCHIVE``, ``REARCHIVE`` and ``BBCPARC`` commands now use the same underlying code.
  All the small differences between the commands has been kept, so they should behave exactly as before.
  This was a required step we needed to take before implementing other improvements/bugfixes.
* The archiving commands listed above are now more efficient in how they calculate the checksum of the incoming data.
  If the data archiving plug-in promises not to change the data, then the checksum is calculated on the incoming stream
  instead of calculating it on the file, reducing disk access and response times.
  This behavior was previously not seen
  neither on the ``ARCHIVE`` command,
  which always dumped all contents to disk
  and then did a checksum on the on-disk contents,
  nor in the ``QARCHIVE`` command,
  which **unconditionally** calculated the checksum
  on the incoming stream,
  irrespective of whether the data archiving plug-in
  changed the data afterward or not.
* Partial content retrieval for the ``RETRIEVE`` command has been implemented.
  This feature was present in the ALMA branch of the NGAS code,
  and now has been incorporated into ours.
* We merged the latest ALMA mirroring code into our code base.
  This and the point above should ensure that NGAS is ALMA-compatible.
* Unified and centralized all the CRC checksuming code,
  and how different variants are chosen.
* We have improved response times for scenarios
  when many parallel ``RETRIEVE`` commands are issued.
  Worst-case scenario times in 100 parallel request scenarios were brought down
  from tens of seconds to about 2 seconds (i.e., an order of magnitude).
* Moved the :ref:`data-check <bg.datacheck_thread>` background thread checksum
  to a separate pool of processes
  to avoid hanging up the main process.
  The checksuming also pauses/resumes depending on whether the server
  is serving any requests or not to avoid exhausting access to the disk.
* Added the ability to write plug-ins that will react to each file archiving
  (e.g., to trigger some processing, etc).
* Added support for the latest `bbcp <https://www.slac.stanford.edu/~abh/bbcp/>`_ release,
  which includes, among other things, our contributions
  to add support for the ``crc32c`` checksum variant,
  plus other fixes to existing code.
* Fixed a few small problems with different installation scenarios.

.. rubric:: 9.1

* NGAS is now hosted in our public `GitHub repository <https://github.com/ICRAR/ngas>`_.
* `Travis CI <https://travis-ci.org/ICRAR/ngas>`_ has been set up
  to ensure that tests runs correctly against SQLite3, MySQL and PostgreSQL.
* User-provided plug-ins do not need to be installed alongside NGAS anymore.
  This allows users to place their plug-ins
  in their own personally-owned directories,
  which in turn allows to install NGAS in isolation,
  and probably with more strict permissions.
* Project-specific plug-ins under the ``ngamsPlugIns`` package
  have been moved to sub-packages (e.g., ``ngamsPlugIns.mwa``),
  and will eventually be phased out as projects take ownership
  of their own plug-ins.
* :ref:`Janitor Thread <bg.janitor_thread>` changes:

  * Plug-ins: Instead of having a fixed, single module with all the business logic of the Janitor Thread,
    its individual components have been broken down into separate modules
    which are loaded and run using a standard interface.
    This makes the whole Janitor Thread logic simpler.
    It also allows us to implement users-written plug-ins
    that can be run as part of the janitor thread.
  * The execution of the Janitor Thread doesn't actually happen in a thread anymore,
    but in a separate process.
    This takes some burden out from the main NGAS process.
    In most places we keep calling it a thread though;
    this will continue changing continuously as we find these occurrences.

* The NGAS server script, the daemon script and the SystemV init script
  have been made more flexible,
  removing the need of having more than one version for each of them.
* Some cleanup has been done on the NGAS client-side HTTP code
  to remove duplicates and offer a better interface both internally and externally.
* Self-archiving of logfiles is now optional.
* A few occurrences of code incorrectly handling database results
  have been fixed,
  making the code behave better across different databases.
* Misc bug fixes and code cleanups.

.. rubric:: 9.0

* Switched from our ``pcc``-based, own home-brewed logging package
  to the standard python logging module.
* Unified time conversion routines, eliminating heaps of old code
* Removed the entire ``pcc`` set of modules.
* General bug fixes and improvements.

.. rubric:: 8.0

* Re-structured NGAS python packages.
  Importing NGAS python packages is now simpler and doesn't alter the python path in any way.
  The different packages can be installed
  either as zipped eggs, exploded eggs, or in development mode.
  This makes NGAS behave like other standard python packages,
  and therefore easier to install in any platform/environment
  where setuptools or pip is available.
* ``RETRIEVE`` command uses ``sendfile(2)`` to serve files to clients.
  This is more efficient both in terms of kernel-user interaction
  (less memory copying), and python performance (less python instructions
  have to be decoded/interpreted, needing less GIL locking, leading to better
  performance and less multithread contention).
* Initial support for logical containers.
  Logical containers are groups of files, similar to how directories group files in a filesystem.
* NGAS server replying with more standard HTTP headers
  (e.g., ``Content-Type`` instead of ``content-type``).
  Most HTTP client-side libraries are lenient to these differences though.
* Streamlined ``crc32c`` support throughout ``QARCHIVE`` and subscription flows.
  We use the `crc32c <https://github.com/ICRAR/crc32c>`_ module for this,
  which was previously found as part of NGAS's source code,
  but that has been separated into its own package for better reusability.
* Stabilization of unit test suite.
  Now the unit test suite shipped with NGAS runs reliably on most computers.
  This made it possible to have a continuous integration environment
  (based on a private Jenkins installation)
  to monitor the health of the software after each change on the code.
* Improved SQL interaction, making sure we use prepared statements all over the place,
  and standard PEP-249 python modules for database connectivity.
* Improved server- and client-side connection handling,
  specially error-handling paths.
* General bug fixes and improvements.
