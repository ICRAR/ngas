#################
Post-installation
#################

.. note::
 If NGAS was installed in a virtual environment
 remember to source it before proceeding.

Setting up an NGAS instance
===========================

These steps describe how to set up an NGAS server instance.

When using on of the :ref:`fabric-based installation <inst.fabric>` procedures
an NGAS root directory is automatically created and prepared
under ``~/NGAS`` of the user hosting the NGAS installation
(or somewhere different if indicated via the ``NGAS_ROOT`` fabric variable)

.. _post_inst.preparing_root:

Create an NGAS root directory
-----------------------------

NGAS's *root* is the top-level directory
that will be used to store all its internal files,
including the data being stored.

The NGAS root directory can be placed anywhere in the filesystem,
and can be totally empty initially.
The only requirement is that it is writable by the user
running the NGAS server.

To help users create their an NGAS root directory,
NGAS comes with a ``prepare_ngas_root.sh`` script.
The script needs at least
the name of the target directory
that will be used as NGAS root.
More options are available,
you can use ``prepare_ngas_root.sh -h`` to learn more.

The ``prepare_ngas_root.sh`` script
will also create simple, but usable, *volumes*
under the NGAS *root* directory.
These are sufficient for testing purposes,
but you may want to setup proper volumes
(see next section).


.. _post_inst.setup_volumes:

Setup volumes
-------------

Inside the NGAS *root* directory
*volumes* should exist
where the data will be stored
(see :ref:`server.storage` for a full explanation).
Volumes need only be directories,
so you can either create directories for each volume
in simple or testing scenarios,
or symbolic link actual partitions as separate volumes
for more demanding, real-world setups
-- it's your choice.

In any case, volumes need to be tagged as such
in order to be recognized by NGAS.
This is done by placing a small, hidden file in the root
of the volume containing a random UID for the disk
using the ``ngasPrepareVolumeNoRoot`` utility.

For example, if the NGAS *root* directory
is under ``~/NGAS`` and a new volume called
``volume1`` is created,
it can be tagged as such::

 $ cd ~/NGAS
 $ mkdir volume1
 $ cd volume1
 $ python /path/to/ngas-sources/src/ngasUtils/src/ngasPrepareVolumeNoRoot.py --path=$PWD

Answer Yes and you're done.

To let NGAS know about your volumes check
the :ref:`config.storage_sets` configuration option.

.. _post_inst.run_server:

Running the server
==================

.. note::
  In case you haven't yet, please review how to
  :ref:`setup an NGAS server instance <post_inst.preparing_root>`
  before you start to start the server for the first time.

After a successful installation and setup,
you should be able to run the server by running::

   ngamsServer -cfg <configFile>

For a full list of all command-line flags run ``ngamsServer -h``.
In particular, when running manually
you will probably want to use the ``-autoonline`` flag
to bring the server to the ``ONLINE`` state immediately,
and ``-v 4`` to increase the output of the server
to the ``INFO`` logging level.

To start the NGAS server as a daemon run instead::

 ngamsDaemon start <params>

The NGAS daemon accepts also the ``stop`` and ``status`` commands.
Any parameters given in ``<params>`` will be passed down verbatim
to the ``ngamsServer`` being started,
and thus should include at least the configuration file flag.

.. _post_inst.run_client:

Running the client
==================

To run the NGAS (python) client run the following command::

 ngamsPClient

For a full list of all command-line flags run ``ngamsPClient -h``.

Likewise, the NGAS C client (if installed)
is run with the following command::

 ngamsCClient


As a more complex test,
the following command can be used
to execute the client and issue an ARCHIVE command
to the server.
If successful, this will signal
that the whole installation is working fine::

	ngamsPClient ARCHIVE --file-uri $(which ngamsPClient) --mime-type application/octet-stream -v

What comes out should look as follows::

   ----------------------------------------------------------------------------------------------
   Host:           icrar-dirp01
   Port:           7777
   Command:        ARCHIVE

   Date:           2015-12-10T16:58:40.759
   Error Code:     0
   Host ID:        icrar-dirp01
   Message:        Successfully handled Archive Push Request for data file with URI ngamsPClient
   Status:         SUCCESS
   State:          ONLINE
   Sub-State:      IDLE
   NG/AMS Version: v4.1-ALMA/2010-04-14T08:00:00
   ----------------------------------------------------------------------------------------------

.. _post_inst.run_tests:

Running the tests
=================

If you want to run the suite of unit tests
then you need to install at least one additional package::

  $> pip install psutil

Unit tests are found in the ``test_*.py`` files
under the ``test`` directory
of the ngas source distribution.
You can use any unittest runner to execute the tests.
In particular, we tend to use pytest, like this::

  $> pip install pytest
  $> pytest

.. note::

 Previous versions of NGAS had the restriction
 that one had to be **inside** the test/ directory
 to run the tests.
 This restriction does not exist anymore,
 although one can *still* run the tests from within the directory.
 Tools ``pytest`` or the ``unittest`` built-in module
 should also be able to automatically discover and execute
 unit tests.


.. _post_inst.run_tests.tmp_dir:

Using alternative filesystem
----------------------------

The NGAS unit tests,
and the servers that are spawned from within,
look through the following possible directories
to host any temporary files that need to be written to disk:

 * The value of the ``NGAS_TESTS_TMP_DIR_BASE`` environment variable.
 * ``/dev/shm``, if present (it usually is in Linux systems)
   and has at least 1 [GB] of space available.
 * The return value of ``tempfile.gettempdir()``,
   which returns a well standardized location temporary directory.

Whatever value is used,
a final ``/ngas`` path element is added at the end.
For example, in a normal Linux system
the tests would write their temporary files
to ``/dev/shm/ngas/``.

The rationale of prioritizing ``/dev/shm``
over the system-dependent standard temporary directory location
is to speedup the test execution.
``/dev/shm``, when present, is often backed up
by an in-memory filesystem,
and therefore I/O operations run much faster there.
If you system has such location in the filesystem
but doesn't correspond to an in-memory filesystem,
or if you want to use your system's temporary directory
(usually ``/tmp`` under Linux)
you can still set the ``NGAS_TESTS_TMP_DIR_BASE`` environment variable
to point to it.

Using alternative databases
---------------------------

By default the unit tests will run
against a temporary on-disk sqlite database.
If users want to run the tests against a different database
they can do so by setting the ``NGAS_TESTDB`` environment variable
to contain a :ref:`config.db` XML element
with the correct information.

For example, in Travis we run our tests against
the local MySQL database like this::

 export NGAS_TESTDB='<Db Id="blah" Snapshot="0" Interface="MySQLdb" host="127.0.0.1" db="ngas" user="ngas" passwd="ngas"/>'
 pip install psutil pytest-cov
 pytest --cov

Keeping intermediate results
----------------------------

Tests generate a number of temporary files and directories on disk
that are automatically removed after each test finishes,
regardless of whether they fail or succeed.
If the output of **the last test executed** needs to be kept
users can set the ``NGAS_TESTS_NO_CLEANUP`` to a value different than ``0``.
This will keep all files under :ref:`the temporary directory <post_inst.run_tests.tmp_dir>` untouched
so users can go and get more details about the test execution.
