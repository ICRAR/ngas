#################
Post-installation
#################

.. note::
 If NGAS was installed in a virtual environment
 remember to source it before proceeding.

Trying out the server
=====================

The following command can be used to just make sure that the whole installation
is working fine::

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

Running the tests
=================

If you want to run the suite of unit tests
then you need to install an additional package::

  $> pip install psutil

Then run the following::

  $> cd ngas_src_directory/src/ngamsTest/ngamsTest
  $> python ngamsTest.py

This will execute the full suite of unit tests. Individual files can also be run
to avoid having to run the entire suite. When running individual files a
comma-separated list of methods to be run can be given via the ``-tests``
command-line flag.

You can alternatively use a different unittest runner tool,
like py.test::

  $> pip install pytest
  $> cd ngas_src_directory/src/ngamsTest/ngamsTest
  $> py.test -o 'python_files=*Test.py'
