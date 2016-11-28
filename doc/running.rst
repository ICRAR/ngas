#######
Running
#######

.. note::
 If NGAS was installed in a virtual environment
 remember to source it before proceeding.
 Remember that the :ref:`fabric-based installation <inst.fabric>`
 always installas NGAS in a virtualenv.

.. _running.server:

Server
======

The NGAS server is run using the following command::

 ngamsServer -cfg <configFile>

For a full list of all command-line flags run ``ngamsServer -h``.

To start the NGAS server as a daemon run instead::

 ngamsDaemon start

The NGAS daemon accepts also the ``stop`` and ``status`` commands. The NGAS
daemon uses the ``NGAS_PREFIX`` environment variable to determine the root of
the NGAS data directory; otherwise it defaults to ``${HOME}/NGAS``. Inside this
directory it will look for the ``cfg/ngamsServer.conf`` file, which will use to
start the NGAS server.

Additionally the ``ngamsCacheServer`` and ``ngamsCacheDaemon`` commands are
available, which start the NGAS server in cache mode.


Client
======

To run the NGAS (python) client run the following command::

 ngamsPClient

For a full list of all command-line flags run ``ngamsPClient -h``.

Likewise, the NGAS C client is run with the following command::

 ngamsCClient
