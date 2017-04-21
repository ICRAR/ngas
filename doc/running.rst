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

 ngamsDaemon start <params>

The NGAS daemon accepts also the ``stop`` and ``status`` commands.
Any parameters given in ``<params>`` will be passed down verbatim
to the ``ngamsServer`` being started,
and thus should include at least the configuration file flag.

.. _running.client:

Client
======

To run the NGAS (python) client run the following command::

 ngamsPClient

For a full list of all command-line flags run ``ngamsPClient -h``.

Likewise, the NGAS C client is run with the following command::

 ngamsCClient
